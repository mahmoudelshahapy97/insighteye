"""
Import the standalone lost-found app's data into InsightEye.

Copies cameras, abandoned events, found items, lost reports and matches from the
standalone app's Postgres into one InsightEye workspace, and its media (alert frames,
found-item crops, evidence clips, lost-report photos) into InsightEye's S3 bucket.

    python scripts/migrate_lost_found_data.py \\
        --source-dsn postgresql://lostfound:lostfound_pass@localhost:5436/lostfound_db \\
        --workspace-id <uuid> --owner-user-id <uuid> \\
        --minio-endpoint http://localhost:9000 --minio-access-key ... --minio-secret-key ... \\
        --clip-root /home/ubuntu/lost-found/backend \\
        [--camera-map cameras.json] [--dry-run]

Cameras: a lost-found camera listed in ``--camera-map`` (``{"<camera_id>": "<stream_id>"}``)
is attached to that existing InsightEye camera, which must be in the workspace. Any
other camera becomes a new, stopped ``video_stream`` row with the same id, owned by
``--owner-user-id``, with Lost & Found enabled; its ROI and rule overrides become its
``lost_found_camera_config``.

Media is read from MinIO by key, else from the BYTEA column, else (clips) from
``--clip-root``/``clip_path``. Statuses are lower-cased to InsightEye's vocabulary.

Re-runnable: rows keep their ids and are inserted with ON CONFLICT DO NOTHING, and media
is only uploaded for rows that have no path yet.

The target database and S3 bucket come from InsightEye's settings (.env), unless
``--target-dsn`` is given.
"""

import argparse
import asyncio
import json
import logging
import os
import sys
from typing import Any, Dict, Optional
from uuid import UUID

import asyncpg

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("migrate_lost_found")

LOST_FOUND_SETTING_KEYS = {
    "static_tolerance_px", "static_tolerance_ratio", "static_window_seconds", "owner_distance_px",
    "abandon_distance_px", "owner_radius_heights", "abandon_radius_heights", "stand_in_seconds",
    "abandon_seconds", "require_owner", "min_object_size_px", "track_max_misses", "reid_iou",
    "event_cooldown_seconds",
}


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _s3_key(workspace_id: UUID, stream_id: Optional[UUID], kind: str, object_id: Any, ext: str) -> str:
    camera = str(stream_id) if stream_id else "unassigned"
    return f"lost-found/{workspace_id}/{camera}/{kind}/{object_id}.{ext}"


class Media:
    """Reads the source media (MinIO, BYTEA or disk) and writes it to InsightEye's S3."""

    def __init__(self, args) -> None:
        import boto3
        from app.config.settings import config

        self.dry_run = args.dry_run
        self.clip_root = args.clip_root
        self.minio_bucket = args.minio_bucket
        self.minio = None
        if args.minio_endpoint:
            self.minio = boto3.client(
                "s3", endpoint_url=args.minio_endpoint,
                aws_access_key_id=args.minio_access_key, aws_secret_access_key=args.minio_secret_key,
                region_name="us-east-1",
            )
        self.bucket = config.s3_bucket_name
        self.s3 = boto3.client(
            "s3", aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key, region_name=config.aws_region,
        ) if self.bucket else None
        self.uploaded = 0
        self.missing = 0

    async def read(self, key: Optional[str], blob: Optional[bytes], disk_path: Optional[str] = None) -> Optional[bytes]:
        if key and self.minio is not None:
            try:
                obj = await asyncio.to_thread(self.minio.get_object, Bucket=self.minio_bucket, Key=key)
                return await asyncio.to_thread(obj["Body"].read)
            except Exception as e:
                logger.debug("MinIO read of %s failed: %s", key, e)
        if blob:
            return bytes(blob)
        if disk_path:
            path = disk_path if os.path.isabs(disk_path) else os.path.join(self.clip_root or ".", disk_path)
            if os.path.isfile(path):
                return await asyncio.to_thread(lambda: open(path, "rb").read())
        return None

    async def copy(self, key: Optional[str], blob: Optional[bytes], target_key: str, content_type: str,
                   disk_path: Optional[str] = None) -> Optional[str]:
        """Copy one object; returns its s3:// URI (None when there is nothing to copy)."""
        data = await self.read(key, blob, disk_path)
        if not data:
            if key or blob or disk_path:
                self.missing += 1
            return None
        if self.dry_run:
            self.uploaded += 1
            return f"s3://{self.bucket or 'dry-run'}/{target_key}"
        if self.s3 is None:
            raise RuntimeError("S3_BUCKET_NAME / AWS credentials are not configured")
        await asyncio.to_thread(
            self.s3.put_object, Bucket=self.bucket, Key=target_key, Body=data, ContentType=content_type,
        )
        self.uploaded += 1
        return f"s3://{self.bucket}/{target_key}"


def _target_dsn(args) -> str:
    if args.target_dsn:
        return args.target_dsn
    from app.config.settings import config
    return (f"postgresql://{config.postgres_user}:{config.postgres_password}"
            f"@{config.db_host}:{config.db_port}/{config.postgres_db}")


async def migrate(args) -> None:
    workspace_id = UUID(args.workspace_id)
    owner_id = UUID(args.owner_user_id)
    camera_map: Dict[str, str] = {}
    if args.camera_map:
        with open(args.camera_map, "r", encoding="utf-8") as fh:
            camera_map = {str(k): str(v) for k, v in json.load(fh).items()}

    src = await asyncpg.connect(args.source_dsn)
    # statement_cache_size=0: the target is usually reached through PgBouncer.
    dst = await asyncpg.connect(_target_dsn(args), statement_cache_size=0)
    media = Media(args)
    counts = {"cameras_created": 0, "cameras_mapped": 0, "events": 0, "found_items": 0,
              "lost_items": 0, "matches": 0}
    try:
        ws = await dst.fetchrow("SELECT workspace_id FROM workspaces WHERE workspace_id = $1", workspace_id)
        if not ws:
            raise SystemExit(f"Workspace {workspace_id} not found in the target database")
        member = await dst.fetchrow(
            "SELECT 1 FROM users u WHERE u.user_id = $1 AND (u.role = 'superadmin' OR EXISTS "
            "(SELECT 1 FROM workspace_members m WHERE m.user_id = u.user_id AND m.workspace_id = $2))",
            owner_id, workspace_id,
        )
        if not member:
            raise SystemExit(f"User {owner_id} is not a member of workspace {workspace_id}")

        tx = dst.transaction()
        await tx.start()
        try:
            # ---------------- cameras
            stream_of: Dict[UUID, UUID] = {}
            for cam in await src.fetch("SELECT * FROM camera ORDER BY created_at"):
                cid = cam["camera_id"]
                if str(cid) in camera_map:
                    stream_id = UUID(camera_map[str(cid)])
                    ok = await dst.fetchrow(
                        "SELECT 1 FROM video_stream WHERE stream_id = $1 AND workspace_id = $2", stream_id, workspace_id,
                    )
                    if not ok:
                        raise SystemExit(f"Mapped camera {stream_id} is not in workspace {workspace_id}")
                    await dst.execute(
                        "UPDATE video_stream SET is_lost_found_camera = TRUE, "
                        "detection_models = ARRAY(SELECT DISTINCT unnest(detection_models || ARRAY['lost_found'])) "
                        "WHERE stream_id = $1",
                        stream_id,
                    )
                    counts["cameras_mapped"] += 1
                else:
                    stream_id = cid
                    if len(cam["rtsp_url"] or "") > 255:
                        logger.warning("Camera %s: source URL longer than 255 chars, truncated", cam["name"])
                    created = await dst.fetchval(
                        """
                        INSERT INTO video_stream (stream_id, workspace_id, user_id, name, path, type, status,
                                                  is_streaming, detection_models, location, is_lost_found_camera,
                                                  auto_retry_enabled, created_at, updated_at)
                        VALUES ($1, $2, $3, $4, $5, $6, 'inactive', FALSE, ARRAY['lost_found'], $7, TRUE,
                                FALSE, $8, $9)
                        ON CONFLICT (stream_id) DO NOTHING
                        RETURNING stream_id
                        """,
                        stream_id, workspace_id, owner_id, (cam["name"] or "camera")[:50],
                        (cam["rtsp_url"] or "")[:255],
                        "rtsp" if (cam["rtsp_url"] or "").startswith("rtsp") else "other",
                        (cam["location"] or None) and cam["location"][:100], cam["created_at"], cam["updated_at"],
                    )
                    counts["cameras_created"] += 1 if created else 0
                stream_of[cid] = stream_id

                roi = _json(cam["roi"])
                settings = {k: v for k, v in (_json(cam["settings"]) or {}).items() if k in LOST_FOUND_SETTING_KEYS}
                if roi or settings:
                    # The standalone app drew the ROI in frame pixels; with no reference
                    # size the engine uses the points as-is.
                    await dst.execute(
                        """
                        INSERT INTO lost_found_camera_config (stream_id, workspace_id, roi, settings, is_active)
                        VALUES ($1, $2, $3::jsonb, $4::jsonb, TRUE)
                        ON CONFLICT (stream_id) DO NOTHING
                        """,
                        stream_id, workspace_id, json.dumps(roi) if roi else None, json.dumps(settings),
                    )

            camera_names = {c["camera_id"]: c["name"] for c in await src.fetch("SELECT camera_id, name FROM camera")}

            # ---------------- events
            for ev in await src.fetch("SELECT * FROM abandoned_event ORDER BY abandoned_at"):
                stream_id = stream_of.get(ev["camera_id"])
                created = await dst.fetchval(
                    """
                    INSERT INTO lost_found_events (event_id, stream_id, workspace_id, camera_name, track_key,
                        class_name, confidence, bbox, owner_track_id, first_seen_at, static_since,
                        event_timestamp, status, created_at,
                        acknowledged_at, resolved_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11, $12, $13::varchar, $14,
                            CASE WHEN $13::varchar <> 'open' THEN $12::timestamptz END,
                            CASE WHEN $13::varchar IN ('resolved', 'false_positive') THEN $12::timestamptz END)
                    ON CONFLICT DO NOTHING
                    RETURNING event_id
                    """,
                    ev["event_id"], stream_id, workspace_id, (camera_names.get(ev["camera_id"]) or "")[:100] or None,
                    ev["track_key"], ev["class_name"], ev["confidence"],
                    json.dumps(_json(ev["bbox"])) if ev["bbox"] else None, ev["owner_track_id"],
                    ev["first_seen_at"], ev["static_since"], ev["abandoned_at"], (ev["status"] or "OPEN").lower(),
                    ev["created_at"],
                )
                counts["events"] += 1 if created else 0
                row = await dst.fetchrow(
                    "SELECT snapshot_path, video_path FROM lost_found_events WHERE event_id = $1", ev["event_id"],
                )
                if row and not row["snapshot_path"]:
                    path = await media.copy(ev["frame_key"], ev["frame_jpeg"],
                                            _s3_key(workspace_id, stream_id, "frames", ev["event_id"], "jpg"),
                                            "image/jpeg")
                    if path:
                        await dst.execute("UPDATE lost_found_events SET snapshot_path = $2 WHERE event_id = $1",
                                          ev["event_id"], path)
                if row and not row["video_path"]:
                    path = await media.copy(ev["clip_key"], None,
                                            _s3_key(workspace_id, stream_id, "clips", ev["event_id"], "mp4"),
                                            "video/mp4", disk_path=ev["clip_path"])
                    if path:
                        await dst.execute("UPDATE lost_found_events SET video_path = $2 WHERE event_id = $1",
                                          ev["event_id"], path)

            # ---------------- found items
            for f in await src.fetch("SELECT * FROM found_item ORDER BY found_at"):
                stream_id = stream_of.get(f["camera_id"])
                created = await dst.fetchval(
                    """
                    INSERT INTO lost_found_found_items (found_id, event_id, stream_id, workspace_id, class_name,
                        dominant_color, phash, color_hist, found_at, status, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    ON CONFLICT DO NOTHING
                    RETURNING found_id
                    """,
                    f["found_id"], f["event_id"], stream_id, workspace_id, f["class_name"], f["dominant_color"],
                    f["phash"], f["color_hist"], f["found_at"], (f["status"] or "UNCLAIMED").lower(), f["created_at"],
                )
                counts["found_items"] += 1 if created else 0
                crop = await dst.fetchval("SELECT crop_path FROM lost_found_found_items WHERE found_id = $1",
                                          f["found_id"])
                if crop is None:
                    path = await media.copy(f["crop_key"], f["crop_jpeg"],
                                            _s3_key(workspace_id, stream_id, "crops", f["found_id"], "jpg"),
                                            "image/jpeg")
                    if path:
                        await dst.execute("UPDATE lost_found_found_items SET crop_path = $2 WHERE found_id = $1",
                                          f["found_id"], path)

            # ---------------- lost reports
            for lost in await src.fetch("SELECT * FROM lost_item ORDER BY reported_at"):
                created = await dst.fetchval(
                    """
                    INSERT INTO lost_found_lost_items (lost_id, workspace_id, reporter_name, reporter_email,
                        description, class_name, dominant_color, phash, color_hist, lost_after, reported_at,
                        status, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                    ON CONFLICT DO NOTHING
                    RETURNING lost_id
                    """,
                    lost["lost_id"], workspace_id, lost["reporter_name"], lost["reporter_email"],
                    lost["description"], lost["class_name"], lost["dominant_color"], lost["phash"],
                    lost["color_hist"], lost["lost_after"], lost["reported_at"],
                    (lost["status"] or "OPEN").lower(), lost["created_at"],
                )
                counts["lost_items"] += 1 if created else 0
                photo = await dst.fetchval("SELECT photo_path FROM lost_found_lost_items WHERE lost_id = $1",
                                           lost["lost_id"])
                if photo is None:
                    path = await media.copy(lost["photo_key"], lost["photo_jpeg"],
                                            f"lost-found/{workspace_id}/lost/{lost['lost_id']}.jpg", "image/jpeg")
                    if path:
                        await dst.execute("UPDATE lost_found_lost_items SET photo_path = $2 WHERE lost_id = $1",
                                          lost["lost_id"], path)

            # ---------------- matches
            for m in await src.fetch("SELECT * FROM match_result ORDER BY created_at"):
                created = await dst.fetchval(
                    """
                    INSERT INTO lost_found_matches (match_id, lost_id, found_id, workspace_id, score, method,
                                                    confirmed, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT DO NOTHING
                    RETURNING match_id
                    """,
                    m["match_id"], m["lost_id"], m["found_id"], workspace_id, m["score"] or 0.0,
                    m["method"] or "rule_based", bool(m["confirmed"]), m["created_at"],
                )
                counts["matches"] += 1 if created else 0

            if args.dry_run:
                await tx.rollback()
            else:
                await tx.commit()
        except BaseException:
            await tx.rollback()
            raise
    finally:
        await src.close()
        await dst.close()

    mode = "DRY RUN (rolled back, nothing uploaded)" if args.dry_run else "done"
    logger.info("%s: %s; media copied: %d, media missing: %d", mode,
                ", ".join(f"{k}={v}" for k, v in counts.items()), media.uploaded, media.missing)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source-dsn", required=True, help="Standalone lost-found Postgres DSN")
    p.add_argument("--target-dsn", help="InsightEye Postgres DSN (default: from .env settings)")
    p.add_argument("--workspace-id", required=True, help="InsightEye workspace to import into")
    p.add_argument("--owner-user-id", required=True, help="Owner of cameras created by the import")
    p.add_argument("--camera-map", help='JSON file {"<lost-found camera_id>": "<insighteye stream_id>"}')
    p.add_argument("--minio-endpoint", default=os.getenv("LF_MINIO_ENDPOINT"), help="e.g. http://localhost:9000")
    p.add_argument("--minio-access-key", default=os.getenv("LF_MINIO_ACCESS_KEY"))
    p.add_argument("--minio-secret-key", default=os.getenv("LF_MINIO_SECRET_KEY"))
    p.add_argument("--minio-bucket", default=os.getenv("LF_MINIO_BUCKET", "lostfound"))
    p.add_argument("--clip-root", help="Directory clip_path is relative to (the lost-found backend dir)")
    p.add_argument("--dry-run", action="store_true", help="Read and map everything, roll back, upload nothing")
    asyncio.run(migrate(p.parse_args()))


if __name__ == "__main__":
    main()
