"""
One-off migration: re-encode all MPEG-4 Part 2 videos in S3 to H.264.

Usage:
    python scripts/migrate_videos_to_h264.py            # migrate everything
    python scripts/migrate_videos_to_h264.py --limit 1  # test on one video first

Reads AWS config from environment variables (or .env via python-dotenv if present):
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET_NAME
"""

import argparse
import logging
import os
import subprocess
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.config import Config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

AWS_ACCESS_KEY_ID     = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_REGION            = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET_NAME        = os.environ["S3_BUCKET_NAME"]

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
    config=Config(signature_version="s3v4", s3={"addressing_style": "virtual"}),
)


def list_video_keys(prefix="videos/"):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(".mp4"):
                yield key


def migrate_one(key: str) -> bool:
    tmp = os.path.join(tempfile.gettempdir(), f"{uuid.uuid4()}.mp4")
    try:
        presigned_url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": S3_BUCKET_NAME, "Key": key},
            ExpiresIn=3600,
        )

        result = subprocess.run(
            [
                "ffmpeg", "-y",
                "-i", presigned_url,
                "-c:v", "libx264",
                "-preset", "fast",
                "-crf", "23",
                "-pix_fmt", "yuv420p",
                "-movflags", "+faststart",
                "-c:a", "copy",
                tmp,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            timeout=300,
        )

        if result.returncode != 0:
            logger.error(f"ffmpeg failed for {key}: {result.stderr.decode()[-500:]}")
            return False

        s3.upload_file(
            Filename=tmp,
            Bucket=S3_BUCKET_NAME,
            Key=key,
            ExtraArgs={"ContentType": "video/mp4"},
        )
        logger.info(f"Migrated: {key}")
        return True

    except Exception as e:
        logger.error(f"Error migrating {key}: {e}")
        return False
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser(description="Migrate S3 videos to H.264")
    parser.add_argument("--limit", type=int, default=None, help="Max videos to process")
    parser.add_argument("--workers", type=int, default=4, help="Parallel workers")
    args = parser.parse_args()

    keys = list(list_video_keys())
    if args.limit:
        keys = keys[: args.limit]

    logger.info(f"Migrating {len(keys)} videos with {args.workers} workers …")

    success = failure = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(migrate_one, k): k for k in keys}
        for fut in as_completed(futures):
            if fut.result():
                success += 1
            else:
                failure += 1

    logger.info(f"Done — {success} succeeded, {failure} failed")


if __name__ == "__main__":
    main()
