# app/services/feature_service.py
"""Feature access control.

A superadmin opens/closes features per user and per workspace
(users.enabled_features, workspaces.enabled_features). A user gets a feature
only when it is enabled on both their own row and their active workspace;
superadmins always get everything. Camera detection is gated the same way
using the camera owner and the camera's workspace.
"""
import logging
import time
from typing import Dict, FrozenSet, Iterable, Optional, Tuple
from uuid import UUID

from fastapi import Depends, HTTPException, status

from app.services.database import db_manager
from app.services.session_service import session_manager

logger = logging.getLogger(__name__)

FEATURES: Tuple[str, ...] = (
    "fire_smoke",
    "people_counting",
    "shoplifting",
    "no_entry_zone",
    "blocked_exit",
    "gender",
)
ALL_FEATURES: FrozenSet[str] = frozenset(FEATURES)

# Detection re-reads a camera's features at most this often.
CAMERA_FEATURES_TTL_SECONDS = 30.0


class FeatureService:
    def __init__(self, db):
        self.db = db
        self._camera_cache: Dict[str, Tuple[float, FrozenSet[str]]] = {}

    async def effective_features(self, user_data: Dict, workspace_id: Optional[UUID]) -> FrozenSet[str]:
        """Features the user may use in the given workspace."""
        if user_data.get("role") == "superadmin":
            return ALL_FEATURES
        if not workspace_id:
            return frozenset()
        row = await self.db.execute_query(
            """
            SELECT ARRAY(
                SELECT unnest(u.enabled_features)
                INTERSECT
                SELECT unnest(w.enabled_features)
            ) AS features
            FROM users u, workspaces w
            WHERE u.user_id = $1 AND w.workspace_id = $2
            """,
            (UUID(str(user_data["user_id"])), workspace_id),
            fetch_one=True,
        )
        return frozenset(row["features"] or []) if row else frozenset()

    async def camera_features(self, stream_id: UUID) -> FrozenSet[str]:
        """Features allowed for detection on a camera: owner ∩ camera workspace.

        A camera owned by a superadmin is only limited by its workspace. Cached
        for CAMERA_FEATURES_TTL_SECONDS so the frame loop can call it freely.
        Fails open (all features) on DB errors so a DB hiccup never silently
        switches detection off.
        """
        key = str(stream_id)
        now = time.monotonic()
        cached = self._camera_cache.get(key)
        if cached and now - cached[0] < CAMERA_FEATURES_TTL_SECONDS:
            return cached[1]
        try:
            row = await self.db.execute_query(
                """
                SELECT ARRAY(
                    SELECT unnest(CASE WHEN u.role = 'superadmin'
                                       THEN w.enabled_features ELSE u.enabled_features END)
                    INTERSECT
                    SELECT unnest(w.enabled_features)
                ) AS features
                FROM video_stream vs
                JOIN users u ON u.user_id = vs.user_id
                JOIN workspaces w ON w.workspace_id = vs.workspace_id
                WHERE vs.stream_id = $1
                """,
                (UUID(key),),
                fetch_one=True,
            )
            features = frozenset(row["features"] or []) if row else frozenset()
        except Exception as e:
            logger.error(f"camera_features lookup failed for {key}: {e}")
            return cached[1] if cached else ALL_FEATURES
        self._camera_cache[key] = (now, features)
        return features

    def invalidate_cameras(self) -> None:
        """Drop the camera cache so a superadmin toggle applies on the next check."""
        self._camera_cache.clear()

    async def recipients_with_feature(self, user_ids: Iterable, workspace_id: UUID, feature: str) -> set:
        """Subset of user_ids that may receive alerts for `feature` in this workspace."""
        ids = [UUID(str(u)) for u in user_ids]
        if not ids:
            return set()
        rows = await self.db.execute_query(
            """
            SELECT u.user_id FROM users u, workspaces w
            WHERE u.user_id = ANY($1::uuid[]) AND w.workspace_id = $2
              AND (u.role = 'superadmin'
                   OR ($3 = ANY(u.enabled_features) AND $3 = ANY(w.enabled_features)))
            """,
            (ids, workspace_id, feature),
            fetch_all=True,
        ) or []
        return {str(r["user_id"]) for r in rows}

    async def set_user_features(self, user_id: UUID, features: Iterable[str]) -> Optional[str]:
        row = await self.db.execute_query(
            "UPDATE users SET enabled_features = $1 WHERE user_id = $2 RETURNING username",
            (sorted(set(features)), user_id),
            fetch_one=True,
        )
        self.invalidate_cameras()
        return row["username"] if row else None

    async def set_workspace_features(self, workspace_id: UUID, features: Iterable[str]) -> Optional[str]:
        row = await self.db.execute_query(
            """UPDATE workspaces SET enabled_features = $1, updated_at = CURRENT_TIMESTAMP
               WHERE workspace_id = $2 RETURNING name""",
            (sorted(set(features)), workspace_id),
            fetch_one=True,
        )
        self.invalidate_cameras()
        return row["name"] if row else None


feature_service = FeatureService(db_manager)


async def current_workspace_id(user_data: Dict) -> Optional[UUID]:
    from app.services.workspace_service import workspace_service
    try:
        _, workspace_id = await workspace_service.get_user_and_workspace(user_data["username"])
        return workspace_id
    except HTTPException as e:
        if e.status_code == status.HTTP_404_NOT_FOUND:
            return None
        raise


def require_feature(feature: str):
    """FastAPI dependency: 403 unless `feature` is enabled for the caller's active workspace."""
    if feature not in ALL_FEATURES:
        raise ValueError(f"Unknown feature '{feature}'")

    async def _check(
        current_user: Dict = Depends(session_manager.get_current_user_full_data_dependency),
    ) -> None:
        workspace_id = await current_workspace_id(current_user)
        if feature not in await feature_service.effective_features(current_user, workspace_id):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Feature '{feature}' is not enabled for your account or workspace.",
            )

    return _check
