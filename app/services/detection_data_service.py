# app/services/detection_data_service.py

from typing import Dict, Any, Optional, List, Union
from uuid import UUID, uuid4
import logging
import numpy as np
from app.services.qdrant_service import qdrant_service
from app.services.postgres_service import postgres_service
from app.utils import get_workspace_qdrant_collection_name
from app.schemas import LocationSearchQuery

logger = logging.getLogger(__name__)

class DetectionDataService:
    """service for handling detection data across PostgreSQL and Qdrant"""
    
    def __init__(self):
        self.qdrant_service = qdrant_service
        self.postgres_service = postgres_service
    
    async def insert_detection_data(
        self,
        stream_id: Union[str, UUID],
        workspace_id: Union[str, UUID],
        user_id: Union[str, UUID],
        camera_name: str,
        username: str,
        person_count: int,
        male_count: int,
        female_count: int,
        fire_status: str,
        frame: Optional[np.ndarray] = None,
        location_info: Optional[Dict[str, Any]] = None,
        save_frame_in_postgres: bool = False,
        save_metadata_in_qdrant: bool = False
    ) -> bool:
        """
        Insert detection data into both PostgreSQL and Qdrant with synchronized IDs.
        
        Strategy:
        - Generate single result_id
        - Save metadata in PostgreSQL (stream_results)
        - Save frame in Qdrant (with optional metadata)
        - Optionally save frame in PostgreSQL too (stream_frames)
        
        Returns:
            bool: Success status
        """
        stream_id = stream_id if isinstance(stream_id, UUID) else UUID(str(stream_id))
        workspace_id = workspace_id if isinstance(workspace_id, UUID) else UUID(str(workspace_id))
        user_id = user_id if isinstance(user_id, UUID) else UUID(str(user_id))
    
        result_id = uuid4()
        
        try:
            # 1. Insert metadata into PostgreSQL first
            pg_success = await self.postgres_service.insert_detection_data(
                stream_id=stream_id,
                workspace_id=workspace_id,
                user_id=user_id,
                camera_name=camera_name,
                username=username,
                person_count=person_count,
                male_count=male_count,
                female_count=female_count,
                fire_status=fire_status,
                frame=frame if save_frame_in_postgres else None,
                location_info=location_info,
                save_frame=save_frame_in_postgres,
                result_id=result_id
            )
            
            if not pg_success:
                logger.error(f"Failed to insert detection data into PostgreSQL for result_id {result_id}")
                return False
            
            # 2. Insert frame into Qdrant
            if frame is not None:
                qdrant_success = await self.qdrant_service.insert_detection_data(
                    username=username,
                    camera_id_str=str(stream_id),
                    camera_name=camera_name,
                    count=person_count if save_metadata_in_qdrant else 0,
                    male_count=male_count if save_metadata_in_qdrant else 0,
                    female_count=female_count if save_metadata_in_qdrant else 0,
                    fire_status=fire_status if save_metadata_in_qdrant else "no detection",
                    frame=frame,
                    workspace_id=workspace_id,
                    location_info=location_info if save_metadata_in_qdrant else None,
                    result_id=result_id
                )
                
                if not qdrant_success:
                    logger.warning(f"Failed to insert frame into Qdrant for result_id {result_id}")
            
            logger.info(f"Successfully inserted detection data with ID: {result_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error in detection data insertion: {e}", exc_info=True)
            return False
    
    async def retrieve_detection_data(
        self,
        workspace_id: UUID,
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str,
        camera_id: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        location: Optional[str] = None,
        area: Optional[str] = None,
        building: Optional[str] = None,
        floor_level: Optional[str] = None,
        zone: Optional[str] = None,
        page: int = 1,
        per_page: Optional[int] = 10,
        include_frame: bool = True
    ) -> Dict[str, Any]:
        """
        Retrieve detection data from both PostgreSQL and Qdrant with filtering.
        
        This method combines metadata from PostgreSQL with frames from Qdrant,
        supporting all the same filters as workspace_search_results_with_location.
        
        Args:
            workspace_id: Target workspace UUID
            user_system_role: System role of requesting user
            user_workspace_role: Workspace role of requesting user  
            requesting_username: Username of requester
            camera_id: Optional list of camera IDs to filter
            start_date: Start date for filtering (YYYY-MM-DD)
            end_date: End date for filtering (YYYY-MM-DD)
            start_time: Start time for filtering (HH:MM:SS)
            end_time: End time for filtering (HH:MM:SS)
            location: Location filter
            area: Area filter
            building: Building filter
            floor_level: Floor level filter
            zone: Zone filter
            page: Page number for pagination
            per_page: Results per page (None for all results)
            include_frame: Whether to include frame data from Qdrant
            
        Returns:
            Dictionary containing combined results with metadata and frames
        """
        try:
            # Build search query
            search_query = LocationSearchQuery(
                camera_id=camera_id,
                start_date=start_date,
                end_date=end_date,
                start_time=start_time,
                end_time=end_time,
                location=location,
                area=area,
                building=building,
                floor_level=floor_level,
                zone=zone
            )
            
            # Get metadata from PostgreSQL
            pg_results = await self.postgres_service.search_workspace_data(
                workspace_id=workspace_id,
                search_query=search_query,
                user_system_role=user_system_role,
                user_workspace_role=user_workspace_role,
                requesting_username=requesting_username,
                page=page,
                per_page=per_page,
                include_frame=False  # Don't get frames from PostgreSQL
            )
            
            if not pg_results or not pg_results.get("data"):
                return {
                    "data": [],
                    "current_page": page,
                    "num_of_pages": 0,
                    "total_count": 0,
                    "per_page": per_page,
                    "source": "(PostgreSQL + Qdrant)",
                    "frames_included": include_frame
                }
            
            # If frames requested, fetch from Qdrant
            if include_frame:
                result_ids = [item["id"] for item in pg_results["data"]]
                frames_map = await self._fetch_frames_from_qdrant(
                    workspace_id=workspace_id,
                    result_ids=result_ids
                )
                
                # Merge frames with metadata
                for item in pg_results["data"]:
                    result_id = item["id"]
                    if result_id in frames_map:
                        item["frame"] = frames_map[result_id]
                    else:
                        item["frame"] = None
                        logger.debug(f"No frame found in Qdrant for result_id {result_id}")
            
            # Add source information
            pg_results["source"] = "(PostgreSQL metadata + Qdrant frames)"
            pg_results["frames_included"] = include_frame
            pg_results["filters_applied"] = {
                "camera_id": camera_id,
                "date_range": {
                    "start_date": start_date,
                    "end_date": end_date,
                    "start_time": start_time,
                    "end_time": end_time
                },
                "location_filters": {
                    "location": location,
                    "area": area,
                    "building": building,
                    "floor_level": floor_level,
                    "zone": zone
                }
            }
            
            return pg_results
            
        except Exception as e:
            logger.error(f"Error retrieving detection data: {e}", exc_info=True)
            raise
    
    async def retrieve_single_detection(
        self,
        result_id: UUID,
        workspace_id: UUID,
        include_frame: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve a single detection data point by result_id.
        
        Args:
            result_id: Unique ID of the detection result
            workspace_id: Workspace ID for validation
            include_frame: Whether to include frame from Qdrant
            
        Returns:
            Dictionary with combined metadata and frame, or None if not found
        """
        try:
            # Get metadata from PostgreSQL
            metadata_query = """
                SELECT sr.*, u.username as owner_username
                FROM stream_results sr
                JOIN users u ON sr.user_id = u.user_id
                WHERE sr.result_id = $1 AND sr.workspace_id = $2
            """
            
            metadata = await self.postgres_service.db_manager.execute_query(
                metadata_query, (result_id, workspace_id), fetch_one=True
            )
            
            if not metadata:
                logger.warning(f"No metadata found for result_id {result_id} in workspace {workspace_id}")
                return None
            
            # Format metadata
            result = {
                "result_id": str(metadata['result_id']),
                "stream_id": str(metadata['stream_id']),
                "workspace_id": str(metadata['workspace_id']),
                "user_id": str(metadata['user_id']),
                "camera_id": metadata['camera_id'],
                "camera_name": metadata['camera_name'],
                "timestamp": metadata['timestamp'].isoformat() if metadata['timestamp'] else None,
                "date": metadata['date'].isoformat() if metadata['date'] else None,
                "time": metadata['time'].isoformat() if metadata['time'] else None,
                "person_count": metadata['person_count'],
                "male_count": metadata['male_count'],
                "female_count": metadata['female_count'],
                "fire_status": metadata['fire_status'],
                "username": metadata['username'],
                "owner_username": metadata.get('owner_username'),
                "location": metadata['location'],
                "area": metadata['area'],
                "building": metadata['building'],
                "floor_level": metadata['floor_level'],
                "zone": metadata['zone'],
                "latitude": float(metadata['latitude']) if metadata['latitude'] else None,
                "longitude": float(metadata['longitude']) if metadata['longitude'] else None,
                "created_at": metadata['created_at'].isoformat() if metadata['created_at'] else None,
                "source": "(PostgreSQL + Qdrant)"
            }
            
            # Get frame from Qdrant if requested
            if include_frame:
                frames_map = await self._fetch_frames_from_qdrant(
                    workspace_id=workspace_id,
                    result_ids=[str(result_id)]
                )
                result["frame"] = frames_map.get(str(result_id))
            
            return result
            
        except Exception as e:
            logger.error(f"Error retrieving single detection data: {e}", exc_info=True)
            return None
    
    async def _fetch_frames_from_qdrant(
        self,
        workspace_id: UUID,
        result_ids: List[str]
    ) -> Dict[str, str]:
        """
        Fetch frames from Qdrant by result IDs.
        
        Args:
            workspace_id: Workspace UUID
            result_ids: List of result IDs to fetch
            
        Returns:
            Dictionary mapping result_id to frame_base64
        """
        frames_map = {}
        
        if not result_ids:
            return frames_map
        
        try:
            client = self.qdrant_service.get_client()
            collection_name = get_workspace_qdrant_collection_name(workspace_id)
            
            # Batch retrieve points by IDs
            points = client.retrieve(
                collection_name=collection_name,
                ids=result_ids,
                with_payload=["frame_base64"],
                with_vectors=False
            )
            
            for point in points:
                if point.payload and point.payload.get("frame_base64"):
                    frames_map[str(point.id)] = point.payload["frame_base64"]
            
            logger.debug(f"Retrieved {len(frames_map)} frames from Qdrant out of {len(result_ids)} requested")
            
        except Exception as e:
            logger.error(f"Error fetching frames from Qdrant: {e}", exc_info=True)
        
        return frames_map

# Global instance
detection_data_service = DetectionDataService()