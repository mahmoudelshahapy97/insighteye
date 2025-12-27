# app/services/unified_data_service.py
"""
Unified Data Service - Single interface for both Qdrant and Elasticsearch backends.
Automatically routes operations to the appropriate backend based on configuration.
"""

from typing import List, Dict, Any, Optional, Union, Tuple
from uuid import UUID
from datetime import datetime
import logging
from abc import ABC, abstractmethod

from app.services.qdrant_service import qdrant_service
from app.services.elasticsearch_service import elasticsearch_service
from app.schemas import (
    SearchQuery, TimestampRangeResponse,
    DeleteDataRequest,
    StreamUpdate
)
from app.config.settings import config

logger = logging.getLogger(__name__)


class DataBackend(ABC):
    """Abstract base class for data backends"""
    
    @abstractmethod
    async def insert_detection_data(self, **kwargs) -> bool:
        pass
    
    @abstractmethod
    async def batch_insert_detection_data(self, **kwargs) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    async def search_workspace_data(self, **kwargs) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    async def get_workspace_statistics(self, **kwargs) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    async def delete_data(self, **kwargs) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    async def get_timestamp_range(self, **kwargs) -> TimestampRangeResponse:
        pass
    
    @abstractmethod
    async def export_workspace_data(self, **kwargs) -> Tuple[bytes, str, str]:
        pass
    
    @abstractmethod
    async def get_prediction_data(self, **kwargs) -> List[Dict[str, Any]]:
        pass
    
    @abstractmethod
    async def update_camera_metadata(self, **kwargs) -> None:
        pass
    
    @abstractmethod
    async def delete_user_camera_data(self, **kwargs) -> Dict:
        pass
    
    @abstractmethod
    async def get_location_data(self, **kwargs) -> List[Dict[str, Any]]:
        pass
    
    @abstractmethod
    async def get_location_analytics(self, **kwargs) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    async def bulk_update_metadata(self, **kwargs) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    async def preview_metadata_update(self, **kwargs) -> Dict[str, Any]:
        pass


class QdrantBackend(DataBackend):
    """Qdrant implementation of data backend"""
    
    def __init__(self):
        self.service = qdrant_service
    
    async def insert_detection_data(self, **kwargs) -> bool:
        return await self.service.insert_detection_data(**kwargs)
    
    async def batch_insert_detection_data(self, **kwargs) -> Dict[str, Any]:
        return await self.service.batch_insert_detection_data(**kwargs)
    
    async def search_workspace_data(self, **kwargs) -> Dict[str, Any]:
        return await self.service.search_workspace_data(**kwargs)
    
    async def get_workspace_statistics(self, **kwargs) -> Dict[str, Any]:
        return await self.service.get_workspace_statistics(**kwargs)
    
    async def delete_data(self, **kwargs) -> Dict[str, Any]:
        return await self.service.delete_data(**kwargs)
    
    async def get_timestamp_range(self, **kwargs) -> TimestampRangeResponse:
        return await self.service.get_timestamp_range(**kwargs)
    
    async def export_workspace_data(self, **kwargs) -> Tuple[bytes, str, str]:
        return await self.service.export_workspace_data(**kwargs)
    
    async def get_prediction_data(self, **kwargs) -> List[Dict[str, Any]]:
        return await self.service.get_prediction_data(**kwargs)
    
    async def update_camera_metadata(self, **kwargs) -> None:
        return await self.service.update_camera_metadata(**kwargs)
    
    async def delete_user_camera_data(self, **kwargs) -> Dict:
        return await self.service.delete_user_camera_data(**kwargs)
    
    async def get_location_data(self, **kwargs) -> List[Dict[str, Any]]:
        return await self.service.get_location_data(**kwargs)
    
    async def get_location_analytics(self, **kwargs) -> Dict[str, Any]:
        return await self.service.get_location_analytics(**kwargs)
    
    async def bulk_update_metadata(self, **kwargs) -> Dict[str, Any]:
        return await self.service.bulk_update_metadata(**kwargs)
    
    async def preview_metadata_update(self, **kwargs) -> Dict[str, Any]:
        return await self.service.preview_metadata_update(**kwargs)


class ElasticsearchBackend(DataBackend):
    """Elasticsearch implementation of data backend"""
    
    def __init__(self):
        self.service = elasticsearch_service
    
    async def insert_detection_data(self, **kwargs) -> bool:
        return await self.service.insert_detection_data(**kwargs)
    
    async def batch_insert_detection_data(self, **kwargs) -> Dict[str, Any]:
        return await self.service.batch_insert_detection_data(**kwargs)
    
    async def search_workspace_data(self, **kwargs) -> Dict[str, Any]:
        return await self.service.search_workspace_data(**kwargs)
    
    async def get_workspace_statistics(self, **kwargs) -> Dict[str, Any]:
        return await self.service.get_workspace_statistics(**kwargs)
    
    async def delete_data(self, **kwargs) -> Dict[str, Any]:
        return await self.service.delete_data(**kwargs)
    
    async def get_timestamp_range(self, **kwargs) -> TimestampRangeResponse:
        return await self.service.get_timestamp_range(**kwargs)
    
    async def export_workspace_data(self, **kwargs) -> Tuple[bytes, str, str]:
        return await self.service.export_workspace_data(**kwargs)
    
    async def get_prediction_data(self, **kwargs) -> List[Dict[str, Any]]:
        return await self.service.get_prediction_data(**kwargs)
    
    async def update_camera_metadata(self, **kwargs) -> None:
        return await self.service.update_camera_metadata(**kwargs)
    
    async def delete_user_camera_data(self, **kwargs) -> Dict:
        return await self.service.delete_user_camera_data(**kwargs)
    
    async def get_location_data(self, **kwargs) -> List[Dict[str, Any]]:
        return await self.service.get_location_data(**kwargs)
    
    async def get_location_analytics(self, **kwargs) -> Dict[str, Any]:
        return await self.service.get_location_analytics(**kwargs)
    
    async def bulk_update_metadata(self, **kwargs) -> Dict[str, Any]:
        return await self.service.bulk_update_metadata(**kwargs)
    
    async def preview_metadata_update(self, **kwargs) -> Dict[str, Any]:
        return await self.service.preview_metadata_update(**kwargs)


class UnifiedDataService:
    """
    Unified service that provides a single interface for data operations.
    Automatically routes to the appropriate backend (Qdrant or Elasticsearch).
    """
    
    def __init__(self):
        self.backend_type = config.get("data_backend", "qdrant").lower()
        
        if self.backend_type == "elasticsearch":
            self.backend = ElasticsearchBackend()
            logger.info("Unified Data Service initialized with Elasticsearch backend")
        else:
            self.backend = QdrantBackend()
            logger.info("Unified Data Service initialized with Qdrant backend")
        
        # Expose common services regardless of backend
        self.workspace_service = self.backend.service.workspace_service
        self.db_manager = self.backend.service.db_manager
        self.user_manager = self.backend.service.user_manager
    
    @property
    def backend_name(self) -> str:
        """Get the name of the current backend"""
        return self.backend_type
    
    # ========== Detection Data Operations ==========
    
    async def insert_detection_data(
        self,
        username: str,
        camera_id_str: str,
        camera_name: str,
        count: int,
        male_count: int,
        female_count: int,
        fire_status: str,
        frame,
        workspace_id: UUID,
        location_info: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Insert detection data into the backend"""
        return await self.backend.insert_detection_data(
            username=username,
            camera_id_str=camera_id_str,
            camera_name=camera_name,
            count=count,
            male_count=male_count,
            female_count=female_count,
            fire_status=fire_status,
            frame=frame,
            workspace_id=workspace_id,
            location_info=location_info
        )
    
    async def batch_insert_detection_data(
        self,
        detection_batch: List[Dict[str, Any]],
        workspace_id: UUID
    ) -> Dict[str, Any]:
        """Batch insert detection data"""
        return await self.backend.batch_insert_detection_data(
            detection_batch=detection_batch,
            workspace_id=workspace_id
        )
    
    async def ensure_workspace_collection(self, workspace_id: UUID) -> bool:
        """Ensure workspace collection/index exists"""
        if self.backend_type == "elasticsearch":
            return await self.backend.service.ensure_workspace_index(workspace_id)
        else:
            return await self.backend.service.ensure_workspace_collection(workspace_id)
    
    # ========== Search Operations ==========
    
    async def search_workspace_data(
        self,
        workspace_id: UUID,
        search_query: SearchQuery,
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str,
        page: int = 1,
        per_page: Optional[int] = 10,
        base64: bool = True
    ) -> Dict[str, Any]:
        """Search workspace data"""
        return await self.backend.search_workspace_data(
            workspace_id=workspace_id,
            search_query=search_query,
            user_system_role=user_system_role,
            user_workspace_role=user_workspace_role,
            requesting_username=requesting_username,
            page=page,
            per_page=per_page,
            base64=base64
        )
    
    async def search_ordered_data(
        self,
        workspace_id: UUID,
        search_query: SearchQuery,
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str,
        page: int = 1,
        per_page: int = 10
    ) -> Dict[str, Any]:
        """Search with timestamp ordering"""
        return await self.backend.service.search_ordered_data(
            workspace_id=workspace_id,
            search_query=search_query,
            user_system_role=user_system_role,
            user_workspace_role=user_workspace_role,
            requesting_username=requesting_username,
            page=page,
            per_page=per_page
        )
    
    # ========== Statistics and Analytics ==========
    
    async def get_workspace_statistics(
        self,
        workspace_id: UUID,
        user_system_role: Optional[str] = None,
        user_workspace_role: Optional[str] = None,
        requesting_username: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get workspace statistics"""
        stats = await self.backend.get_workspace_statistics(
            workspace_id=workspace_id,
            user_system_role=user_system_role,
            user_workspace_role=user_workspace_role,
            requesting_username=requesting_username
        )
        # Add backend info
        stats["backend_type"] = self.backend_type
        return stats
    
    async def get_timestamp_range(
        self,
        workspace_id: UUID,
        camera_ids: Optional[List[str]],
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str
    ) -> TimestampRangeResponse:
        """Get timestamp range for data"""
        return await self.backend.get_timestamp_range(
            workspace_id=workspace_id,
            camera_ids=camera_ids,
            user_system_role=user_system_role,
            user_workspace_role=user_workspace_role,
            requesting_username=requesting_username
        )
    
    async def get_prediction_data(
        self,
        workspace_id: UUID,
        camera_ids: List[Tuple[str, str]],
        search_query: SearchQuery,
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str
    ) -> List[Dict[str, Any]]:
        """Get prediction data for cameras"""
        return await self.backend.get_prediction_data(
            workspace_id=workspace_id,
            camera_ids=camera_ids,
            search_query=search_query,
            user_system_role=user_system_role,
            user_workspace_role=user_workspace_role,
            requesting_username=requesting_username
        )
    
    # ========== Location Operations ==========
    
    async def get_location_data(
        self,
        workspace_id: UUID,
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str,
        location_field: Optional[str] = None,
        filter_conditions: Optional[Dict[str, Union[str, List[str]]]] = None
    ) -> List[Dict[str, Any]]:
        """Get location data"""
        return await self.backend.get_location_data(
            workspace_id=workspace_id,
            user_system_role=user_system_role,
            user_workspace_role=user_workspace_role,
            requesting_username=requesting_username,
            location_field=location_field,
            filter_conditions=filter_conditions
        )
    
    async def get_location_analytics(
        self,
        workspace_id: UUID,
        search_query: SearchQuery,
        group_by: str,
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str
    ) -> Dict[str, Any]:
        """Get location analytics"""
        return await self.backend.get_location_analytics(
            workspace_id=workspace_id,
            search_query=search_query,
            group_by=group_by,
            user_system_role=user_system_role,
            user_workspace_role=user_workspace_role,
            requesting_username=requesting_username
        )
    
    async def get_location_summary(
        self,
        workspace_id: UUID,
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str
    ) -> Dict[str, Any]:
        """Get location summary"""
        return await self.backend.service.get_location_summary(
            workspace_id=workspace_id,
            user_system_role=user_system_role,
            user_workspace_role=user_workspace_role,
            requesting_username=requesting_username
        )
    
    async def search_location_data(
        self,
        workspace_id: UUID,
        search_term: str,
        search_fields: List[str],
        exact_match: bool,
        limit: int,
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str
    ) -> Dict[str, Any]:
        """Search location data"""
        return await self.backend.service.search_location_data(
            workspace_id=workspace_id,
            search_term=search_term,
            search_fields=search_fields,
            exact_match=exact_match,
            limit=limit,
            user_system_role=user_system_role,
            user_workspace_role=user_workspace_role,
            requesting_username=requesting_username
        )
    
    # ========== Data Management ==========
    
    async def delete_data(
        self,
        workspace_id: UUID,
        delete_request: DeleteDataRequest,
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str
    ) -> Dict[str, Any]:
        """Delete data from workspace"""
        return await self.backend.delete_data(
            workspace_id=workspace_id,
            delete_request=delete_request,
            user_system_role=user_system_role,
            user_workspace_role=user_workspace_role,
            requesting_username=requesting_username
        )
    
    async def delete_all_workspace_data(
        self,
        workspace_id: UUID,
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str
    ) -> Dict[str, Any]:
        """Delete all data from workspace"""
        return await self.backend.service.delete_all_workspace_data(
            workspace_id=workspace_id,
            user_system_role=user_system_role,
            user_workspace_role=user_workspace_role,
            requesting_username=requesting_username
        )
    
    async def delete_user_camera_data(
        self,
        workspace_camera_mapping: Dict[str, List[str]]
    ) -> Dict:
        """Delete user's camera data"""
        return await self.backend.delete_user_camera_data(
            workspace_camera_mapping=workspace_camera_mapping
        )
    
    async def delete_camera_data_from_workspaces(
        self,
        workspace_camera_mapping: Dict[str, List[str]]
    ) -> List[str]:
        """Delete camera data from workspaces"""
        return await self.backend.service.delete_camera_data_from_workspaces(
            workspace_camera_mapping=workspace_camera_mapping
        )
    
    # ========== Export Operations ==========
    
    async def export_workspace_data(
        self,
        workspace_id: UUID,
        search_query: Optional[SearchQuery],
        format: str,
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str,
        include_frame: bool = False
    ) -> Tuple[bytes, str, str]:
        """Export workspace data"""
        return await self.backend.export_workspace_data(
            workspace_id=workspace_id,
            search_query=search_query,
            format=format,
            user_system_role=user_system_role,
            user_workspace_role=user_workspace_role,
            requesting_username=requesting_username,
            include_frame=include_frame
        )
    
    # ========== Metadata Operations ==========
    
    async def update_camera_metadata(
        self,
        stream_update: StreamUpdate,
        stream_id: str,
        workspace_id: str
    ) -> None:
        """Update camera metadata"""
        return await self.backend.update_camera_metadata(
            stream_update=stream_update,
            stream_id=stream_id,
            workspace_id=workspace_id
        )
    
    async def bulk_update_metadata(
        self,
        workspace_id: UUID,
        camera_ids: Optional[List[str]],
        metadata_updates: Dict[str, Any],
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str
    ) -> Dict[str, Any]:
        """Bulk update metadata"""
        return await self.backend.bulk_update_metadata(
            workspace_id=workspace_id,
            camera_ids=camera_ids,
            metadata_updates=metadata_updates,
            user_system_role=user_system_role,
            user_workspace_role=user_workspace_role,
            requesting_username=requesting_username
        )
    
    async def preview_metadata_update(
        self,
        workspace_id: UUID,
        camera_ids: Optional[List[str]],
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str,
        limit: int = 10
    ) -> Dict[str, Any]:
        """Preview metadata update"""
        return await self.backend.preview_metadata_update(
            workspace_id=workspace_id,
            camera_ids=camera_ids,
            user_system_role=user_system_role,
            user_workspace_role=user_workspace_role,
            requesting_username=requesting_username,
            limit=limit
        )
    
    async def update_camera_metadata_by_id(
        self,
        workspace_id: UUID,
        camera_id: str,
        stream_update: StreamUpdate,
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str
    ) -> Dict[str, Any]:
        """Update camera metadata by ID"""
        return await self.backend.service.update_camera_metadata_by_id(
            workspace_id=workspace_id,
            camera_id=camera_id,
            stream_update=stream_update,
            user_system_role=user_system_role,
            user_workspace_role=user_workspace_role,
            requesting_username=requesting_username
        )
    
    # ========== Fire Detection State Management ==========
    
    async def get_persistent_fire_state(self, stream_id: UUID) -> Dict[str, Any]:
        """Get persistent fire state"""
        return await self.backend.service.get_persistent_fire_state(stream_id)
    
    async def update_persistent_fire_state(
        self,
        stream_id: UUID,
        fire_status: str,
        detection_time: datetime = None,
        notification_time: datetime = None
    ):
        """Update persistent fire state"""
        return await self.backend.service.update_persistent_fire_state(
            stream_id=stream_id,
            fire_status=fire_status,
            detection_time=detection_time,
            notification_time=notification_time
        )
    
    async def load_fire_state_on_stream_start(self, stream_id: UUID) -> Dict[str, Any]:
        """Load fire state on stream start"""
        return await self.backend.service.load_fire_state_on_stream_start(stream_id)
    
    async def cleanup_old_fire_states(self):
        """Cleanup old fire states"""
        return await self.backend.service.cleanup_old_fire_states()
    
    # ========== Camera Operations ==========
    
    async def get_camera_ids_for_workspace(self, workspace_id: UUID) -> List[Dict[str, str]]:
        """Get camera IDs for workspace"""
        return await self.backend.service.get_camera_ids_for_workspace(workspace_id)
    
    async def get_stream_camera_info(self, stream_id: UUID) -> Optional[Dict[str, Any]]:
        """Get stream camera info"""
        return await self.backend.service.get_stream_camera_info(stream_id)
    
    async def search_cameras_by_location(
        self,
        workspace_id: UUID,
        location_filters: Dict[str, Optional[str]],
        status: Optional[str],
        search_term: Optional[str],
        group_by: str,
        include_inactive: bool,
        user_id: UUID,
        user_role: str
    ) -> Dict[str, Any]:
        """Search cameras by location"""
        return await self.backend.service.search_cameras_by_location(
            workspace_id=workspace_id,
            location_filters=location_filters,
            status=status,
            search_term=search_term,
            group_by=group_by,
            include_inactive=include_inactive,
            user_id=user_id,
            user_role=user_role
        )
    
    # ========== User and Workspace Management ==========
    
    async def get_user_workspace_info(
        self, username: str
    ) -> Tuple[Optional[Dict], Optional[UUID]]:
        """Get user workspace info"""
        return await self.backend.service.get_user_workspace_info(username)
    
    async def check_workspace_membership(
        self, user_id: UUID, workspace_id: UUID, required_role: Optional[str] = None
    ) -> Dict[str, str]:
        """Check workspace membership"""
        return await self.backend.service.check_workspace_membership(
            user_id=user_id,
            workspace_id=workspace_id,
            required_role=required_role
        )
    
    # ========== Backend-Specific Operations ==========
    
    async def list_collections_or_indices(self) -> Dict[str, List]:
        """List all collections (Qdrant) or indices (Elasticsearch)"""
        if self.backend_type == "elasticsearch":
            return await self.backend.service.list_indices()
        else:
            return await self.backend.service.list_collections()
    
    async def get_collection_or_index_count(
        self,
        name: str,
        search_query: Optional[SearchQuery],
        user_system_role: str,
        user_workspace_role: Optional[str],
        requesting_username: str
    ) -> Dict[str, Any]:
        """Get count in collection/index"""
        if self.backend_type == "elasticsearch":
            return await self.backend.service.get_index_count(
                index_name=name,
                search_query=search_query,
                user_system_role=user_system_role,
                user_workspace_role=user_workspace_role,
                requesting_username=requesting_username
            )
        else:
            return await self.backend.service.get_collection_count(
                collection_name=name,
                search_query=search_query,
                user_system_role=user_system_role,
                user_workspace_role=user_workspace_role,
                requesting_username=requesting_username
            )
    
    # ========== Elasticsearch-Specific Methods ==========
    
    async def get_unique_locations(self, **kwargs) -> Dict[str, Any]:
        """Get unique locations (Elasticsearch only)"""
        return await self.backend.service.get_unique_locations(**kwargs)
    
    async def get_unique_areas(self, **kwargs) -> Dict[str, Any]:
        """Get unique areas (Elasticsearch only)"""
        return await self.backend.service.get_unique_areas(**kwargs)
    
    async def get_unique_buildings(self, **kwargs) -> Dict[str, Any]:
        """Get unique buildings (Elasticsearch only)"""
        return await self.backend.service.get_unique_buildings(**kwargs)
    
    async def get_unique_floor_levels(self, **kwargs) -> Dict[str, Any]:
        """Get unique floor levels (Elasticsearch only)"""
        return await self.backend.service.get_unique_floor_levels(**kwargs)
    
    async def get_unique_zones(self, **kwargs) -> Dict[str, Any]:
        """Get unique zones (Elasticsearch only)"""
        return await self.backend.service.get_unique_zones(**kwargs)
    
    async def get_workspace_cameras(self, **kwargs) -> Dict[str, Any]:
        """Get workspace cameras with filtering"""
        return await self.backend.service.get_workspace_cameras(**kwargs)
    
    # ========== Collection/Index Management ==========
    
    async def create_collection_or_index(
        self,
        name: str,
        **kwargs
    ) -> Dict[str, str]:
        """Create collection (Qdrant) or index (Elasticsearch)"""
        if self.backend_type == "elasticsearch":
            mappings = kwargs.get('mappings')
            return await self.backend.service.create_index(
                index_name=name,
                mappings=mappings
            )
        else:
            vector_size = kwargs.get('vector_size', config.get("qdrant_vector_size", 1))
            distance = kwargs.get('distance', 'DOT')
            return await self.backend.service.create_collection(
                collection_name=name,
                vector_size=vector_size,
                distance=distance
            )
    
    async def delete_collection_or_index(self, name: str) -> Dict[str, str]:
        """Delete collection (Qdrant) or index (Elasticsearch)"""
        if self.backend_type == "elasticsearch":
            return await self.backend.service.delete_index(index_name=name)
        else:
            return await self.backend.service.delete_collection(collection_name=name)
    
    async def get_total_count(self, name: str) -> int:
        """Get total count without filters"""
        if self.backend_type == "elasticsearch":
            return await self.backend.service.get_index_total_count(index_name=name)
        else:
            return await self.backend.service.get_collection_total_count(collection_name=name)
    
    # ========== Table/Schema Management ==========
    
    async def ensure_fire_detection_state_table(self):
        """Ensure fire detection state table exists"""
        return await self.backend.service.ensure_fire_detection_state_table()
    
    # ========== Lifecycle Methods ==========
    
    async def initialize(self):
        """Initialize the service"""
        if self.backend_type == "elasticsearch":
            await self.backend.service.initialize()
        logger.info(f"Unified Data Service initialized with {self.backend_type} backend")
    
    async def close(self):
        """Close the service"""
        if self.backend_type == "elasticsearch":
            await self.backend.service.close()
        logger.info(f"Unified Data Service closed")


# Create singleton instance
unified_data_service = UnifiedDataService()