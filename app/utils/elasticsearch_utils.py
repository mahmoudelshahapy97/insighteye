# app/utils/elasticsearch_utils.py
"""Elasticsearch utilities for workspace-specific indices."""

import logging
from typing import Union, Dict
from uuid import UUID
import re

from elasticsearch import AsyncElasticsearch
from fastapi import HTTPException

from app.config.settings import config

logger = logging.getLogger(__name__)

BASE_ELASTICSEARCH_INDEX_NAME = config.get("elasticsearch_index_name", "personcounts")
_workspace_index_init_cache: Dict[str, bool] = {}


def validate_index_name(index_name: str) -> bool:
    """
    Validate Elasticsearch index name according to ES naming rules:
    - Must be lowercase
    - Cannot include \\, /, *, ?, ", <, >, |, ` ` (space), ,, #
    - Cannot start with -, _, +
    - Cannot be . or ..
    - Cannot be longer than 255 bytes
    """
    if not index_name or len(index_name) > 255:
        return False
    
    if index_name in ('.', '..'):
        return False
    
    if index_name[0] in ('-', '_', '+'):
        return False
    
    # Check for invalid characters
    invalid_chars = ['\\', '/', '*', '?', '"', '<', '>', '|', ' ', ',', '#', ':']
    if any(char in index_name for char in invalid_chars):
        return False
    
    # Must be lowercase
    if index_name != index_name.lower():
        return False
    
    return True

def get_workspace_elasticsearch_index_name(workspace_id: Union[str, UUID]) -> str:
    """
    Generates the Elasticsearch index name for a given workspace.
    All data related to a workspace will be stored in this single index.
    
    Example: person_counts-ws-3bec3843-efc6-4e24-b37c-8fd419a374ae
    """
    if isinstance(workspace_id, UUID):
        # Use hex representation (no hyphens, lowercase)
        workspace_id_str = workspace_id.hex
    else:
        # Remove hyphens and ensure lowercase
        workspace_id_str = str(workspace_id).replace("-", "").lower().strip()

    base_name = BASE_ELASTICSEARCH_INDEX_NAME.lower().strip()

    # # Remove any invalid characters from base name
    # base_name = re.sub(r'[^a-z0-9\-_]', '', base_name)

    index_name = f"{base_name}ws{workspace_id_str}".lower()

    # # Validate the index name
    # if not validate_index_name(index_name):
    #     raise ValueError(
    #         f"Invalid Elasticsearch index name generated: '{index_name}'. "
    #         f"Index names must be lowercase, cannot contain special characters, "
    #         f"and cannot start with -, _, or +"
    #     )
    
    return index_name

async def ensure_workspace_elasticsearch_index_exists(
    client: AsyncElasticsearch, 
    workspace_id: Union[str, UUID]
):
    """
    Ensures that an Elasticsearch index for the given workspace_id exists.
    Creates it if it doesn't, with appropriate mappings and settings.
    """
    index_name = get_workspace_elasticsearch_index_name(workspace_id)
    
    # Check cache first
    if index_name in _workspace_index_init_cache:
        return

    try:
        # Check if index exists with better error handling
        try:
            exists = await client.indices.exists(index=index_name)
        except Exception as check_error:
            logger.error(f"Error checking if index '{index_name}' exists: {check_error}")
            # If we can't check, try to get more info
            try:
                # Try to get cluster health to verify ES connection
                health = await client.cluster.health()
                logger.info(f"Elasticsearch cluster health: {health['status']}")
            except Exception as health_error:
                logger.error(f"Cannot connect to Elasticsearch cluster: {health_error}")
                raise HTTPException(
                    status_code=503,
                    detail="Elasticsearch cluster is not available"
                )
            # Re-raise the original error
            raise
        
        if exists:
            logger.debug(f"Elasticsearch index '{index_name}' already exists for workspace {str(workspace_id)}.")
            _workspace_index_init_cache[index_name] = True
            return
        
        logger.info(f"Elasticsearch index '{index_name}' not found for workspace {str(workspace_id)}. Creating it now.")
        
        # Define mappings for detection data with location support
        mappings = {
            "properties": {
                "camera_id": {
                    "type": "keyword"
                },
                "name": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword"}
                    }
                },
                "timestamp": {
                    "type": "double"
                },
                "datetime": {
                    "type": "date"
                },
                "date": {
                    "type": "keyword"
                },
                "time": {
                    "type": "keyword"
                },
                "person_count": {
                    "type": "integer"
                },
                "male_count": {
                    "type": "integer"
                },
                "female_count": {
                    "type": "integer"
                },
                "fire_status": {
                    "type": "keyword"
                },
                "frame_base64": {
                    "type": "text",
                    "index": False
                },
                "username": {
                    "type": "keyword"
                },
                # Location fields
                "location": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword"}
                    }
                },
                "area": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword"}
                    }
                },
                "building": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword"}
                    }
                },
                "zone": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword"}
                    }
                },
                "floor_level": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword"}
                    }
                },
                "latitude": {
                    "type": "float"
                },
                "longitude": {
                    "type": "float"
                },
                "location_point": {
                    "type": "geo_point"
                }
            }
        }
        
        # Define index settings
        settings = {
            "number_of_shards": config.get("elasticsearch_shards", 1),
            "number_of_replicas": config.get("elasticsearch_replicas", 1),
            "refresh_interval": config.get("elasticsearch_refresh_interval", "1s")
        }
        
        # Create index with mappings and settings
        create_response = await client.indices.create(
            index=index_name,
            mappings=mappings,
            settings=settings
        )
        
        logger.info(f"Successfully created Elasticsearch index: '{index_name}' for workspace {str(workspace_id)}")
        logger.debug(f"Create index response: {create_response}")
        _workspace_index_init_cache[index_name] = True
        
    except Exception as e:
        logger.error(
            f"Error ensuring Elasticsearch index '{index_name}' exists for workspace {str(workspace_id)}: {e}", 
            exc_info=True
        )
        
        # Provide more specific error messages
        error_msg = str(e)
        if "400" in error_msg or "BadRequestError" in error_msg:
            detail = (
                f"Invalid Elasticsearch index configuration for workspace. "
                f"Index name: '{index_name}'. "
                f"This may be due to invalid index name format or mapping configuration. "
                f"Original error: {error_msg}"
            )
        elif "503" in error_msg or "ConnectionError" in error_msg:
            detail = "Elasticsearch cluster is not available"
        else:
            detail = f"Failed to initialize Elasticsearch workspace index: {error_msg}"
        
        raise HTTPException(
            status_code=500, 
            detail=detail
        )

async def delete_workspace_elasticsearch_index(
    client: AsyncElasticsearch,
    workspace_id: Union[str, UUID]
) -> bool:
    """
    Delete the Elasticsearch index for a workspace.
    Returns True if deleted successfully, False if index didn't exist.
    """
    index_name = get_workspace_elasticsearch_index_name(workspace_id)
    
    try:
        exists = await client.indices.exists(index=index_name, allow_no_indices=True)
        if not exists:
            logger.info(f"Index '{index_name}' does not exist, nothing to delete")
            return False
        
        await client.indices.delete(index=index_name)
        logger.info(f"Successfully deleted index '{index_name}' for workspace {str(workspace_id)}")
        
        # Remove from cache
        _workspace_index_init_cache.pop(index_name, None)
        
        return True
    except Exception as e:
        logger.error(f"Error deleting index '{index_name}': {e}", exc_info=True)
        raise

def _format_bytes(self, bytes_value: int) -> str:
    """Convert bytes to human readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.2f}{unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.2f}PB"
