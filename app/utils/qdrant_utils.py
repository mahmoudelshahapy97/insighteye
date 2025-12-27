# app/utils/qdrant_utils.py
"""Qdrant database utilities for workspace-specific collections."""

import logging
import asyncio
from typing import Union, Dict
from uuid import UUID

from qdrant_client import QdrantClient, models as qdrant_models
from fastapi import HTTPException

from app.config.settings import config

logger = logging.getLogger(__name__)

BASE_QDRANT_COLLECTION_NAME = config.get("qdrant_collection_name", "person_counts")
_workspace_collection_init_cache: Dict[str, bool] = {}


def get_workspace_qdrant_collection_name(workspace_id: Union[str, UUID]) -> str:
    """
    Generates the Qdrant collection name for a given workspace.
    All data related to a workspace will be stored in this single collection.
    Example: person_counts_ws_xxxxxxxx_xxxx_xxxx_xxxx_xxxxxxxxxxxx
    """
    return f"{BASE_QDRANT_COLLECTION_NAME}_ws_{str(workspace_id).replace('-', '_')}"


async def ensure_workspace_qdrant_collection_exists(
    client: QdrantClient, 
    workspace_id: Union[str, UUID]
):
    """
    Ensures that a Qdrant collection for the given workspace_id exists.
    Creates it if it doesn't, with appropriate payload indexes.
    Uses executor for synchronous Qdrant client calls.
    """
    collection_name = get_workspace_qdrant_collection_name(workspace_id)
    
    if collection_name in _workspace_collection_init_cache:
        return

    loop = asyncio.get_event_loop()
    try:
        try:
            await loop.run_in_executor(None, client.get_collection, collection_name)
            logger.debug(f"Qdrant collection '{collection_name}' already exists for workspace {str(workspace_id)}.")
            _workspace_collection_init_cache[collection_name] = True
            return
        except Exception as e:
            # Broader check for "not found" conditions from Qdrant client
            if "not found" in str(e).lower() or \
               (hasattr(e, 'status_code') and e.status_code == 404) or \
               "NOT_FOUND" in str(e).upper() or \
               (hasattr(e, 'message') and "doesn't exist" in str(getattr(e, 'message', '')).lower()):
                logger.info(f"Qdrant collection '{collection_name}' not found for workspace {str(workspace_id)}. Will create.")
            else:
                logger.error(f"Error checking Qdrant collection '{collection_name}' for ws {str(workspace_id)}: {e}", exc_info=True)
                raise

        # Vector size 1 is unusual but matches utils.py
        vector_size_for_collection = 1
        
        await loop.run_in_executor(
            None,
            client.create_collection,
            collection_name,
            qdrant_models.VectorParams(size=vector_size_for_collection, distance=qdrant_models.Distance.DOT)
        )
        logger.info(f"Created Qdrant collection: '{collection_name}' for workspace {str(workspace_id)}")

        payload_indexes_map = {
            "timestamp": qdrant_models.PayloadSchemaType.FLOAT,
            "camera_id": qdrant_models.PayloadSchemaType.KEYWORD,
            "username": qdrant_models.PayloadSchemaType.KEYWORD,
            "date": qdrant_models.PayloadSchemaType.KEYWORD,
        }
        for field, schema_type in payload_indexes_map.items():
            try:
                await loop.run_in_executor(
                    None,
                    client.create_payload_index,
                    collection_name,
                    field, 
                    schema_type 
                )
                logger.info(f"Created payload index for field '{field}' in collection '{collection_name}'.")
            except Exception as e_idx:
                if "already exists" in str(e_idx).lower() or "already present" in str(e_idx).lower():
                     logger.info(f"Payload index for field '{field}' already exists in '{collection_name}'.")
                else:
                    logger.warning(f"Could not create payload index for '{field}' in '{collection_name}': {e_idx}")
        
        _workspace_collection_init_cache[collection_name] = True
    except Exception as e:
        logger.error(f"Error ensuring Qdrant collection '{collection_name}' exists for workspace {str(workspace_id)}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to initialize Qdrant workspace collection: {collection_name}")
