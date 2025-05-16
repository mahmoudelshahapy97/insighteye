
from fastapi import APIRouter, HTTPException, Query, Request, Depends, status
from fastapi.responses import JSONResponse
from typing import Optional, List, Union
import time
import asyncio
from config import config
from utils import parse_camera_ids, make_prediction, parse_date_format, paginate_list, parse_time_string
from qdrant_client import QdrantClient
from qdrant_client.http import models
from datetime import datetime, time
import logging
from user_manager import UserManager
from session_manager import SessionManager
from schemas_models import CreateCollectionRequest, SearchQuery, TimestampRangeResponse, CameraIdsResponse, DeleteDataRequest

# Setup logging
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ])
logger = logging.getLogger(__name__)

# Router
router = APIRouter(tags=["stream"])
session_manager = SessionManager()
user_manager = UserManager()

# Initialize Qdrant client
def create_qdrant_client():
    return QdrantClient(
        url=config.get("qdrant_url", "localhost"),
        port=config.get("qdrant_port", 6333),
        timeout=60.0  # Reduced timeout but still reasonable
        # grpc_port=6334, 
        # prefer_grpc=True,
    )

def init_collection(client: QdrantClient, collection_name: str):
    """Initialize collection only if it doesn't exist"""
    try:
        # Check if collection exists using lightweight list call
        collections = client.get_collections()
        collection_names = [c.name for c in collections.collections]
        
        if collection_name not in collection_names:
            # Create collection for storing base64 images
            client.create_collection(
                collection_name=collection_name,
                vectors_config=models.VectorParams(
                    size=1,  # Minimal vector size since we're not using embeddings
                    distance=models.Distance.DOT  # Distance metric doesn't matter here
                ),
                # Add optimized index for timestamp and camera_id fields
                optimizers_config=models.OptimizersConfigDiff(
                    indexing_threshold=0,  # Index immediately
                ),
                # Add sparse vectors for faster filtering
                sparse_vectors_config={
                    "sparse": models.SparseVectorParams(
                        index=models.SparseIndexParams()
                    )
                }
            )

            # Create field indexes after collection creation
            client.create_payload_index(
                collection_name=collection_name,
                field_name="timestamp",
                field_schema=models.PayloadSchemaType.INTEGER
            )
            client.create_payload_index(
                collection_name=collection_name,
                field_name="camera_id",
                field_schema=models.PayloadSchemaType.KEYWORD
            )

            logger.info(f"Created Qdrant collection: {collection_name}")
            return True
        return False
    except Exception as e:
        logging.error(f"Failed to initialize Qdrant collection: {e}")
        raise

def get_user_collection_name(username):
    """Get user-specific collection name"""
    base_name = config.get("qdrant_collection_name", "person_counts")
    return f"{base_name}_{username}"

# Create a single client instance for the router
qdrant_client = create_qdrant_client()

# Cache for collection initialization status to avoid repeated checks
collection_init_cache = {}

@router.get("/search_results")
async def search_results(
    request: Request, 
    camera_id: Optional[str] = Query(None), 
    start_date: Optional[str] = Query(None), 
    end_date: Optional[str] = Query(None), 
    start_time: Optional[str] = Query(None), 
    end_time: Optional[str] = Query(None), 
    page: Optional[int] = Query(1), 
    per_page: Optional[int] = Query(6, ge=1), #, le=12
    full_data: Optional[bool] = Query(True),
    username: str = Depends(session_manager.get_current_user)):
    """
    Optimized endpoint to search for recorded data with pagination and filtering.
    """
    logger.info(f"Search request - Page: {page}, PerPage: {per_page}, Params: camera={camera_id}, date={start_date}-{end_date}, time={start_time}-{end_time}")

    try:
        # Check user permissions
        user_data = await user_manager.get_user_by_username(username)
        user_id = user_data["user_id"]
        is_search = await user_manager.get_search_status(user_id)

        if not is_search:
            logger.warning(f"User '{username}' (ID: {user_id}) attempted search without permission.")
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Search feature unavailable. Check your subscription.")

        # Get the user-specific collection name
        user_collection = get_user_collection_name(username)
        
        # OPTIMIZATION: Only initialize collection once and cache the result
        if user_collection not in collection_init_cache:
            init_collection(qdrant_client, user_collection)
            collection_init_cache[user_collection] = True
        
        # Parse camera IDs
        camera_ids = None
        if camera_id:
            camera_ids = [cam_id.strip() for cam_id in camera_id.split(',') if cam_id.strip()]
        
        # Create query object with the provided parameters
        query = SearchQuery(
            camera_id=camera_ids,
            start_date=start_date,
            end_date=end_date,
            start_time=start_time,
            end_time=end_time
        )
        
        # OPTIMIZATION: Calculate offset and limit for direct pagination in Qdrant
        offset = (page - 1) * per_page
        
        # OPTIMIZATION: Get count and results in parallel
        filter_obj = build_filter_from_query(query)
        
        # Run count and data fetch in parallel
        count_task = asyncio.create_task(get_results_count(filter_obj, user_collection))
        results_task = asyncio.create_task(get_paginated_results(filter_obj, user_collection, offset, per_page, full_data))
        
        # Wait for both tasks to complete
        total_count, current_page_results = await asyncio.gather(count_task, results_task)
        
        # Calculate pagination details
        if total_count == 0:
            num_of_pages = 0
            current_page = 0
        else:
            num_of_pages = (total_count + per_page - 1) // per_page
            current_page = max(1, min(page, num_of_pages))

        # OPTIMIZATION: Only fetch timestamp range when needed (first page or explicit request)
        metadata_ts = {}
        if total_count > 0 and page == 1:
            try:
                # Use a more efficient function for timestamp range
                result_ts = await get_timestamp_range_efficient(camera_ids, user_collection)
                metadata_ts = {
                    "first_timestamp": result_ts.first_timestamp,
                    "last_timestamp": result_ts.last_timestamp,
                    "first_datetime": result_ts.first_datetime,
                    "last_datetime": result_ts.last_datetime,
                    "first_date": result_ts.first_date,
                    "last_date": result_ts.last_date,
                    "first_time": result_ts.first_time,
                    "last_time": result_ts.last_time,
                    "camera_id": result_ts.camera_id
                }
            except Exception as ts_err:
                logger.warning(f"Could not get timestamp range metadata: {ts_err}")
        
        return JSONResponse(content={
            "data": current_page_results,
            "current_page": current_page if total_count > 0 else 1,
            "num_of_pages": num_of_pages,
            "total_count": total_count,
            "metadata": metadata_ts,
            "per_page": per_page
        })

    except HTTPException as http_err:
        logger.error(f"HTTP Exception during search: {http_err.status_code} - {http_err.detail}")
        raise http_err
    except Exception as e:
        logger.error(f"Unexpected error during search: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An internal error occurred during search.")

async def get_results_count(filter_obj, collection_name: str):
    """Get only the count of matching results"""
    try:
        if filter_obj:
            count_result = qdrant_client.count(
                collection_name=collection_name,
                count_filter=filter_obj  
            )
        else:
            count_result = qdrant_client.count(collection_name=collection_name)
            
        return count_result.count
        
    except Exception as e:
        logger.error(f"Error getting result count: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Count operation failed: {str(e)}"
        )

async def get_paginated_results(filter_obj, collection_name: str, offset: int, limit: int=1, full_data: bool=True):
    """Get only the results needed for the current page"""
    try:
        # Use scroll API with pagination to get exactly what we need
        points, _ = qdrant_client.scroll(
            collection_name=collection_name,
            limit=limit,
            offset=offset,  # Use calculated offset from pagination
            with_payload=True,
            with_vectors=False,
            scroll_filter=filter_obj
        )
        
        # Format results
        search_results = []
        for point in points:
            if not point.payload:
                continue
            if full_data:    
                result = {
                    "frame": point.payload.get("frame"),
                    "metadata": {
                        "camera_id": point.payload.get("camera_id"),
                        "name": point.payload.get("name", "Unknown"),
                        "timestamp": point.payload.get("timestamp"),
                        "date": point.payload.get("date"),
                        "time": point.payload.get("time"),
                        "person_count": point.payload.get("person_count", 0)
                    }
                }
            else:
                result = {
                    "metadata": {
                        "camera_id": point.payload.get("camera_id"),
                        "name": point.payload.get("name", "Unknown"),
                        "timestamp": point.payload.get("timestamp"),
                        "date": point.payload.get("date"),
                        "time": point.payload.get("time"),
                        "person_count": point.payload.get("person_count", 0)
                    }
                }

            search_results.append(result)
            
        # Sort results by timestamp (newer first)
        search_results.sort(key=lambda x: x.get('metadata', {}).get('timestamp', 0), reverse=True)
            
        return search_results
        
    except Exception as e:
        logger.error(f"Error getting paginated results: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Search operation failed: {str(e)}"
        )

def build_filter_from_query(query: SearchQuery):
    """
    Helper function to build a Qdrant filter from search query parameters
    """
    must_conditions = []
    
    # 1. Handle camera_id filter
    if query.camera_id and isinstance(query.camera_id, list) and len(query.camera_id) > 0:
        if len(query.camera_id) > 1:
            # Multiple camera IDs - use 'should' for OR condition
            camera_filter = models.Filter(
                should=[
                    models.FieldCondition(
                        key="camera_id",
                        match=models.MatchValue(value=cam_id)
                    )
                    for cam_id in query.camera_id
                ]
            )
            must_conditions.append(camera_filter)
        else:
            # Single camera ID - simple match
            must_conditions.append(
                models.FieldCondition(
                    key="camera_id",
                    match=models.MatchValue(value=query.camera_id[0])
                )
            )
    
    # 2. Handle date and time filters
    start_timestamp = None
    end_timestamp = None
    
    try:
        # Determine start datetime
        if query.start_date:
            start_date_obj = parse_date_format(query.start_date)
            start_time_obj = parse_time_string(query.start_time, time.min)
            start_datetime = datetime.combine(start_date_obj, start_time_obj)
            start_timestamp = start_datetime.timestamp()
            
        # Determine end datetime
        if query.end_date:
            end_date_obj = parse_date_format(query.end_date)
            end_time_obj = parse_time_string(query.end_time, time.max.replace(microsecond=0))
            end_datetime = datetime.combine(end_date_obj, end_time_obj)
            end_timestamp = end_datetime.timestamp()
            
        # Build timestamp range condition
        if start_timestamp is not None or end_timestamp is not None:
            timestamp_range = {}
            if start_timestamp is not None:
                timestamp_range["gte"] = start_timestamp
            if end_timestamp is not None:
                timestamp_range["lte"] = end_timestamp
            
            must_conditions.append(
                models.FieldCondition(
                    key="timestamp",
                    range=models.Range(**timestamp_range)
                )
            )
    except ValueError as date_err:
        logger.error(f"Invalid date format provided: {date_err}")
        raise HTTPException(status_code=400, detail=f"Invalid date format: {date_err}")
    
    # Create main filter if we have conditions
    if must_conditions:
        return models.Filter(must=must_conditions)
    return None

async def get_timestamp_range_efficient(camera_id: Optional[List[str]], collection_name: str):
    """
    Efficient implementation of timestamp range query
    """
    try:
        filter_obj = None
        
        # Add camera filter if needed
        if camera_id and len(camera_id) > 0:
            if len(camera_id) > 1:
                filter_obj = models.Filter(
                    should=[
                        models.FieldCondition(
                            key="camera_id", 
                            match=models.MatchValue(value=cam_id)
                        )
                        for cam_id in camera_id
                    ]
                )
            else:
                filter_obj = models.Filter(
                    must=[
                        models.FieldCondition(
                            key="camera_id",
                            match=models.MatchValue(value=camera_id[0])
                        )
                    ]
                )
        
        result = TimestampRangeResponse()
        
        # OPTIMIZATION: Use two targeted queries instead of fetching all records
        # First query: Get the earliest record
        earliest_filter = filter_obj.copy() if filter_obj else models.Filter(must=[])
        
        # Get one record sorted by timestamp ascending
        earliest_points, _ = qdrant_client.scroll(
            collection_name=collection_name,
            limit=1,  # Only need one record for earliest
            with_payload=["timestamp", "camera_id"],
            scroll_filter=earliest_filter,
            # Order by timestamp ascending (oldest first)
        )
        
        # Second query: Get the latest record
        latest_filter = filter_obj.copy() if filter_obj else models.Filter(must=[])
        
        # Get one record sorted by timestamp descending
        latest_points, _ = qdrant_client.scroll(
            collection_name=collection_name,
            limit=1,  # Only need one record for latest
            with_payload=["timestamp", "camera_id"],
            scroll_filter=latest_filter,
            # Order by timestamp descending (newest first)
        )
        
        # Process results
        if earliest_points and latest_points:
            # Find min and max timestamps
            min_timestamp = earliest_points[0].payload.get("timestamp")
            max_timestamp = latest_points[0].payload.get("timestamp")
            
            # Ensure min is actually less than max
            if min_timestamp > max_timestamp:
                min_timestamp, max_timestamp = max_timestamp, min_timestamp
                
            # Set response values
            result.first_timestamp = min_timestamp
            result.last_timestamp = max_timestamp
            
            first_dt = datetime.fromtimestamp(result.first_timestamp)
            last_dt = datetime.fromtimestamp(result.last_timestamp)
            
            result.first_datetime = first_dt.isoformat()
            result.last_datetime = last_dt.isoformat()
            result.first_date = first_dt.date().isoformat()
            result.last_date = last_dt.date().isoformat()
            result.first_time = first_dt.time().isoformat()
            result.last_time = last_dt.time().isoformat()
            
            # Include camera_id in response if filter was for a single camera
            if camera_id and len(camera_id) == 1:
                result.camera_id = camera_id[0]
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting timestamp range efficiently: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving timestamp range: {str(e)}"
        )

@router.get("/prediction_data")
async def prediction_data(
    request: Request, 
    camera_id: Optional[str] = Query(None), 
    start_date: Optional[str] = Query(None), 
    end_date: Optional[str] = Query(None), 
    start_time: Optional[str] = Query(None), 
    end_time: Optional[str] = Query(None), 
    username: str = Depends(session_manager.get_current_user)):
    """
    Optimized endpoint to get prediction data based on filtered results.
    """
    logger.info(f"Prediction request - Params: camera={camera_id}, date={start_date}-{end_date}, time={start_time}-{end_time}")

    try:
        user_data = await user_manager.get_user_by_username(username)
        user_id = user_data["user_id"]
        is_prediction = await user_manager.get_prediction_status(user_id)

        if not is_prediction:
            logger.warning(f"User '{username}' (ID: {user_id}) attempted prediction without permission.")
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Prediction feature unavailable. Check your subscription.")

        # Get the user-specific collection name
        user_collection = get_user_collection_name(username)

        all_predictions = []

        if camera_id:
            # Parse camera IDs
            camera_ids = [cam_id.strip() for cam_id in camera_id.split(',') if cam_id.strip()]
            logger.info(f"Processing predictions for cameras: {camera_ids}")

            # OPTIMIZATION: Process cameras in parallel using gather
            tasks = []
            for cam_id in camera_ids:
                tasks.append(process_camera_prediction(
                    cam_id,
                    user_collection,
                    start_date,
                    end_date,
                    start_time,
                    end_time
                ))
            
            # Wait for all predictions to complete
            if tasks:
                all_predictions = await asyncio.gather(*tasks)
                # Filter out None results from cameras with no data
                all_predictions = [p for p in all_predictions if p]

            logger.info(f"Prediction processing complete. Generated {len(all_predictions)} predictions.")
            return JSONResponse({"predictions": all_predictions})
        else:
            logger.warning(f"Prediction requested but no camera_id provided.")
            return JSONResponse({"predictions": []})

    except HTTPException as http_err:
        logger.error(f"HTTP Exception during prediction: {http_err.status_code} - {http_err.detail}")
        raise http_err
    except Exception as e:
        logger.error(f"Unexpected error during prediction: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An internal error occurred during prediction.")

async def process_camera_prediction(
    cam_id: str,
    collection_name: str,
    start_date: Optional[str],
    end_date: Optional[str],
    start_time: Optional[str],
    end_time: Optional[str]
):
    """
    Process prediction for a single camera with optimized data retrieval.
    Only fetches the minimum data needed for prediction.
    """
    try:
        logger.info(f"Fetching optimized data for prediction - Camera ID: {cam_id}")
        
        # Create query for this specific camera
        cam_query = SearchQuery(
            camera_id=[cam_id],
            start_date=start_date,
            end_date=end_date,
            start_time=start_time,
            end_time=end_time
        )

        # OPTIMIZATION: Only fetch the fields needed for prediction
        # This reduces payload size and speeds up transmission
        filter_obj = build_filter_from_query(cam_query)
        
        # Fetch limited number of points with only the needed fields
        points, _ = qdrant_client.scroll(
            collection_name=collection_name,
            limit=250,  # Limit to reasonable number needed for prediction
            with_payload=["timestamp", "person_count", "camera_id", "name"],
            with_vectors=False,
            scroll_filter=filter_obj,
        )
        
        if not points:
            logger.info(f"No data found for camera {cam_id} with the given filters.")
            return None
            
        # Extract camera name from the first result
        camera_name = points[0].payload.get("name", "Unknown")
        
        # Prepare data in the format expected by make_prediction
        # Only include the fields needed for prediction
        search_results = []
        for point in points:
            if not point.payload:
                continue
                
            result = {
                "metadata": {
                    "camera_id": point.payload.get("camera_id"),
                    "timestamp": point.payload.get("timestamp"),
                    "person_count": point.payload.get("person_count", 0)
                }
            }
            search_results.append(result)
            
        # Sort results by timestamp (newest first) if needed by prediction algorithm
        search_results.sort(key=lambda x: x['metadata']['timestamp'], reverse=True)
        
        # Call prediction function with the optimized dataset
        prediction = make_prediction(cam_id, search_results)
        return {"camera_id": cam_id, "name": camera_name, "prediction": prediction}
        
    except Exception as e:
        logger.error(f"Error processing prediction for camera {cam_id}: {e}", exc_info=True)
        return None

@router.get("/timestamp-range", response_model=TimestampRangeResponse)
async def get_timestamp_range(camera_id: Optional[str] = None, username: str = Depends(session_manager.get_current_user)):
    """
    Get the first and last timestamps in the Qdrant database.
    Optimized version that uses targeted queries instead of fetching all data.
    """
    try:
        # Get the user-specific collection name
        user_collection = get_user_collection_name(username)
        
        # Parse camera IDs if provided
        camera_ids = None
        if camera_id:
            camera_ids = parse_camera_ids(camera_id)
        
        # Use the efficient implementation
        return await get_timestamp_range_efficient(camera_ids, user_collection)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving timestamp range: {str(e)}")

async def perform_search(query: SearchQuery, collection_name: str):
    """
    Performs a search based on the SearchQuery parameters.
    """
    logger.info(f"Performing search in '{collection_name}' with query: {query.dict()}")
    try:
        # Create filter conditions
        must_conditions = []
        
        # 1. Handle camera_id filter
        if query.camera_id:
            # We now assume query.camera_id is already a properly parsed list
            camera_ids = query.camera_id if isinstance(query.camera_id, list) else [query.camera_id]
            
            # Log the camera IDs we're using for search
            logger.info(f"Using camera IDs for search: {camera_ids}")
            
            if camera_ids:
                if len(camera_ids) > 1:
                    # For multiple camera IDs, create a different approach using filter constructor
                    # Create an OR condition by using a separate Filter for each camera_id
                    camera_filter = models.Filter(
                        should=[
                            models.FieldCondition(
                                key="camera_id",
                                match=models.MatchValue(value=cam_id)
                            )
                            for cam_id in camera_ids
                        ]
                        # Remove the minimum_should_match parameter
                    )
                    
                    # Add this to the main filter's must conditions
                    must_conditions.append(camera_filter)
                    logger.info(f"Added OR filter for {len(camera_ids)} camera IDs: {camera_ids}")
                else:
                    # Single camera ID - simple match
                    must_conditions.append(
                        models.FieldCondition(
                            key="camera_id",
                            match=models.MatchValue(value=camera_ids[0])
                        )
                    )
                    logger.info(f"Added single camera filter: {camera_ids[0]}")

        # Rest of the function remains unchanged
        # 2. Handle date and time filters for timestamp range
        start_timestamp = None
        end_timestamp = None

        try:
            # Determine start datetime
            if query.start_date:
                start_date_obj = parse_date_format(query.start_date)
                start_time_obj = parse_time_string(query.start_time, time.min)
                start_datetime = datetime.combine(start_date_obj, start_time_obj)
                start_timestamp = start_datetime.timestamp()
                logger.debug(f"Calculated start timestamp: {start_timestamp} ({start_datetime})")

            # Determine end datetime
            if query.end_date:
                end_date_obj = parse_date_format(query.end_date)
                # Use end of day if no end_time is specified
                end_time_obj = parse_time_string(query.end_time, time.max.replace(microsecond=0))
                end_datetime = datetime.combine(end_date_obj, end_time_obj)
                end_timestamp = end_datetime.timestamp()
                logger.debug(f"Calculated end timestamp: {end_timestamp} ({end_datetime})")

            # Build timestamp range condition only if we have timestamps
            if start_timestamp is not None or end_timestamp is not None:
                timestamp_range = {}
                if start_timestamp is not None:
                    timestamp_range["gte"] = start_timestamp
                if end_timestamp is not None:
                    timestamp_range["lte"] = end_timestamp
                
                must_conditions.append(
                    models.FieldCondition(
                        key="timestamp",
                        range=models.Range(**timestamp_range)
                    )
                )
                logger.debug(f"Added timestamp range filter: {timestamp_range}")

        except ValueError as date_err:
            logger.error(f"Invalid date format provided: {date_err}")
            raise HTTPException(status_code=400, detail=f"Invalid date format: {date_err}")
        except Exception as dt_err:
            logger.error(f"Error processing date/time filters: {dt_err}", exc_info=True)
            raise HTTPException(status_code=400, detail=f"Error processing date/time parameters: {str(dt_err)}")
        
        # Create the main filter using the collected filter parts
        main_filter = None
        if must_conditions:
            main_filter = models.Filter(must=must_conditions)
            logger.info(f"Created filter with {len(must_conditions)} conditions")
            logger.debug(f"Full filter: {main_filter}")
        
        # Debug print for troubleshooting
        if query.camera_id and isinstance(query.camera_id, list) and len(query.camera_id) > 1:
            logger.info(f"Multi-camera query filter structure: {main_filter}")
            print(f"DEBUG - Multi-camera query filter: {main_filter}")

        # First, get the total count of matching documents
        if main_filter:
            count_result = qdrant_client.count(
                collection_name=collection_name,
                count_filter=main_filter
            )
            logger.info(f"Using count_filter with Qdrant count method")
        else:
            count_result = qdrant_client.count(collection_name=collection_name)
            logger.info("No filters applied for count.")

        total_count = count_result.count
        logger.info(f"Found {total_count} matching records (count query).")

        fetch_limit = config.get("qdrant_fetch_limit", 50)
        all_points = []

        if total_count > 0:
            logger.info(f"Scrolling data with limit={fetch_limit}...")
            
            # Use scroll API to retrieve all points
            offset = None
            while True:
                if main_filter:
                    points, offset = qdrant_client.scroll(
                        collection_name=collection_name,
                        limit=fetch_limit,
                        with_payload=True,
                        with_vectors=False,
                        scroll_filter=main_filter,
                        offset=offset
                    )
                else:
                    points, offset = qdrant_client.scroll(
                        collection_name=collection_name,
                        limit=fetch_limit,
                        with_payload=True,
                        with_vectors=False,
                        offset=offset
                    )
                
                if not points:
                    break
                    
                all_points.extend(points)
                logger.debug(f"Retrieved {len(points)} points. Total: {len(all_points)}. Next offset: {offset}")
                
                if offset is None:
                    break  # No more results

            logger.info(f"Scroll completed. Retrieved {len(all_points)} total points.")

        # Format Results
        search_results = []
        for point in all_points:
            if not point.payload:
                continue
                
            result = {
                "frame": point.payload.get("frame"),
                "metadata": {
                    "camera_id": point.payload.get("camera_id"),
                    "name": point.payload.get("name", "Unknown"),
                    "timestamp": point.payload.get("timestamp"),
                    "date": point.payload.get("date"),
                    "time": point.payload.get("time"),
                    "person_count": point.payload.get("person_count", 0)
                }
            }
            search_results.append(result)

        # Sort results by timestamp descending (newest first)
        search_results.sort(key=lambda x: x.get('metadata', {}).get('timestamp', 0), reverse=True)
        logger.info(f"Formatted and sorted {len(search_results)} results.")

        # Return both the results, limit and the total count
        return {
            "results": search_results,
            "limit": fetch_limit,
            "total_count": total_count
        }
    
    except HTTPException:
        raise  # Re-raise HTTP exceptions
    except Exception as e:
        logger.error(f"Qdrant search/scroll error in '{collection_name}': {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Search operation failed: {str(e)}"
        )

# Alternative implementation with more explicit type handling
@router.get("/camera-ids-alt", response_model=CameraIdsResponse)
async def get_all_camera_ids_alternative(username: str = Depends(session_manager.get_current_user)):
    """
    Alternative implementation with more explicit type handling.
    """
    try:
        # Get the user-specific collection name
        user_collection = get_user_collection_name(username)
        
        # Initialize collection for this user if it doesn't exist
        init_collection(qdrant_client, user_collection)

        # Fetch all records with only camera_id payload
        results = qdrant_client.scroll(
            collection_name=user_collection,
            limit=10000,
            with_payload=["camera_id"]
        )[0]
        
        # Extract unique camera IDs with type preservation
        unique_camera_ids = set()
        
        for point in results:
            if "camera_id" in point.payload:
                camera_id = point.payload["camera_id"]
                # Keep original type (int or str)
                unique_camera_ids.add(camera_id)
        
        # Convert to list and sort (ints and strings separately if needed)
        camera_ids_list = list(unique_camera_ids)
        
        # Sort numerically if all are integers, otherwise lexicographically
        if all(isinstance(cid, int) for cid in camera_ids_list):
            camera_ids_list.sort()  # Sort numerically for integers
        else:
            # Convert all to strings for consistent sorting if mixed types
            camera_ids_list = sorted(camera_ids_list, key=str)
        
        return CameraIdsResponse(
            camera_ids=camera_ids_list,
            count=len(camera_ids_list)
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving camera IDs: {str(e)}")

@router.delete("/collections/{collection_name}")
async def delete_collection(collection_name: str, force: bool = Query(False), username: str = Depends(session_manager.get_current_user)):
    """
    Delete a collection from Qdrant database.
    
    Parameters:
    - collection_name: Name of the collection to delete
    - force: If True, delete without additional confirmation (default: False)
    """
    try:
        client = create_qdrant_client()
        collections = client.get_collections()
        collection_names = [c.name for c in collections.collections]
        
        if collection_name not in collection_names:
            raise HTTPException(status_code=404, detail=f"Collection '{collection_name}' not found")
        
        # Delete the collection
        client.delete_collection(collection_name=collection_name)
        
        return {"status": "success", "message": f"Collection '{collection_name}' deleted successfully"}
    except Exception as e:
        logging.error(f"Failed to delete Qdrant collection: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to delete collection: {str(e)}")

@router.delete("/delete_data")
async def delete_data(request: DeleteDataRequest, collection_name: str, username: str = Depends(session_manager.get_current_user)):
    """
    Delete data from a collection based on filter conditions.
    """
    try:
        client = create_qdrant_client()
        # collection_name = config.get("qdrant_collection_name", "person_counts")
        must_conditions = []
        
        # Handle camera_id filter
        if request.camera_id:
            camera_ids = parse_camera_ids(request.camera_id)
            if camera_ids:
                if len(camera_ids) > 1:
                    should_conditions = [
                        models.FieldCondition(
                            key="camera_id",
                            match=models.MatchValue(value=cam_id)
                        )
                        for cam_id in camera_ids
                    ]
                    must_conditions.append(models.Or(should=should_conditions))
                else:
                    must_conditions.append(
                        models.FieldCondition(
                            key="camera_id",
                            match=models.MatchValue(value=camera_ids[0])
                        )
                    )

        # Handle date and time filters
        if request.start_date or request.end_date:
            try:
                range_params = {}
                
                if request.start_date:
                    start_date = parse_date_format(request.start_date)
                    start_datetime = datetime.combine(start_date, datetime.time.min)
                    range_params["gte"] = start_datetime.timestamp()
                    
                if request.end_date:
                    end_date = parse_date_format(request.end_date)
                    end_datetime = datetime.combine(end_date, datetime.time.max)
                    range_params["lte"] = end_datetime.timestamp()
                
                # Only add the range condition if we have valid parameters
                if range_params:
                    must_conditions.append(
                        models.FieldCondition(
                            key="timestamp",
                            range=models.Range(**range_params)
                        )
                    )
            except Exception as e:
                logging.error(f"Date filtering error: {str(e)}")
                raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
        
        # Get count before deletion for reporting
        count_params = {
            "collection_name": collection_name
        }

        # # Create filter
        # delete_filter = None
        # if must_conditions:
        #     delete_filter = models.Filter(must=must_conditions)
        
        
        # if delete_filter:
        #     count_params["count_filter"] = delete_filter
        
        # count_result = client.count(**count_params)
        
        # # Delete points based on filter
        # delete_result = client.delete(
        #     collection_name=collection_name,
        #     points_selector=models.FilterSelector(filter=delete_filter) if delete_filter else None
        # )
        if must_conditions:
            delete_filter = models.Filter(must=must_conditions)
            count_params["count_filter"] = delete_filter
        
            # Delete points based on filter
            delete_result = client.delete(
                collection_name=collection_name,
                points_selector=models.FilterSelector(filter=delete_filter)
            )
        else:
            # If no filter is provided, we'll delete all points
            # Using scroll ID method to delete all points without a filter
            logging.info("No filter provided, deleting all points in the collection")
            
            # Get all point IDs
            scroll_result = client.scroll(
                collection_name=collection_name,
                limit=10000  # Adjust this based on your collection size
            )
            points = scroll_result[0]
            point_ids = [point.id for point in points]
            
            if point_ids:
                delete_result = client.delete(
                    collection_name=collection_name,
                    points_selector=models.PointIdsList(points=point_ids)
                )
            else:
                logging.info("No points found in the collection")
                
        count_result = client.count(**count_params)
        
        return {
            "status": "success", 
            "message": f"Successfully deleted data matching the criteria",
            "deleted_count": count_result.count
        }
        
    except Exception as e:
        logging.error(f"Failed to delete data: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to delete data: {str(e)}")

@router.get("/collections")
async def list_collections(username: str = Depends(session_manager.get_current_user)):
    """
    List all available collections in the Qdrant database.
    """
    try:
        client = create_qdrant_client()
        collections = client.get_collections()
        
        # Get collection details with proper attributes
        collection_info = []
        for c in collections.collections:
            try:
                # Get the collection info which includes vector count
                details = client.get_collection(collection_name=c.name)
                collection_info.append({
                    "name": c.name,
                    "points_count": details.points_count if hasattr(details, "points_count") else "Unknown"
                })
            except Exception as e:
                collection_info.append({
                    "name": c.name,
                    "points_count": "Error fetching details"
                })
        
        return {"collections": collection_info}
    except Exception as e:
        logging.error(f"Failed to list Qdrant collections: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list collections: {str(e)}")

@router.get("/collections/{collection_name}/count")
async def get_collection_count(
    collection_name: str,
    query: SearchQuery = Depends(),
    username: str = Depends(session_manager.get_current_user)
):
    """
    Get count of data in a collection with optional filters.
    
    Parameters:
    - collection_name: Name of the collection to query
    - query: SearchQuery with filter parameters (camera_id, date range, etc.)
    """
    try:
        client = create_qdrant_client()
        collections = client.get_collections()
        collection_names = [c.name for c in collections.collections]
        
        if collection_name not in collection_names:
            raise HTTPException(status_code=404, detail=f"Collection '{collection_name}' not found")
        
        # Build filter conditions
        must_conditions = []
        
        # Handle camera_id filter
        if query.camera_id:
            camera_ids = parse_camera_ids(query.camera_id)
            if camera_ids:
                if len(camera_ids) > 1:
                    should_conditions = [
                        models.FieldCondition(
                            key="camera_id",
                            match=models.MatchValue(value=cam_id)
                        )
                        for cam_id in camera_ids
                    ]
                    must_conditions.append(models.Or(should=should_conditions))
                else:
                    must_conditions.append(
                        models.FieldCondition(
                            key="camera_id",
                            match=models.MatchValue(value=camera_ids[0])
                        )
                    )

        # Handle date filters
        if query.start_date or query.end_date:
            try:
                range_params = {}
                
                if query.start_date:
                    start_date = parse_date_format(query.start_date)
                    start_time = datetime.time.min
                    
                    # If start_time is provided, use it
                    if query.start_time:
                        try:
                            # Parse time in format HH:MM
                            hours, minutes = map(int, query.start_time.split(':'))
                            start_time = datetime.time(hour=hours, minute=minutes)
                        except ValueError:
                            logging.warning(f"Invalid start_time format: {query.start_time}. Using default.")
                    
                    start_datetime = datetime.combine(start_date, start_time)
                    range_params["gte"] = start_datetime.timestamp()
                    
                if query.end_date:
                    end_date = parse_date_format(query.end_date)
                    end_time = datetime.time.max
                    
                    # If end_time is provided, use it
                    if query.end_time:
                        try:
                            # Parse time in format HH:MM
                            hours, minutes = map(int, query.end_time.split(':'))
                            end_time = datetime.time(hour=hours, minute=minutes)
                        except ValueError:
                            logging.warning(f"Invalid end_time format: {query.end_time}. Using default.")
                    
                    end_datetime = datetime.combine(end_date, end_time)
                    range_params["lte"] = end_datetime.timestamp()
                
                # Only add the range condition if we have valid parameters
                if range_params:
                    must_conditions.append(
                        models.FieldCondition(
                            key="timestamp",
                            range=models.Range(**range_params)
                        )
                    )
            except Exception as e:
                logging.error(f"Date filtering error: {str(e)}")
                raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
        
        # Prepare count parameters
        count_params = {
            "collection_name": collection_name
        }
        
        # Add filter to count parameters if conditions exist
        if must_conditions:
            count_params["count_filter"] = models.Filter(must=must_conditions)
        
        # Get count from Qdrant
        count_result = client.count(**count_params)
        
        # Return the result
        return {
            "collection_name": collection_name,
            "count": count_result.count,
            "filters": {
                "camera_id": query.camera_id,
                "start_date": query.start_date,
                "end_date": query.end_date,
                "start_time": query.start_time,
                "end_time": query.end_time
            }
        }
        
    except Exception as e:
        logging.error(f"Failed to get collection count: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get collection count: {str(e)}")

@router.post("/collections")
async def create_collection(request: CreateCollectionRequest, username: str = Depends(session_manager.get_current_user)):
    """
    Create a new collection in Qdrant database.
    
    Follows the same pattern as your existing init_collection function
    but allows for custom collection names and parameters.
    """
    try:
        client = create_qdrant_client()
        
        # Check if collection already exists
        collections = client.get_collections()
        collection_names = [c.name for c in collections.collections]
        
        if request.collection_name in collection_names:
            return {"status": "success", "message": f"Collection '{request.collection_name}' already exists"}
        
        # Map string distance to enum
        distance_map = {
            "DOT": models.Distance.DOT,
            "COSINE": models.Distance.COSINE,
            "EUCLID": models.Distance.EUCLID
        }
        
        distance = distance_map.get(request.distance.upper())
        if not distance:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid distance metric. Use one of: DOT, COSINE, EUCLID"
            )
        
        # Create the collection using similar parameters to your init_collection function
        client.create_collection(
            collection_name=request.collection_name,
            vectors_config=models.VectorParams(
                size=request.vector_size,  
                distance=distance
            )
        )
        
        return {
            "status": "success", 
            "message": f"Collection '{request.collection_name}' created successfully"
        }
    except Exception as e:
        logging.error(f"Failed to create Qdrant collection: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to create collection: {str(e)}")

# ###################
# @router.get("/search_results")
# async def search_results(
#     request: Request, 
#     camera_id: Optional[str] = Query(None), 
#     start_date: Optional[str] = Query(None), 
#     end_date: Optional[str] = Query(None), 
#     start_time: Optional[str] = Query(None), 
#     end_time: Optional[str] = Query(None), 
#     page: Optional[int] = Query(1), 
#     per_page: Optional[int] = Query(12), 
#     username: str = Depends(session_manager.get_current_user)):
#     """
#     Optimized endpoint to search for recorded data with pagination and filtering.
#     """
#     logger.info(f"Search request for user '{username}' - Page: {page}, PerPage: {per_page}, Params: camera={camera_id}, date={start_date}-{end_date}, time={start_time}-{end_time}")

#     try:
#         # Check user permissions - keep this part unchanged
#         user_data = await user_manager.get_user_by_username(username)
#         user_id = user_data["user_id"]
#         is_search = await user_manager.get_search_status(user_id)

#         if not is_search:
#             logger.warning(f"User '{username}' (ID: {user_id}) attempted search without permission.")
#             raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Search feature unavailable. Check your subscription.")

#         # Get the user-specific collection name
#         user_collection = get_user_collection_name(username)
        
#         # Parse camera IDs
#         camera_ids = None
#         if camera_id:
#             camera_ids = [cam_id.strip() for cam_id in camera_id.split(',') if cam_id.strip()]
#             logger.info(f"Parsed camera IDs from URL: {camera_ids}")
        
#         # Create query object with the provided parameters
#         query = SearchQuery(
#             camera_id=camera_ids,
#             start_date=start_date,
#             end_date=end_date,
#             start_time=start_time,
#             end_time=end_time
#         )
        
#         # OPTIMIZATION 1: Calculate offset and limit for direct pagination in Qdrant
#         offset = (page - 1) * per_page
        
#         # OPTIMIZATION 2: Add metadata fetch in one pass
#         # First, get the total count for pagination metadata
#         total_count = await get_results_count(query, user_collection)
        
#         # Calculate pagination details
#         if total_count == 0:
#             num_of_pages = 0
#             current_page = 0
#             current_page_results = []
#         else:
#             num_of_pages = (total_count + per_page - 1) // per_page
#             current_page = max(1, min(page, num_of_pages))
            
#             # OPTIMIZATION 3: Get only the records needed for this page
#             current_page_results = await get_paginated_results(query, user_collection, offset, per_page)

#         logger.info(f"Search successful for '{username}'. Total results: {total_count}, Pages: {num_of_pages}. Returning page {current_page} with {len(current_page_results)} items.")

#         # OPTIMIZATION 4: Only fetch timestamp range when needed and cache it
#         metadata_ts = {}
#         if total_count > 0:
#             try:
#                 # Use a more efficient function for timestamp range
#                 result_ts = await get_timestamp_range_efficient(camera_ids, username)
#                 metadata_ts = {
#                     "first_timestamp": result_ts.first_timestamp,
#                     "last_timestamp": result_ts.last_timestamp,
#                     "first_datetime": result_ts.first_datetime,
#                     "last_datetime": result_ts.last_datetime,
#                     "first_date": result_ts.first_date,
#                     "last_date": result_ts.last_date,
#                     "first_time": result_ts.first_time,
#                     "last_time": result_ts.last_time,
#                     "camera_id": result_ts.camera_id
#                 }
#             except Exception as ts_err:
#                 logger.warning(f"Could not get timestamp range metadata: {ts_err}")
#                 # Continue without metadata if it fails

#         return JSONResponse(content={
#             "data": current_page_results,
#             "current_page": current_page if total_count > 0 else 1,
#             "num_of_pages": num_of_pages,
#             "total_count": total_count,
#             "metadata": metadata_ts,
#             "per_page": per_page
#         })

#     except HTTPException as http_err:
#         logger.error(f"HTTP Exception during search for '{username}': {http_err.status_code} - {http_err.detail}")
#         raise http_err
#     except Exception as e:
#         logger.error(f"Unexpected error during search for user '{username}': {e}", exc_info=True)
#         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An internal error occurred during search.")

# async def get_results_count(query: SearchQuery, collection_name: str):
#     """
#     Get only the count of matching results
#     """
#     try:
#         # Create filter conditions
#         filter_obj = build_filter_from_query(query)
        
#         # Get just the count using the filter
#         if filter_obj:
#             count_result = qdrant_client.count(
#                 collection_name=collection_name,
#                 count_filter=filter_obj  
#             )
#         else:
#             count_result = qdrant_client.count(collection_name=collection_name)
            
#         return count_result.count
        
#     except Exception as e:
#         logger.error(f"Error getting result count: {e}", exc_info=True)
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=f"Count operation failed: {str(e)}"
#         )

# async def get_paginated_results(query: SearchQuery, collection_name: str, offset: int, limit: int):
#     """
#     Get only the results needed for the current page
#     """
#     try:
#         # Create filter conditions
#         filter_obj = build_filter_from_query(query)
        
#         # OPTIMIZATION: Use offset + limit to get only records for current page
#         # Use scroll API with pagination
#         points, _ = qdrant_client.scroll(
#             collection_name=collection_name,
#             limit=limit,
#             offset=offset,  # Use calculated offset from pagination
#             with_payload=True,
#             with_vectors=False,
#             scroll_filter=filter_obj,
#         )
        
#         # Format results
#         search_results = []
#         for point in points:
#             if not point.payload:
#                 continue
                
#             result = {
#                 "frame": point.payload.get("frame"),
#                 "metadata": {
#                     "camera_id": point.payload.get("camera_id"),
#                     "name": point.payload.get("name", "Unknown"),
#                     "timestamp": point.payload.get("timestamp"),
#                     "date": point.payload.get("date"),
#                     "time": point.payload.get("time"),
#                     "person_count": point.payload.get("person_count", 0)
#                 }
#             }
#             search_results.append(result)
            
#         # Sort results by timestamp (newer first)
#         search_results.sort(key=lambda x: x.get('metadata', {}).get('timestamp', 0), reverse=True)
            
#         return search_results
        
#     except Exception as e:
#         logger.error(f"Error getting paginated results: {e}", exc_info=True)
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=f"Search operation failed: {str(e)}"
#         )

# def build_filter_from_query(query: SearchQuery):
#     """
#     Helper function to build a Qdrant filter from search query parameters
#     """
#     must_conditions = []
    
#     # 1. Handle camera_id filter
#     if query.camera_id and isinstance(query.camera_id, list) and len(query.camera_id) > 0:
#         if len(query.camera_id) > 1:
#             # Multiple camera IDs - use 'should' for OR condition
#             camera_filter = models.Filter(
#                 should=[
#                     models.FieldCondition(
#                         key="camera_id",
#                         match=models.MatchValue(value=cam_id)
#                     )
#                     for cam_id in query.camera_id
#                 ]
#             )
#             must_conditions.append(camera_filter)
#         else:
#             # Single camera ID - simple match
#             must_conditions.append(
#                 models.FieldCondition(
#                     key="camera_id",
#                     match=models.MatchValue(value=query.camera_id[0])
#                 )
#             )
    
#     # 2. Handle date and time filters
#     start_timestamp = None
#     end_timestamp = None
    
#     try:
#         # Determine start datetime
#         if query.start_date:
#             start_date_obj = parse_date_format(query.start_date)
#             start_time_obj = parse_time_string(query.start_time, time.min)
#             start_datetime = datetime.combine(start_date_obj, start_time_obj)
#             start_timestamp = start_datetime.timestamp()
            
#         # Determine end datetime
#         if query.end_date:
#             end_date_obj = parse_date_format(query.end_date)
#             end_time_obj = parse_time_string(query.end_time, time.max.replace(microsecond=0))
#             end_datetime = datetime.combine(end_date_obj, end_time_obj)
#             end_timestamp = end_datetime.timestamp()
            
#         # Build timestamp range condition
#         if start_timestamp is not None or end_timestamp is not None:
#             timestamp_range = {}
#             if start_timestamp is not None:
#                 timestamp_range["gte"] = start_timestamp
#             if end_timestamp is not None:
#                 timestamp_range["lte"] = end_timestamp
            
#             must_conditions.append(
#                 models.FieldCondition(
#                     key="timestamp",
#                     range=models.Range(**timestamp_range)
#                 )
#             )
#     except ValueError as date_err:
#         logger.error(f"Invalid date format provided: {date_err}")
#         raise HTTPException(status_code=400, detail=f"Invalid date format: {date_err}")
    
#     # Create main filter if we have conditions
#     if must_conditions:
#         return models.Filter(must=must_conditions)
#     return None

# async def get_timestamp_range_efficient(camera_id: Optional[str], username: str):
#     """
#     Efficient implementation of timestamp range query that works with older Qdrant client versions
#     """
#     try:
#         # Get user collection
#         user_collection = get_user_collection_name(username)
#         filter_obj = None
        
#         # Add camera filter if needed
#         if camera_id:
#             camera_ids = parse_camera_ids(camera_id) if isinstance(camera_id, str) else camera_id
            
#             if camera_ids and len(camera_ids) > 0:
#                 if len(camera_ids) > 1:
#                     filter_obj = models.Filter(
#                         should=[
#                             models.FieldCondition(
#                                 key="camera_id", 
#                                 match=models.MatchValue(value=cam_id)
#                             )
#                             for cam_id in camera_ids
#                         ]
#                     )
#                 else:
#                     filter_obj = models.Filter(
#                         must=[
#                             models.FieldCondition(
#                                 key="camera_id",
#                                 match=models.MatchValue(value=camera_ids[0])
#                             )
#                         ]
#                     )
        
#         result = TimestampRangeResponse()
        
#         # Fetch a small batch of records with timestamps
#         # Since we can't use order_by in older versions, we'll fetch a small batch 
#         # and find min/max locally
#         points, _ = qdrant_client.scroll(
#             collection_name=user_collection,
#             limit=50,  # Keep this number small but enough to likely contain the min/max
#             with_payload=["timestamp", "camera_id"],
#             scroll_filter=filter_obj,
#         )
        
#         if points:
#             # Extract timestamps from points
#             timestamps = [point.payload.get("timestamp") for point in points if point.payload and "timestamp" in point.payload]
            
#             if timestamps:
#                 # Find min and max timestamps in the sample
#                 min_timestamp = min(timestamps)
#                 max_timestamp = max(timestamps)
                
#                 # We need to ensure these are truly the min/max across all data
#                 # Create specific filters for values less than min and greater than max
                
#                 # Check if there are earlier records
#                 earlier_filter = models.Filter(
#                     must=[
#                         models.FieldCondition(
#                             key="timestamp",
#                             range=models.Range(lt=min_timestamp)
#                         )
#                     ]
#                 )
#                 if filter_obj and hasattr(filter_obj, 'must') and filter_obj.must:
#                     earlier_filter.must.extend(filter_obj.must)
                    
#                 earlier_points, _ = qdrant_client.scroll(
#                     collection_name=user_collection,
#                     limit=1,
#                     with_payload=["timestamp"],
#                     scroll_filter=earlier_filter
#                 )
                
#                 if earlier_points:
#                     min_timestamp = earlier_points[0].payload.get("timestamp", min_timestamp)
                
#                 # Check if there are later records
#                 later_filter = models.Filter(
#                     must=[
#                         models.FieldCondition(
#                             key="timestamp",
#                             range=models.Range(gt=max_timestamp)
#                         )
#                     ]
#                 )
#                 if filter_obj and hasattr(filter_obj, 'must') and filter_obj.must:
#                     later_filter.must.extend(filter_obj.must)
                    
#                 later_points, _ = qdrant_client.scroll(
#                     collection_name=user_collection,
#                     limit=1,
#                     with_payload=["timestamp"],
#                     scroll_filter=later_filter
#                 )
                
#                 if later_points:
#                     max_timestamp = later_points[0].payload.get("timestamp", max_timestamp)
                
#                 # Now we have true min/max timestamps
#                 result.first_timestamp = min_timestamp
#                 result.last_timestamp = max_timestamp
                
#                 first_dt = datetime.fromtimestamp(result.first_timestamp)
#                 last_dt = datetime.fromtimestamp(result.last_timestamp)
                
#                 result.first_datetime = first_dt.isoformat()
#                 result.last_datetime = last_dt.isoformat()
#                 result.first_date = first_dt.date().isoformat()
#                 result.last_date = last_dt.date().isoformat()
#                 result.first_time = first_dt.time().isoformat()
#                 result.last_time = last_dt.time().isoformat()
                
#                 # Include camera_id in response if filter was for a single camera
#                 if camera_id and not isinstance(camera_id, list) and ',' not in camera_id:
#                     result.camera_id = camera_id
        
#         return result
        
#     except Exception as e:
#         logger.error(f"Error getting timestamp range efficiently: {e}", exc_info=True)
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail=f"Error retrieving timestamp range: {str(e)}"
#         )


# ###################
# @router.get("/search_results")
# async def search_results(
#     request: Request, 
#     camera_id: Optional[str] = Query(None), 
#     start_date: Optional[str] = Query(None), 
#     end_date: Optional[str] = Query(None), 
#     start_time: Optional[str] = Query(None), 
#     end_time: Optional[str] = Query(None), 
#     page: Optional[int] = Query(1), 
#     per_page: Optional[int] = Query(12), 
#     username: str = Depends(session_manager.get_current_user)):
#     """
#     Endpoint to search for recorded data with pagination and filtering.
#     """
#     logger.info(f"Search request for user '{username}' - Page: {page}, PerPage: {per_page}, Params: camera={camera_id}, date={start_date}-{end_date}, time={start_time}-{end_time}")

#     try:
#         user_data = await user_manager.get_user_by_username(username)
#         user_id = user_data["user_id"]
#         is_search = await user_manager.get_search_status(user_id)

#         if not is_search:
#             logger.warning(f"User '{username}' (ID: {user_id}) attempted search without permission.")
#             # Return a more standard error response
#             raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Search feature unavailable. Check your subscription.")

#         # Get the user-specific collection name
#         user_collection = get_user_collection_name(username)
#         logger.debug(f"Using collection: {user_collection}")
        
#         # Ensure collection exists (consider if this check is needed on every search)
#         init_collection(qdrant_client, user_collection) # Might slow down requests

#         # Handle camera_id parsing directly here
#         camera_ids = None
#         if camera_id:
#             # Explicitly split by comma - this is the key fix
#             camera_ids = [cam_id.strip() for cam_id in camera_id.split(',') if cam_id.strip()]
#             logger.info(f"Manually parsed camera IDs from URL: {camera_ids}")
        
#         # Create query object with the provided parameters
#         query = SearchQuery(
#             camera_id=camera_ids,  # Use our manually parsed camera IDs
#             start_date=start_date,
#             end_date=end_date,
#             start_time=start_time,
#             end_time=end_time
#         )
#         # Perform the search using the updated function
#         search_data = await perform_search(query, user_collection)

#         all_results = search_data["results"]
#         total_count = search_data["total_count"]
        
#         # Calculate pagination details based on the *total* count
#         if total_count == 0:
#             num_of_pages = 0
#             current_page = 0
#             current_page_results = []
#         else:
#             num_of_pages = (total_count + per_page - 1) // per_page
#             # Ensure requested page is valid
#             current_page = max(1, min(page, num_of_pages))
#             start_index = (current_page - 1) * per_page
#             end_index = start_index + per_page
#             current_page_results = all_results[start_index:end_index]

#         logger.info(f"Search successful for '{username}'. Total results: {total_count}, Pages: {num_of_pages}. Returning page {current_page} with {len(current_page_results)} items.")

#         #  Get timestamp metadata (consider optimizing this if it's slow)
#         try:
#             result_ts = await get_timestamp_range_alternative(camera_ids, username) # Passing our manually parsed camera_ids
#             metadata_ts = {
#                 "first_timestamp": result_ts.first_timestamp,
#                 "last_timestamp": result_ts.last_timestamp,
#                 "first_datetime": result_ts.first_datetime,
#                 "last_datetime": result_ts.last_datetime,
#                 "first_date": result_ts.first_date,
#                 "last_date": result_ts.last_date,
#                 "first_time": result_ts.first_time,
#                 "last_time": result_ts.last_time,
#                 "camera_id": result_ts.camera_id
#             }
#         except Exception as ts_err:
#              logger.warning(f"Could not get timestamp range metadata: {ts_err}")
#              metadata_ts = {} # Default to empty if fails

#         return JSONResponse(content={
#             "data": current_page_results,
#             "current_page": current_page if total_count > 0 else 1,
#             "num_of_pages": num_of_pages,
#             "limit": search_data.get("limit", 0),
#             "total_count": total_count,
#             "metadata": metadata_ts, # Add timestamp metadata if needed
#             "per_page": per_page,
#         })

#     except HTTPException as http_err:
#          # Log and re-raise known HTTP errors (like 403, 400)
#          logger.error(f"HTTP Exception during search for '{username}': {http_err.status_code} - {http_err.detail}")
#          raise http_err
#     except Exception as e:
#          logger.error(f"Unexpected error during search for user '{username}': {e}", exc_info=True)
#          raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An internal error occurred during search.")


# @router.get("/prediction_data")
# async def prediction_data(
#     request: Request, 
#     camera_id: Optional[str] = Query(None), 
#     start_date:Optional[str] = Query(None), 
#     end_date:Optional[str] = Query(None), 
#     start_time:Optional[str] = Query(None), 
#     end_time:Optional[str] = Query(None), 
#     username: str = Depends(session_manager.get_current_user)):
#     """
#     Endpoint to get prediction data based on filtered results.
#     """
#     logger.info(f"Prediction request for user '{username}' - Params: camera={camera_id}, date={start_date}-{end_date}, time={start_time}-{end_time}")

#     try:
#         user_data = await user_manager.get_user_by_username(username)
#         user_id = user_data["user_id"]
#         is_prediction = await user_manager.get_prediction_status(user_id)

#         if not is_prediction:
#             logger.warning(f"User '{username}' (ID: {user_id}) attempted prediction without permission.")
#             raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Prediction feature unavailable. Check your subscription.")

#         # Get the user-specific collection name
#         user_collection = get_user_collection_name(username)
#         logger.debug(f"Using collection: {user_collection}")

#         # Ensure collection exists (optional check)
#         # init_collection(qdrant_client, user_collection)

#         all_predictions = []

#         if camera_id:
#             # Explicitly split by comma - this is the key fix
#             camera_ids = [cam_id.strip() for cam_id in camera_id.split(',') if cam_id.strip()]
#             logger.info(f"Manually parsed camera IDs for prediction: {camera_ids}")

#             for cam_id in camera_ids:
#                 logger.info(f"Fetching data for prediction - Camera ID: {cam_id}")
#                 # Create a query for this specific camera ID, respecting date/time filters
#                 cam_query = SearchQuery(
#                     camera_id=[cam_id], # Pass a list with a single camera ID
#                     start_date=start_date,
#                     end_date=end_date,
#                     start_time=start_time,
#                     end_time=end_time
#                 )

#                 # Use the updated perform_search
#                 search_data = await perform_search(cam_query, user_collection)

#                 if search_data["results"]:
#                     logger.info(f"Found {search_data['total_count']} results for camera {cam_id}. Making prediction.")
#                     # Extract the camera name from the first result if available
#                     # Find the first result that actually matches this cam_id (search might return others if query.camera_id was initially None/multi)
#                     first_match = next((r for r in search_data["results"] if r["metadata"]["camera_id"] == cam_id), None)
#                     camera_name = first_match["metadata"]["name"] if first_match else "Unknown"

#                     # Call prediction function (ensure make_prediction handles potential empty list)
#                     prediction = make_prediction(cam_id, search_data["results"]) # Pass all results matching the query
#                     all_predictions.append({"camera_id": cam_id, "name": camera_name, "prediction": prediction})
#                     logger.debug(f"Prediction for {cam_id} ({camera_name}): {prediction}")
#                 else:
#                     logger.info(f"No search results found for camera {cam_id} with the given filters. Skipping prediction.")
#                     # Optionally add an entry indicating no data
#                     # all_predictions.append({"camera_id": cam_id, "name": "Unknown", "prediction": {"error": "No data found for prediction"}})

#             logger.info(f"Prediction processing complete for user '{username}'. Generated {len(all_predictions)} predictions.")
#             return JSONResponse({"predictions": all_predictions})
#         else:
#              # Handle case where no camera_id is provided
#              logger.warning(f"Prediction requested for user '{username}' but no camera_id provided.")
#              return JSONResponse({"predictions": []})

#     except HTTPException as http_err:
#          logger.error(f"HTTP Exception during prediction for '{username}': {http_err.status_code} - {http_err.detail}")
#          raise http_err
#     except Exception as e:
#          logger.error(f"Unexpected error during prediction for user '{username}': {e}", exc_info=True)
#          raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An internal error occurred during prediction.")

###################