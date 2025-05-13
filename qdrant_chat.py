# Import necessary modules
from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import StreamingResponse, JSONResponse
from typing import List, Dict, Optional, Union
from pydantic import BaseModel, Field
import logging
import json
import asyncio
from langchain_ollama import ChatOllama
from langchain.schema import HumanMessage, SystemMessage
from config import config
from session_manager import SessionManager
from schemas_models import ChatRequest, BaseChatRequest, SearchQuery
from video_streaming_qdrant import perform_search, get_user_collection_name
# Setup logging
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ])
logger = logging.getLogger(__name__)


# Initialize router and session manager
router = APIRouter(prefix="/chat", tags=["chat"])
session_manager = SessionManager()

# Model storage
_models = {}

# Function to get or create a chat model
def get_ChatOllama_model(model_id, temperature, num_predict, format_):
    """Get or create LLM model"""
    if model_id not in _models:
        _models[model_id] = ChatOllama(
            model=model_id,
            temperature=temperature,
            num_predict=num_predict,
            format=format_
        )
    return _models[model_id]

# Function to format chat history
def format_chat_history(
    history: Union[str, List[Dict[str, str]]],
    system_prompt: str,
    context: Optional[str] = None
) -> List[Dict[str, str]]:
    """Format chat history with context"""
    if isinstance(history, str):
        history = json.loads(history)
    
    messages = []
    
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    if context:
        messages.append({"role": "system", "content": f"Context: {context}"})
    
    messages.extend(history)
    return messages

# Define search function that can be called by the LLM
def format_search_function():
    return {
        "name": "perform_search",
        "description": "Searches the database for camera data based on filters",
        "parameters": {
            "type": "object",
            "properties": {
                "camera_id": {
                    "type": "string",
                    "description": "Camera ID to filter by. Can be a single ID or comma-separated list."
                },
                "start_date": {
                    "type": "string",
                    "description": "Start date for filtering (format: YYYY-MM-DD)"
                },
                "end_date": {
                    "type": "string",
                    "description": "End date for filtering (format: YYYY-MM-DD)"
                },
                "start_time": {
                    "type": "string",
                    "description": "Start time for filtering (format: HH:MM:SS)"
                },
                "end_time": {
                    "type": "string",
                    "description": "End time for filtering (format: HH:MM:SS)"
                }
            }
        }
    }

# Function to generate a system prompt for Qdrant search
def generate_qdrant_system_prompt():
    return """
    You are an AI assistant that helps users query camera data. You have access to a database of camera records with the following fields:
    - camera_id: The ID of the camera (string)
    - start_date: Date of capture (YYYY-MM-DD)
    - end_date: Date of capture (YYYY-MM-DD)
    - start_time: Time of capture (HH:MM:SS)
    - end_time: Time of capture (HH:MM:SS)
    - person_count: Number of people detected in the frame
    - name: Name of the camera location
    - timestamp: Unix timestamp of when the image was captured
    
    When a user asks a question about camera data, you should:
    1. Determine what filters need to be applied (camera_id, date range, time range)
    2. Call the perform_search function with appropriate parameters
    3. Analyze the results and provide a concise answer
    
    Always be specific and provide data-driven responses.
    """

# Qdrant chat endpoint
@router.post("/qdrant")
async def chat_with_qdrant(
    request: ChatRequest, 
    username: str = Depends(session_manager.get_current_user)
):
    """
    Chat endpoint that uses Qdrant to answer questions about camera data.
    
    :param request: Parsed ChatRequest object
    :return: Chat response as a JSONResponse or StreamingResponse
    """
    try:
        logger.info(f"Received qdrant chat request: {request.dict()}")
        
        # Extract parameters
        prompt = request.prompt
        context = request.context
        history = request.history
        system_prompt = request.system_prompt or generate_qdrant_system_prompt()
        max_tokens = request.max_tokens or config['models']['chat']['max_tokens']
        temperature = request.temperature or config['models']['chat']['temperature']
        response_format = request.format_
        stream = request.stream
        
        # Get user's collection name
        collection_name = get_user_collection_name(username)
        
        # Create or get model
        model_id = config['models']['chat']['llama']
        model = get_ChatOllama_model(model_id, temperature, max_tokens, response_format)
        
        # Format chat history
        messages = format_chat_history(
            history,
            system_prompt=system_prompt,
            context=context
        )
        messages.append({"role": "user", "content": prompt})
        
        # Define the search function
        from langchain.tools import StructuredTool
        from schemas_models import SearchQuery
        
        async def perform_search_wrapper(camera_id=None, start_date=None, end_date=None, start_time=None, end_time=None):
            """Wrapper for the perform_search function"""
            query = SearchQuery(
                camera_id=camera_id,
                start_date=start_date,
                end_date=end_date,
                start_time=start_time,
                end_time=end_time
            )
            return await perform_search(query, collection_name)
        
        search_tool = StructuredTool.from_function(
            func=perform_search_wrapper,
            name="perform_search",
            description="Search camera data with optional filters",
            return_direct=False
        )
        
        # Add tools to the model
        model.bind_tools([search_tool])
        
        # Generate response
        if stream:
            async def generate_response():
                async for chunk in model.astream(messages):
                    yield chunk.content
                    await asyncio.sleep(0.01)
            
            return StreamingResponse(generate_response(), media_type="text/plain")
        else:
            response = await model.ainvoke(messages)
            print(response)
            return JSONResponse(content=response.content, status_code=200)
    
    except Exception as e:
        logger.error(f"Error in chat_with_qdrant: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error occurred: {str(e)}")

'''
search_system_prompt = f"""
You are a Qdrant database expert. Convert questions about camera footage into search parameters.
Available parameters: camera_id, start_date, end_date, start_time, end_time.
Respond with ONLY a JSON object containing relevant parameters. No explanations.
"""

search_system_prompt = """
You are a helpful assistant. When the user asks for data from the Qdrant database, 
respond with a JSON object containing the search parameters: camera_id (string), 
start_date (YYYY-MM-DD), end_date (YYYY-MM-DD), start_time (HH:MM:SS), end_time (HH:MM:SS). 
The database contains camera feed data with fields: camera_id, timestamp, person_count, etc. 
Otherwise, answer normally.
"""
'''
