# qdrant_chat.py

from fastapi import APIRouter, HTTPException, Depends, status
from fastapi.responses import StreamingResponse, JSONResponse
from typing import List, Dict, Optional, Union
import logging
import json
import asyncio

from langchain_ollama import ChatOllama
from langchain_core.tools import StructuredTool
# from langchain_core.schema import HumanMessage, SystemMessage # Not directly used here for message construction

from config import config
from session_manager import SessionManager
from schemas_models import ChatRequest, SearchQuery # SearchQuery is key for the tool
from video_streaming_qdrant import perform_search, get_qdrant_client # Import the actual search function and client getter
from utils import (
    get_workspace_qdrant_collection_name,
    ensure_workspace_qdrant_collection_exists,
    parse_camera_ids # For the wrapper to prepare args for perform_search
)
from workspaces import get_user_and_workspace

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/chat", tags=["chat"])
session_manager = SessionManager()

_models: Dict[str, ChatOllama] = {}

def get_ChatOllama_model(model_id: str, temperature: float, num_predict: int, format_: str) -> ChatOllama:
    model_key = f"{model_id}-{temperature}-{num_predict}-{format_}"
    if model_key not in _models:
        _models[model_key] = ChatOllama(
            model=model_id,
            temperature=temperature,
            num_predict=num_predict,
            format=format_
        )
        logger.info(f"Initialized ChatOllama model with key: {model_key}")
    return _models[model_key]

def format_chat_history(
    history: Union[str, List[Dict[str, str]]],
    system_prompt: str,
    context: Optional[str] = None
) -> List[Dict[str, str]]:
    if isinstance(history, str):
        try:
            history = json.loads(history)
        except json.JSONDecodeError:
            logger.error(f"Failed to parse history JSON string: {history}")
            history = []
    
    messages: List[Dict[str, str]] = []
    
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    if context:
        if messages and messages[0]["role"] == "system":
            messages[0]["content"] += f"\n\nContext: {context}"
        else:
            messages.append({"role": "system", "content": f"Context: {context}"})
    
    valid_history = [{"role": item.get("role"), "content": item.get("content")} 
                     for item in history if isinstance(item, dict) and "role" in item and "content" in item]
    messages.extend(valid_history)
    return messages

def generate_qdrant_system_prompt() -> str:
    # The fields listed here should match what `perform_search` returns in its "metadata"
    return """
    You are an AI assistant that helps users query camera data from their current active workspace.
    You have access to a database of camera records with the following fields available in search results:
    - camera_id: The ID of the camera (string)
    - name: The name/location of the camera (string)
    - timestamp: Unix timestamp of when the image was captured (float)
    - date: Date of capture (YYYY-MM-DD)
    - time: Time of capture (HH:MM:SS)
    - person_count: Number of people detected in the frame (integer)
    
    When a user asks a question about camera data, you should:
    1. Determine what filters might be needed (e.g., specific camera_id, a date range like "today" or "last week", a time range).
    2. Call the 'perform_search' tool with the appropriate parameters.
    3. Analyze the results provided by the tool and answer the user's question concisely.
    
    Always be specific and provide data-driven responses.
    If the user mentions a specific camera (by name or ID), use that in your search.
    The current year is 2024. (You may need to make this dynamic or update it periodically).
    If the search returns many items, mention the total count and provide a summary or examples from the first few results.
    """

@router.post("/qdrant")
async def chat_with_qdrant(
    request: ChatRequest,
    username: str = Depends(session_manager.get_current_user)
):
    try:
        logger.info(f"Received Qdrant chat request from user '{username}': {request.model_dump(exclude_none=True)}")

        _ , active_workspace_id_obj = get_user_and_workspace(username)
        if not active_workspace_id_obj:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No active workspace found. Please activate a workspace to use the chat.")

        workspace_collection_name = get_workspace_qdrant_collection_name(active_workspace_id_obj)
        
        # Ensure the collection exists before attempting to search it
        # get_qdrant_client() is from video_streaming_qdrant.py
        qdrant_cli = get_qdrant_client() 
        await ensure_workspace_qdrant_collection_exists(qdrant_cli, active_workspace_id_obj)
        
        logger.info(f"User '{username}' interacting with workspace '{active_workspace_id_obj}' (Qdrant collection: '{workspace_collection_name}')")

        prompt = request.prompt
        context = request.context or ""
        history = request.history or []
        system_prompt = request.system_prompt or generate_qdrant_system_prompt()
        max_tokens = request.max_tokens or config['models']['chat']['max_tokens']
        temperature = request.temperature if request.temperature is not None else config['models']['chat']['temperature']
        response_format = request.format_
        stream = request.stream

        model_id = config['models']['chat']['llama']
        model = get_ChatOllama_model(model_id, temperature, max_tokens, response_format)

        messages = format_chat_history(history, system_prompt, context)
        messages.append({"role": "user", "content": prompt})
        
        async def perform_search_wrapper(camera_id: Optional[str] = None, 
                                         start_date: Optional[str] = None, 
                                         end_date: Optional[str] = None, 
                                         start_time: Optional[str] = None, 
                                         end_time: Optional[str] = None) -> str:
            logger.info(f"LLM tool 'perform_search_wrapper' invoked for collection '{workspace_collection_name}' with args: "
                        f"camera_id={camera_id}, start_date={start_date}, end_date={end_date}, "
                        f"start_time={start_time}, end_time={end_time}")
            
            query = SearchQuery(
                camera_id=parse_camera_ids(camera_id) if camera_id else None,
                start_date=start_date,
                end_date=end_date,
                start_time=start_time,
                end_time=end_time
            )
            
            # Call the imported perform_search function
            search_result_dict = await perform_search(query, workspace_collection_name)
            
            if search_result_dict and search_result_dict.get("results"):
                limited_results = search_result_dict["results"][:5] # Limit to top 5 for brevity
                
                formatted_results_for_llm = []
                for res_idx, res_item in enumerate(limited_results):
                    meta = res_item.get("metadata", {})
                    # Ensure all keys exist or provide defaults
                    cam_name = meta.get('name', 'Unknown Camera')
                    cam_id_val = meta.get('camera_id', 'N/A')
                    person_c = meta.get('person_count', 0)
                    res_date = meta.get('date', 'N/A')
                    res_time = meta.get('time', 'N/A')
                    
                    formatted_results_for_llm.append(
                        f"{res_idx+1}. Camera: {cam_name} (ID: {cam_id_val}), "
                        f"Detected: {person_c} people, "
                        f"On: {res_date} at {res_time}."
                    )
                
                if not formatted_results_for_llm:
                     return "No specific records found matching the criteria in your active workspace."
                
                summary = (f"Search found {search_result_dict.get('total_count', 0)} records in your active workspace. "
                           f"Showing a few examples:\n" + "\n".join(formatted_results_for_llm))
                logger.debug(f"Search tool result summary for LLM: {summary}")
                return summary
            else:
                logger.info("Search tool found no results.")
                return "No records found matching the criteria in your active workspace."

        search_tool = StructuredTool.from_function(
            func=perform_search_wrapper,
            name="perform_search",
            description="Searches the camera database for records based on provided filters like camera_id, start_date, end_date, start_time, and end_time. The search is automatically scoped to the user's current active workspace.",
            # args_schema=SearchQuery, # Langchain can infer from type hints
            return_direct=False 
        )
        
        try:
            # For ChatOllama, tool binding might require specific model versions or prompt structures.
            # Langchain attempts to make this work, but it's less mature than with OpenAI models.
            llm_with_tools = model.bind_tools([search_tool])
            logger.info("Tool bound to LLM (or Langchain will attempt to manage it).")
        except Exception as e_bind:
            logger.warning(f"Direct tool binding might not be fully supported by the ChatOllama model '{model_id}': {e_bind}. Relying on prompt engineering or agentic behavior if tool use fails.")
            llm_with_tools = model # Fallback

        if stream:
            async def stream_llm_response():
                try:
                    full_response_content = ""
                    async for chunk in llm_with_tools.astream(messages):
                        # Chunk can be AIMessageChunk with content, or AIMessage with tool_calls
                        chunk_content = ""
                        if hasattr(chunk, 'content') and chunk.content is not None:
                            chunk_content = chunk.content
                        elif isinstance(chunk, dict) and chunk.get('content') is not None:
                            chunk_content = chunk['content']
                        
                        if chunk_content:
                            full_response_content += chunk_content
                            yield chunk_content
                        
                        # Handle potential tool calls if model structures them this way in streaming
                        if hasattr(chunk, 'tool_calls') and chunk.tool_calls:
                             logger.info(f"Streaming: LLM initiated tool calls: {chunk.tool_calls}")
                             # or implement the explicit loop of invoking tool and feeding back.
                             # For now, we primarily stream content.
                        
                        await asyncio.sleep(0.01)
                    logger.info(f"LLM streaming finished. Full response starts with: {full_response_content[:100]}...")
                except Exception as stream_err:
                    logger.error(f"Error during streaming LLM response: {stream_err}", exc_info=True)
                    yield f"\n[Error during response generation: {str(stream_err)}]"
            return StreamingResponse(stream_llm_response(), media_type="text/plain")
        else:
            response_message = await llm_with_tools.ainvoke(messages)
            
            final_content = ""
            # Check for tool calls in the response message
            if hasattr(response_message, 'tool_calls') and response_message.tool_calls:
                logger.info(f"LLM invoked tool(s): {response_message.tool_calls}")
                if hasattr(response_message, 'content'):
                    final_content = response_message.content
            elif hasattr(response_message, 'content'):
                final_content = response_message.content
            elif isinstance(response_message, dict) and 'content' in response_message: # Fallback
                final_content = response_message['content']
            else:
                logger.warning(f"Unexpected LLM response structure: {type(response_message)}. Full response: {response_message}")
                final_content = str(response_message)

            logger.info(f"LLM final response content (first 200 chars): {final_content[:200]}")
            return JSONResponse(content={"response": final_content}, status_code=200)
    
    except HTTPException as http_exc:
        logger.error(f"HTTP Exception in chat_with_qdrant (user: {username}): {http_exc.detail}", exc_info=http_exc)
        raise http_exc
    except Exception as e:
        logger.error(f"Error in chat_with_qdrant (user: {username}): {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An internal server error occurred: {str(e)}")
    