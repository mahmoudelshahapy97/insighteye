# app/routes/text_chat_router.py
import logging
from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import StreamingResponse, JSONResponse
from app.schemas import BaseChatRequest, ChatRequest
from app.utils import generate_chat_response
from app.config.settings import config
from app.services.session_service import session_manager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/chat", tags=["chat"])

@router.post("/text")
async def chat_with_text(request: ChatRequest, username: str = Depends(session_manager.get_current_user)):#
    """
    Chat endpoint with enhanced queue management
    
    :param request: Parsed ChatRequest object
    :return: Chat response as a JSONResponse or StreamingResponse
    """
    try:
        logger.info(f"Received request: {request.dict()}")
        
        # Extract parameters
        prompt = request.prompt
        context = request.context
        history = request.history
        system_prompt = request.system_prompt or config['prompts']['text_chat']
        max_tokens = request.max_tokens or config['models']['chat']['max_tokens']
        temperature = request.temperature or config['models']['chat']['temperature']
        response_format = request.format_  # Avoid using the reserved keyword `format`
        stream = request.stream
        
        response_generator = await generate_chat_response(
            prompt=prompt,
            history=history,
            context=context,
            system_prompt=system_prompt,
            max_tokens=max_tokens,
            temperature=temperature,
            stream=stream,
            format_=response_format,
        )

        if request.stream:
            return StreamingResponse(response_generator, media_type="text/plain")
        else:
            return JSONResponse(content=response_generator, status_code=200)
    except Exception as e:
        logger.error(f"Error in chat_with_text: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error occurred")
