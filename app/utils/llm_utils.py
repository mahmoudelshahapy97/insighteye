# app/utils/llm_utils.py
"""LLM utilities for chat and AI model interactions."""

import logging
import json
import re
import asyncio
from typing import List, Dict, Optional, Union, Any

from langchain_community.chat_models import ChatOpenAI
from langchain_ollama import ChatOllama

from app.config.settings import config

logger = logging.getLogger(__name__)

_models: Dict[str, Any] = {}


def get_ChatOllama_model(
    model_id: str, 
    temperature: float, 
    num_predict: int, 
    format_: str
):
    """Get or create ChatOllama model instance."""
    cache_key = f"ollama_{model_id}_{temperature}_{num_predict}_{format_}"
    if cache_key not in _models:
        _models[cache_key] = ChatOllama(
            model=model_id,
            temperature=temperature,
            num_predict=num_predict,
            format=format_ if format_ else None
        )
    return _models[cache_key]


async def generate_chat_response(
    prompt: str,
    history: List[dict],
    context: str,
    system_prompt: str,
    max_tokens: int = 512,
    temperature: float = 0.7,
    stream: bool = False,
    format_: str = "",
    image: Optional[str] = "",
):
    """Core logic for generating a chat response using Ollama models."""
    
    if not image:
        model_id = config.chat_llama
        model = get_ChatOllama_model(model_id, temperature, max_tokens, format_)
        logger.info(f"Using Ollama model {model_id} for text chat.")

        messages = format_chat_history(history, system_prompt=system_prompt, context=context)
        messages.append({"role": "human", "content": prompt})
    else:
        model_id = config.chat_llama
        model = get_ChatOllama_model(model_id, temperature, max_tokens, format_)
        logger.info(f"Using Ollama model {model_id} for image chat.")

        messages = format_chat_history(history, system_prompt=system_prompt, context=context)
        
        content_parts = []
        text_part = {"type": "text", "text": prompt}
        if image: 
            image_part = {
                "type": "image_url",
                "image_url": f"data:image/jpeg;base64,{image}",
            }
            content_parts.append(image_part)
        content_parts.append(text_part)
        
        messages.append({"role": "user", "content": content_parts})

    if stream:
        async def generate_response_stream():
            async for chunk in model.astream(messages):
                yield chunk.content
                await asyncio.sleep(0.001)
        return generate_response_stream()

    response = await model.ainvoke(messages)
    return response.content


def format_chat_history(
    history: Union[str, List[Dict[str, str]]],
    system_prompt: str,
    context: Optional[str] = None
) -> List[Dict[str, str]]:
    """Format chat history with system prompt and context."""
    if isinstance(history, str):
        try:
            history = json.loads(history)
            if not isinstance(history, list):
                logger.warning(f"Loaded history from JSON string is not a list: {history}. Treating as empty.")
                history = []
        except json.JSONDecodeError:
            logger.error(f"Failed to parse history string as JSON: {history}. Treating as empty.")
            history = []
    
    messages: List[Dict[str, str]] = []
    
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    if context:
        messages.append({"role": "system", "content": f"Context: {context}"})
    
    valid_history_messages = []
    if isinstance(history, list):
        for msg in history:
            if isinstance(msg, dict) and "role" in msg and "content" in msg:
                valid_history_messages.append(msg)
            else:
                logger.warning(f"Skipping invalid history message (must be dict with 'role' and 'content'): {msg}")
    messages.extend(valid_history_messages)
    return messages
