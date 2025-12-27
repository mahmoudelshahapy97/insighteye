# app/schemas/chat_schemas.py
from pydantic import BaseModel, Field
from typing import Optional, List, Dict

class BaseChatRequest(BaseModel):
    history: Optional[List[Dict[str, str]]] = Field(default_factory=list)
    system_prompt: Optional[str] = Field(default="")
    max_tokens: Optional[int] = Field(default=512, ge=1, le=2048)
    temperature: Optional[float] = Field(default=0.7, ge=0.0, le=2.0)
    format_: Optional[str] = Field(default='')
    stream: Optional[bool] = Field(default=False)

class ChatRequest(BaseChatRequest):
    """Basic chat request"""
    id: Optional[str] = Field(default="0")
    context: Optional[str] = Field(default="")
    prompt: str = Field(..., min_length=1)

class ChatResponse(BaseModel):
    """Response model for chat"""
    response: str = Field(..., description="Chat response")
    id: Optional[str] = None
    tokens_used: Optional[int] = None