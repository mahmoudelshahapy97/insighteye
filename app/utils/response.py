# app/utils/response.py
from typing import Dict, Any, Optional 

def create_response(success: bool, message: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Helper function to create consistent response format"""
    response_dict = {
        "success": success,
        "message": message
    }
    if data:
        response_dict["data"] = data
    return response_dict
