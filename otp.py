from fastapi import APIRouter, HTTPException, Response, status, Depends, BackgroundTasks
from fastapi.responses import JSONResponse
import logging
from pydantic import EmailStr
from otp_manager import OTPManager  
from user_manager import UserManager
from schemas_models import OTPRequest, OTPVerification, OTPDeletion, OTPSendEmail, EmailRequest
from typing import Dict, Any

# Setup logging
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ])
logger = logging.getLogger(__name__)
router = APIRouter(prefix="/otp", tags=["otp"])

otp_manager = OTPManager()
user_manager = UserManager()

def create_response(success: bool, message: str, data: Dict[str, Any] = None) -> Dict[str, Any]:
    """Helper function to create consistent response format"""
    response = {
        "success": success,
        "message": message
    }
    if data:
        response["data"] = data
    return response

@router.post("/generate-otp")
async def generate_otp(request: OTPRequest):
    """Generate an OTP for the given email."""
    try:

        # Check if email exists in user database
        user_data = await user_manager.get_user_by_email(request.email)
        if not user_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=create_response(success=False, message="Email not registered")
            )
            # return create_response(
            #     success=False,
            #     message="Email not registered",
            #     status_code=status.HTTP_404_NOT_FOUND
            # )

        # Generate OTP
        result = await otp_manager.generate_otp(request.email, request.length)

        # Check if result is a tuple (error occurred)
        if isinstance(result, tuple) and not result[0]:
            # Assuming result is (False, error_message)
            error_message = result[1]
            status_code = status.HTTP_429_TOO_MANY_REQUESTS if "frequent" in error_message.lower() else status.HTTP_500_INTERNAL_SERVER_ERROR
            raise HTTPException(
                status_code=status_code,
                detail=create_response(success=False, message=error_message)
            )
            # return create_response(
            #     success=False,
            #     message=result[1],
            #     status_code=status.HTTP_429_TOO_MANY_REQUESTS
            # )

        otp = result  # If not a tuple, result is the OTP

        return create_response(
            success=True,
            message="OTP generated successfully (but not sent)",
            data={"email": request.email, "otp": otp}
        )
    
    except HTTPException as http_exc:
        # Re-raise HTTPException to let FastAPI handle it
        raise http_exc
    except Exception as e:
        logger.error(f"Error in generate_otp endpoint for {request.email}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=create_response_body(success=False, message="Failed to generate OTP due to an internal error")
        )
        # return create_response(
        #     success=False,
        #     message="Failed to generate OTP",
        #     status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        # )

@router.post("/send-otp")
async def send_otp(request: OTPRequest, background_tasks: BackgroundTasks):
    """Generate and send an OTP to the specified email."""
    try:
        email = request.email
        length = request.length

        # Find user by email
        user_data  = await user_manager.get_user_by_email(email)
        
        if not user_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=create_response(success=False, message="Email not registered")
            )
            # return create_response(
            #     success=False, 
            #     message="Email not found",
            #     status_code=status.HTTP_404_NOT_FOUND
            # )

        # Generate OTP
        result  = await otp_manager.generate_otp(email, length)
        
        # Check if result is a tuple (error occurred)
        if isinstance(result, tuple) and not result[0]:
            error_message = result[1]
            status_code = status.HTTP_429_TOO_MANY_REQUESTS if "frequent" in error_message.lower() else status.HTTP_500_INTERNAL_SERVER_ERROR
            raise HTTPException(
                status_code=status_code,
                detail=create_response(success=False, message=error_message)
            )
            # return create_response(
            #     success=False,
            #     message=result[1],
            #     status_code=status.HTTP_429_TOO_MANY_REQUESTS
            # )
        
        otp = result  # If not a tuple, result is the OTP

        # Send OTP email as a background task to improve response time
        background_tasks.add_task(otp_manager.send_otp_email, email, otp)
        logger.info(f"Background task added to send OTP to {email}")

        return create_response(
            success=True,
            message="OTP sent successfully initiated and email sending scheduled."
        )

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Error in send_otp endpoint for {request.email}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=create_response(success=False, message="Failed to send OTP due to an internal error")
        )
        # return create_response(
        #     success=False,
        #     message="Internal server error", 
        #     status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        # )
    
@router.post("/verify-otp")
async def verify_otp(request: OTPVerification):
    """Verify an OTP for the given email."""
    try:
        is_valid = await otp_manager.verify_otp(request.email, request.otp, request.expiration)
        
        if not is_valid:
            # OTPManager handles logging details internally (expired vs invalid)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=create_response(success=False, message="Invalid or expired OTP")
            )
            # return create_response(
            #     success=False,
            #     message="Invalid or expired OTP",
            #     status_code=status.HTTP_400_BAD_REQUEST
            # )

        return create_response(
            success=True,
            message="OTP verified successfully"
        )
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Error in verify_otp endpoint for {request.email}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=create_response(success=False, message="Failed to verify OTP due to an internal error")
        )

        # return create_response(
        #     success=False,
        #     message="Failed to verify OTP",
        #     status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        # )

@router.delete("/delete-otp")
async def delete_otp(request: OTPDeletion):
    """Delete an OTP for the given email."""
    try:
        success = await otp_manager.delete_otp(request.email)
        if not success:
            # This indicates an unexpected error within otp_manager.delete_otp
             logger.error(f"otp_manager.delete_otp failed unexpectedly for {request.email}")
             raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=create_response(success=False, message="Failed to delete OTP due to an internal error")
            )
            # return create_response(
            #     success=False, 
            #     message="Failed to delete OTP",
            #     status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
            # )

        return create_response(
            success=True,
            message=f"OTP for {request.email} deleted if it existed"
        )
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Error in delete_otp endpoint for {request.email}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=create_response(success=False, message="Failed to delete OTP due to an internal error")
        )
        # return create_response(
        #     success=False, 
        #     message="Failed to delete OTP",
        #     status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        # )
