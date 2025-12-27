# app/routes/otp_router.py
from fastapi import APIRouter, HTTPException, status, BackgroundTasks
import logging
from app.schemas import OTPRequest, OTPVerification, OTPDeletion, OTPSendEmail 
from app.services.otp_service import otp_manager
from app.services.user_service import user_manager
from typing import Dict, Any, Optional 

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/otp", tags=["otp"])

def create_response(success: bool, message: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Helper function to create consistent response format"""
    response_dict = {
        "success": success,
        "message": message
    }
    if data:
        response_dict["data"] = data
    return response_dict

@router.post("/generate-otp")
async def generate_otp(request: OTPRequest): # Changed name from generate_otp_endpoint
    """Generate an OTP for the given email."""
    try:
        # Check if email exists in user database
        user_data = await user_manager.get_user_by_email(request.email)
        if not user_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                # Matched message from otp.py
                detail=create_response(success=False, message="Email not registered")
            )

        # Generate OTP
        # OTPManager.generate_otp returns Tuple[bool, str]: (success_status, value_or_error_msg)
        # Use keyword arguments for email and length. `purpose` will take its default value in OTPManager.
        success_status, value_or_error_msg = await otp_manager.generate_otp(
            email=request.email,
            length=request.length  # Assuming OTPRequest has a 'length' attribute
        )

        otp_value: str
        if not success_status:
            # If generation failed, value_or_error_msg is the error message string
            error_message = value_or_error_msg
            status_code = status.HTTP_429_TOO_MANY_REQUESTS if "frequent" in error_message.lower() else status.HTTP_500_INTERNAL_SERVER_ERROR
            raise HTTPException(
                status_code=status_code,
                detail=create_response(success=False, message=error_message)
            )
        else:
            # If generation succeeded, value_or_error_msg is the OTP string
            otp_value = value_or_error_msg

        return create_response(
            success=True,
            # Matched message and data structure from otp.py
            message="OTP generated successfully (but not sent)",
            data={"email": request.email, "otp": otp_value}
        )

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        # Matched logging style from otp.py (removed purpose)
        logger.error(f"Error in generate_otp endpoint for {request.email}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # Matched message from otp.py
            detail=create_response(success=False, message="Failed to generate OTP due to an internal error")
        )

@router.post("/send-otp")
async def send_otp(request: OTPRequest, background_tasks: BackgroundTasks): # Changed name from send_otp_endpoint
    """Generate and send an OTP to the specified email."""
    try:
        email = request.email
        length = request.length # Assuming OTPRequest has email and length

        user_data = await user_manager.get_user_by_email(email)
        if not user_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                # Matched message from otp.py
                detail=create_response(success=False, message="Email not registered")
            )

        # Generate OTP
        success_status, value_or_error_msg = await otp_manager.generate_otp(
            email=email,
            length=length
        )

        otp_value: str
        if not success_status:
            # If generation failed, value_or_error_msg is the error message string
            error_message = value_or_error_msg
            status_code = status.HTTP_429_TOO_MANY_REQUESTS if "frequent" in error_message.lower() else status.HTTP_500_INTERNAL_SERVER_ERROR
            raise HTTPException(
                status_code=status_code,
                detail=create_response(success=False, message=error_message)
            )
        else:
            # If generation succeeded, value_or_error_msg is the OTP string
            otp_value = value_or_error_msg

        # Pass email and otp_value. `purpose` and `expiration` will use defaults in send_otp_email.
        background_tasks.add_task(otp_manager.send_otp_email, email, otp_value)
        logger.info(f"Background task added to send OTP to {email}") # Matched log from otp.py

        return create_response(
            success=True,
            # Matched message from otp.py
            message="OTP sent successfully initiated and email sending scheduled."
        )

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        # Matched logging style from otp.py (removed purpose)
        logger.error(f"Error in send_otp endpoint for {request.email}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # Matched message from otp.py
            detail=create_response(success=False, message="Failed to send OTP due to an internal error")
        )

@router.post("/verify-otp")
async def verify_otp(request: OTPVerification): # Changed name from verify_otp_endpoint
    """Verify an OTP for the given email."""
    try:
        is_valid = await otp_manager.verify_otp(request.email, request.otp)

        if not is_valid:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                # Matched message from otp.py
                detail=create_response(success=False, message="Invalid or expired OTP")
            )

        return create_response(
            success=True,
            # Matched message from otp.py
            message="OTP verified successfully"
        )
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        # Matched logging style from otp.py (removed purpose)
        logger.error(f"Error in verify_otp endpoint for {request.email}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # Matched message from otp.py
            detail=create_response(success=False, message="Failed to verify OTP due to an internal error")
        )

@router.delete("/delete-otp")
async def delete_otp(request: OTPDeletion): # Changed name from delete_otp_endpoint
    """Delete an OTP for the given email."""
    try:
        success = await otp_manager.delete_otp(request.email)
        if not success:
            # Matched logging and error handling from otp.py
            logger.error(f"otp_manager.delete_otp failed unexpectedly for {request.email}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=create_response(success=False, message="Failed to delete OTP due to an internal error")
            )

        return create_response(
            success=True,
            # Matched message from otp.py (removed purpose)
            message=f"OTP for {request.email} deleted if it existed"
        )
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        # Matched logging style from otp.py (removed purpose)
        logger.error(f"Error in delete_otp endpoint for {request.email}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            # Matched message from otp.py
            detail=create_response(success=False, message="Failed to delete OTP due to an internal error")
        )
