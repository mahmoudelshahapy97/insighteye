# user.py
from fastapi import APIRouter, HTTPException, status
from user_manager import UserManager
from schemas_models import CreateUserRequest, VerifyPasswordRequest, ResetPasswordRequest, EmailRequest, UserRequest 
import logging

logger = logging.getLogger(__name__) # Use logger

router = APIRouter(tags=["users"])
user_manager = UserManager()  

@router.post("/users", status_code=status.HTTP_201_CREATED)  # Use 201 Created for successful creation
async def create_user_route(request: CreateUserRequest):
    """
    Creates a new user.

    Args:
        username: The username for the new user.
        password: The password for the new user.
        email: The email address for the new user.

    Returns:
        A message indicating success or failure.

    Raises:
        HTTPException: If the username already exists or if a database error occurs.
    """
    try:
        success = await user_manager.create_user(request.username, request.email, request.password)
        if success:
            return {"message": "User created successfully"}
        else:
            raise HTTPException(status_code=409, detail="Username already exists")  # Conflict
    except HTTPException as http_exc:
        print(f"create_user_route: HTTPException caught - Status Code: {http_exc.status_code}") # Add print here
        raise http_exc
    except Exception as e:
        print(f"create_user_route: Unexpected Exception - Raising 500: {e}") # Add print here
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")

@router.post("/users/verify_password")
async def verify_password_route(request: VerifyPasswordRequest):
    """
    Verifies a user's password.

    Args:
        username: The username of the user.
        password: The password to verify.

    Returns:
        A message indicating whether the password is valid.

    Raises:
        HTTPException: If the user is not found or if a database error occurs.
    """
    try:
        is_valid = await user_manager.verify_user_password(request.username, request.password)
        if is_valid:
            return {"message": "Password is valid"}
        else:
            return {"message": "Invalid username or password"}  # Don't reveal which is wrong
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")

@router.post("/users/by_email")
async def get_user_by_email_route(request: EmailRequest):
    """
    Gets a user's username by their email address.

    Args:
        email: The email address of the user.

    Returns:
        The username of the user

    Raises:
        HTTPException: If a database error occurs.
    """
    try:
        data = await user_manager.get_user_by_email(request.email)
        if data:  # FIX: Changed from 'username' to 'data'
            return {"username": data["username"]}
        else:
            raise HTTPException(status_code=404, detail="User not found")
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")

@router.put("/users/reset_password")
async def reset_password_route(request: ResetPasswordRequest):
    """
    Resets a user's password.

    Args:
        email: The email of the user.
        new_password: The new password for the user.

    Returns:
        A message indicating success or failure.

    Raises:
       HTTPException: If the user is not found or if a database error occurs.
    """

    try:
        user_data = await user_manager.get_user_by_email(request.email)
        success = await user_manager.reset_password(user_data["username"], request.new_password)
        if success:
            return {"message": "Password reset successfully"}
        else:
            raise HTTPException(status_code=404, detail="User not found")
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
         raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")

@router.get("/users")
async def get_all_users_route():
    """
    Gets all users.

    Returns:
        A list of all users.

    Raises:
        HTTPException: If a database error occurs.
    """
    try:
        users = await user_manager.get_all_users()
        return users
    except HTTPException as http_exc:
         raise http_exc
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")

@router.delete("/users/user")
async def delete_user_route(request: UserRequest):
    """
    Deletes a user.

    Args:
        request: UserRequest containing username.

    Returns:
        A message indicating success.

    Raises:
        HTTPException: If the user is not found or if a database error occurs.
    """
    try:
        success = await user_manager.delete_user(request.username)
        if success:
            return {"message": f"User '{request.username}' deleted successfully."}
        else:
            raise HTTPException(status_code=404, detail=f"User '{request.username}' not found.")
    except HTTPException as e:  # Catch HTTPExceptions raised by delete_user
        raise e
    except Exception as e: #catch the other exceptions
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")

@router.delete("/users")
async def delete_all_users_route():
    """
    Deletes all users.

    Returns:
        A message indicating how many users were deleted.

    Raises:
        HTTPException: If a database error occurs.
    """
    try:
        count = await user_manager.delete_all_users()
        return {"message": f"Deleted {count} users."}
    except HTTPException as e:
        raise e
    except Exception as e: #catch the other exceptions
         raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")
