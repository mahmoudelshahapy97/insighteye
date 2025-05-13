from fastapi import HTTPException, Response, Body, Path, status
# import datetime
import json
import psycopg2
from config import config
from langchain_community.chat_models import ChatOpenAI
from langchain_ollama import ChatOllama#OllamaLLM, 
import logging
import asyncio
from typing import List, Dict, Tuple, Union, Optional
import cv2
import base64
import numpy as np
import pandas as pd
import xgboost as xgb
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from schemas_models import TokenRequest, ValidateSessionRequest, GetUsernameFromSessionRequest, TokenRequest
import time
from datetime import datetime, timedelta, time

# Setup logging
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ])
logger = logging.getLogger(__name__)

_models = {}

def handle_exceptions(func):
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except HTTPException as http_exc:
            raise http_exc
        except Exception as e:
            logging.error(f"{func.__name__} error: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"An unexpected error occurred: {e}"
            )
    return wrapper

def datetime_to_iso(dt: datetime) -> str:
    """Convert datetime to ISO format string."""
    return dt.isoformat()

def iso_to_datetime(iso_str: str) -> datetime:
    """Convert ISO format string to datetime with timezone handling."""
    return datetime.fromisoformat(iso_str.replace('Z', '+00:00'))

def timestamp_to_iso(timestamp: int) -> str:
    """Convert Unix timestamp to ISO format string."""
    return datetime.fromtimestamp(timestamp).isoformat()

def iso_to_timestamp(iso_str: str) -> int:
    """Convert ISO format string to Unix timestamp."""
    return int(iso_to_datetime(iso_str).timestamp())

def parse_camera_ids(camera_id: Optional[Union[str, List[str]]]) -> List[str]:
    """
    Parse and validate camera_id from query parameters.
    Returns a list of cleaned camera IDs.
    """
    print(f"Parsed camera IDs: input='{camera_id}'")
    if not camera_id:
        return []

    camera_ids = []
    
    # Handle string input
    if isinstance(camera_id, str):
        # First, check if the string already has commas - direct URL parameter format
        if ',' in camera_id and not (camera_id.strip().startswith('[') and camera_id.strip().endswith(']')):
            # Direct comma-separated string (common in URL parameters)
            camera_ids = [cam_id.strip() for cam_id in camera_id.split(',') if cam_id.strip()]
            print(f"Parsed direct comma-separated format: {camera_ids}")
        else:
            # Remove the surrounding single quotes if they exist
            camera_id = camera_id.strip("'")
            
            # Try to parse as JSON array
            if camera_id.strip().startswith('[') and camera_id.strip().endswith(']'):
                try:
                    # Try JSON parsing first
                    try:
                        parsed_ids = json.loads(camera_id)
                        if isinstance(parsed_ids, list):
                            camera_ids = parsed_ids
                            print(f"Parsed JSON array format: {camera_ids}")
                        else:
                            # Not a list, fallback to manual parsing
                            cleaned_str = camera_id.strip()[1:-1]
                            camera_ids = [id.strip() for id in cleaned_str.split(',') if id.strip()]
                            print(f"Parsed bracketed non-JSON format: {camera_ids}")
                    except json.JSONDecodeError:
                        # If JSON parsing fails, extract content between brackets and split by comma
                        cleaned_str = camera_id.strip()[1:-1]
                        camera_ids = [id.strip() for id in cleaned_str.split(',') if id.strip()]
                        print(f"Parsed bracketed format after JSON failure: {camera_ids}")
                except Exception as e:
                    logger.error(f"Error parsing camera IDs: {e}")
                    # Fallback to comma-separated
                    camera_ids = [cam_id.strip() for cam_id in camera_id.split(',') if cam_id.strip()]
                    print(f"Fallback parsing after exception: {camera_ids}")
            else:
                # Simple comma-separated string
                camera_ids = [cam_id.strip() for cam_id in camera_id.split(',') if cam_id.strip()]
                print(f"Parsed as simple string: {camera_ids}")
    
    # Handle list/array input
    elif isinstance(camera_id, (list, tuple)):
        camera_ids = camera_id
        print(f"Used direct list input: {camera_ids}")
    
    # Filter out empty strings and ensure each ID is really a string
    camera_ids = [str(cam_id).strip().strip("'\"") for cam_id in camera_ids if cam_id]
    
    # Debug info
    logger.debug(f"Parsed camera IDs: input='{camera_id}', output={camera_ids}")
    print(f"Parsed camera IDs: final result={camera_ids}")
    
    return camera_ids

def parse_date_format(date_str: str) -> datetime.date:
    """
    Try multiple date formats to parse the input string.
    Returns a datetime.date object if successful, raises ValueError if all formats fail.
    """
    formats = [
        "%Y-%m-%d",    # Standard ISO format: 2025-02-23
        "%d-%m-%Y",    # European format: 23-02-2025
        "%m-%d-%Y",    # US format: 02-23-2025
        "%d/%m/%Y",    # European with slashes: 23/02/2025
        "%m/%d/%Y",    # US with slashes: 02/23/2025
        "%Y/%m/%d"     # ISO with slashes: 2025/02/23
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    
    # If all formats fail, try a more lenient approach for partial dates
    try:
        # Handle year only (assume January 1st)
        if len(date_str) == 4 and date_str.isdigit():
            return datetime.date(int(date_str), 1, 1)
    except ValueError:
        pass
        
    # If all attempts fail, raise an error
    raise ValueError(f"Could not parse date string: {date_str}")

def parse_iso_date(date_str: str) -> str:
    """Ensures the date is in YYYY-MM-DD format before parsing."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()  # Valid format
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {date_str}. Expected YYYY-MM-DD.")

def parse_time_string(time_str: Optional[str], default_time: time) -> time:
    """Parses HH:MM or HH:MM:SS format, returns default on failure or None input."""
    if time_str is None:
        return default_time
    try:
        parts = list(map(int, time_str.split(':')))
        hour = parts[0]
        minute = parts[1] if len(parts) > 1 else 0
        second = parts[2] if len(parts) > 2 else 0
        # Basic validation
        if 0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59:
            return time(hour, minute, second)
        else:
            logger.warning(f"Invalid time component value in '{time_str}'. Using default: {default_time}")
            return default_time
    except (ValueError, IndexError) as e:
        logger.warning(f"Could not parse time string '{time_str}': {e}. Using default: {default_time}")
        return default_time

def paginate_list(data, per_page):
    return [data[i : i + per_page] for i in range(0, len(data), per_page)]

def paginate_list1(data, page, per_page):
    return [data[i] for i in range((page - 1) * per_page, min(page * per_page, len(data)))]


def make_prediction_defalut(camera_id, data):
    """
    Placeholder function to simulate making a prediction.
    """
    # Here you would implement the machine learning model
    #   and do prediction with the existing data for this camera_id.
    if data and len(data) > 0:
        total_count = sum([item["metadata"]["person_count"] for item in data])
        average_count = total_count / len(data)
    else:
        average_count = 100
    
    return {
        "next_hour": round(average_count * 1.2),
        "next_day": round(average_count * 1.1),
        "next_week": round(average_count * 1.05)
        }

# def make_prediction(camera_id, data):
#     """
#     Makes predictions using an XGBoost model based on historical data.
#     Returns predictions for next hour, day, and week.
#     """
#     if not data or len(data) == 0:
#         # Return default predictions if no data is available
#         return {
#             "next_hour": 0,
#             "next_day": 0,
#             "next_week": 0
#         }
    
#     # Convert the data to a DataFrame for easier processing
#     records = []
#     for item in data:
#         timestamp = item["metadata"]["timestamp"]
#         person_count = item["metadata"]["person_count"]
#         records.append({
#             "timestamp": timestamp,
#             "person_count": person_count,
#             "camera_id": item["metadata"]["camera_id"]
#         })
    
#     df = pd.DataFrame(records)
    
#     # Convert timestamp to datetime and extract features
#     df["datetime"] = pd.to_datetime(df["timestamp"], unit='s')
#     df["hour"] = df["datetime"].dt.hour
#     df["day_of_week"] = df["datetime"].dt.dayofweek
#     df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)
    
#     # Calculate rolling statistics as features
#     df = df.sort_values("timestamp")
#     df["rolling_mean_3h"] = df["person_count"].rolling(window=3, min_periods=1).mean()
#     df["rolling_mean_24h"] = df["person_count"].rolling(window=24, min_periods=1).mean()
    
#     # Load the pre-trained models
#     model_path = f"models/{camera_id}"
#     try:
#         hourly_model = xgb.Booster()
#         hourly_model.load_model(f"{model_path}_hourly.model")
        
#         daily_model = xgb.Booster()
#         daily_model.load_model(f"{model_path}_daily.model")
        
#         weekly_model = xgb.Booster()
#         weekly_model.load_model(f"{model_path}_weekly.model")
#     except Exception as e:
#         logging.error(f"Failed to load XGBoost models for camera {camera_id}: {e}")
#         # Fallback to a simple average-based prediction if models aren't available
#         total_count = sum([item["metadata"]["person_count"] for item in data])
#         average_count = total_count / len(data)
#         return {
#             "next_hour": round(average_count * 1.2),
#             "next_day": round(average_count * 1.1),
#             "next_week": round(average_count * 1.05)
#         }
    
#     # Prepare features for prediction
#     # Get the most recent data point
#     latest_data = df.iloc[-1]
    
#     # Create feature array for prediction
#     features = np.array([
#         latest_data["hour"],
#         latest_data["day_of_week"],
#         latest_data["is_weekend"],
#         latest_data["rolling_mean_3h"],
#         latest_data["rolling_mean_24h"],
#         latest_data["person_count"]
#     ]).reshape(1, -1)
    
#     # Convert to DMatrix for XGBoost
#     dmatrix = xgb.DMatrix(features)
    
#     # Make predictions
#     next_hour_prediction = round(float(hourly_model.predict(dmatrix)[0]))
#     next_day_prediction = round(float(daily_model.predict(dmatrix)[0]))
#     next_week_prediction = round(float(weekly_model.predict(dmatrix)[0]))
    
#     # Ensure predictions are non-negative
#     next_hour_prediction = max(0, next_hour_prediction)
#     next_day_prediction = max(0, next_day_prediction)
#     next_week_prediction = max(0, next_week_prediction)
    
#     return {
#         "next_hour": next_hour_prediction,
#         "next_day": next_day_prediction,
#         "next_week": next_week_prediction
#     }

def make_prediction_all_cameras(camera_id, data):
    """
    Makes predictions using an XGBoost model based on historical data.
    Returns predictions for next hour, day, and week.
    
    For individual cameras, it uses camera-specific models.
    For "all" camera_id, it combines data from all cameras.
    """
    if not data or len(data) == 0:
        # Return default predictions if no data is available
        return {
            "next_hour": 0,
            "next_day": 0,
            "next_week": 0
        }
    
    # Convert the data to a DataFrame for easier processing
    records = []
    for item in data:
        timestamp = item["metadata"]["timestamp"]
        person_count = item["metadata"]["person_count"]
        cam_id = item["metadata"]["camera_id"]
        records.append({
            "timestamp": timestamp,
            "person_count": person_count,
            "camera_id": cam_id
        })
    
    df = pd.DataFrame(records)
    
    # Convert timestamp to datetime and extract features
    df["datetime"] = pd.to_datetime(df["timestamp"], unit='s')
    df["hour"] = df["datetime"].dt.hour
    df["day_of_week"] = df["datetime"].dt.dayofweek
    df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)
    
    # Sort by timestamp for proper time series processing
    df = df.sort_values("timestamp")
    
    # Check if we're making a prediction for "all" cameras
    if camera_id.lower() == "all":
        # Group by timestamp and sum person counts across all cameras
        grouped_df = df.groupby("timestamp").agg({
            "person_count": "sum", 
            "hour": "first", 
            "day_of_week": "first", 
            "is_weekend": "first",
            "datetime": "first"
        }).reset_index()
        
        # Calculate rolling statistics as features
        grouped_df["rolling_mean_3h"] = grouped_df["person_count"].rolling(window=3, min_periods=1).mean()
        grouped_df["rolling_mean_24h"] = grouped_df["person_count"].rolling(window=24, min_periods=1).mean()
        
        # Load the "all cameras" model
        model_path = "models/all_cameras"
        try:
            hourly_model = xgb.Booster()
            hourly_model.load_model(f"{model_path}_hourly.model")
            
            daily_model = xgb.Booster()
            daily_model.load_model(f"{model_path}_daily.model")
            
            weekly_model = xgb.Booster()
            weekly_model.load_model(f"{model_path}_weekly.model")
        except Exception as e:
            logging.error(f"Failed to load XGBoost models for all cameras: {e}")
            # Fallback to a simple average-based prediction if models aren't available
            total_count = df["person_count"].sum()
            average_count = total_count / len(df)
            return {
                "next_hour": round(average_count * 1.2),
                "next_day": round(average_count * 1.1),
                "next_week": round(average_count * 1.05)
            }
        
        # Get the most recent aggregated data point
        latest_data = grouped_df.iloc[-1]
        
    else:
        # For individual cameras, calculate camera-specific rolling statistics
        df_camera = df[df["camera_id"] == camera_id]
        df_camera["rolling_mean_3h"] = df_camera["person_count"].rolling(window=3, min_periods=1).mean()
        df_camera["rolling_mean_24h"] = df_camera["person_count"].rolling(window=24, min_periods=1).mean()
        
        # Load the camera-specific models
        model_path = f"models/{camera_id}"
        try:
            hourly_model = xgb.Booster()
            hourly_model.load_model(f"{model_path}_hourly.model")
            
            daily_model = xgb.Booster()
            daily_model.load_model(f"{model_path}_daily.model")
            
            weekly_model = xgb.Booster()
            weekly_model.load_model(f"{model_path}_weekly.model")
        except Exception as e:
            logging.error(f"Failed to load XGBoost models for camera {camera_id}: {e}")
            # Fallback to a simple average-based prediction if models aren't available
            total_count = df_camera["person_count"].sum()
            average_count = total_count / len(df_camera) if len(df_camera) > 0 else 0
            return {
                "next_hour": round(average_count * 1.2),
                "next_day": round(average_count * 1.1),
                "next_week": round(average_count * 1.05)
            }
        
        # Get the most recent data point for this camera
        if len(df_camera) > 0:
            latest_data = df_camera.iloc[-1]
        else:
            # No data for this specific camera
            return {
                "next_hour": 0,
                "next_day": 0,
                "next_week": 0
            }
    
    # Create feature array for prediction
    features = np.array([
        latest_data["hour"],
        latest_data["day_of_week"],
        latest_data["is_weekend"],
        latest_data["rolling_mean_3h"],
        latest_data["rolling_mean_24h"],
        latest_data["person_count"]
    ]).reshape(1, -1)
    
    # Convert to DMatrix for XGBoost
    dmatrix = xgb.DMatrix(features)
    
    # Make predictions
    next_hour_prediction = round(float(hourly_model.predict(dmatrix)[0]))
    next_day_prediction = round(float(daily_model.predict(dmatrix)[0]))
    next_week_prediction = round(float(weekly_model.predict(dmatrix)[0]))
    
    # Ensure predictions are non-negative
    next_hour_prediction = max(0, next_hour_prediction)
    next_day_prediction = max(0, next_day_prediction)
    next_week_prediction = max(0, next_week_prediction)
    
    return {
        "next_hour": next_hour_prediction,
        "next_day": next_day_prediction,
        "next_week": next_week_prediction
    }

def make_prediction(camera_id, data):
    """
    Makes predictions using an XGBoost model based on historical data.
    Returns predictions for next hour, day, and week.
    Falls back to average-based prediction if models aren't available.
    """
    if not data or len(data) == 0:
        logging.info(f"No data available for camera {camera_id}. Returning zero predictions.")
        return {
            "next_hour": 0,
            "next_day": 0,
            "next_week": 0
        }
    
    # Convert the data to a DataFrame for easier processing
    records = []
    for item in data:
        try:
            timestamp = item["metadata"]["timestamp"]
            person_count = item["metadata"]["person_count"]
            records.append({
                "timestamp": timestamp,
                "person_count": person_count,
                "camera_id": item["metadata"]["camera_id"]
            })
        except KeyError as e:
            logging.warning(f"Skipping record with missing metadata field: {e}")
            continue
    
    # If we couldn't extract any valid records, return zeros
    if not records:
        logging.warning(f"No valid records found for camera {camera_id}. Returning zero predictions.")
        return {
            "next_hour": 0,
            "next_day": 0,
            "next_week": 0
        }
    
    df = pd.DataFrame(records)
    
    # Get model paths
    model_path = f"models/{camera_id}"
    hourly_model_path = f"{model_path}_hourly.model"
    daily_model_path = f"{model_path}_daily.model"
    weekly_model_path = f"{model_path}_weekly.model"
    
    # Check if model files exist before trying to load them
    import os
    models_exist = (
        os.path.exists(hourly_model_path) and 
        os.path.exists(daily_model_path) and 
        os.path.exists(weekly_model_path)
    )
    
    if not models_exist:
        logging.info(f"XGBoost models not found for camera {camera_id}. Using fallback prediction method.")
        return use_fallback_prediction(df)
    
    try:
        # Try to load models if they exist
        hourly_model = xgb.Booster()
        hourly_model.load_model(hourly_model_path)
        
        daily_model = xgb.Booster()
        daily_model.load_model(daily_model_path)
        
        weekly_model = xgb.Booster()
        weekly_model.load_model(weekly_model_path)
        
        # Convert timestamp to datetime and extract features
        df["datetime"] = pd.to_datetime(df["timestamp"], unit='s')
        df["hour"] = df["datetime"].dt.hour
        df["day_of_week"] = df["datetime"].dt.dayofweek
        df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)
        
        # Calculate rolling statistics as features
        df = df.sort_values("timestamp")
        df["rolling_mean_3h"] = df["person_count"].rolling(window=3, min_periods=1).mean()
        df["rolling_mean_24h"] = df["person_count"].rolling(window=24, min_periods=1).mean()
        
        # Get the most recent data point
        latest_data = df.iloc[-1]
        
        # Create feature array for prediction
        features = np.array([
            latest_data["hour"],
            latest_data["day_of_week"],
            latest_data["is_weekend"],
            latest_data["rolling_mean_3h"],
            latest_data["rolling_mean_24h"],
            latest_data["person_count"]
        ]).reshape(1, -1)
        
        # Convert to DMatrix for XGBoost
        dmatrix = xgb.DMatrix(features)
        
        # Make predictions
        next_hour_prediction = round(float(hourly_model.predict(dmatrix)[0]))
        next_day_prediction = round(float(daily_model.predict(dmatrix)[0]))
        next_week_prediction = round(float(weekly_model.predict(dmatrix)[0]))
        
        # Ensure predictions are non-negative
        next_hour_prediction = max(0, next_hour_prediction)
        next_day_prediction = max(0, next_day_prediction)
        next_week_prediction = max(0, next_week_prediction)
        
        logging.info(f"Successfully used XGBoost models for camera {camera_id}")
        return {
            "next_hour": next_hour_prediction,
            "next_day": next_day_prediction,
            "next_week": next_week_prediction
        }
    
    except Exception as e:
        logging.error(f"Error using XGBoost models for camera {camera_id}: {e}")
        return use_fallback_prediction(df)

def use_fallback_prediction(df):
    """
    Fallback prediction method based on simple averages.
    """
    try:
        # Basic statistics-based prediction
        # Sort by timestamp to ensure we're working with time-ordered data
        df = df.sort_values("timestamp")
        
        # Calculate mean person count
        total_count = df["person_count"].sum()
        avg_count = total_count / len(df)
        
        # Try to determine if there's a trend by comparing recent vs older data
        if len(df) >= 4:
            half_point = len(df) // 2
            older_half = df.iloc[:half_point]
            newer_half = df.iloc[half_point:]
            older_avg = older_half["person_count"].mean()
            newer_avg = newer_half["person_count"].mean()
            
            # Calculate trend factor (how much recent data differs from older data)
            trend_factor = newer_avg / older_avg if older_avg > 0 else 1.1
            
            # Limit trend factor to reasonable range
            trend_factor = max(0.8, min(trend_factor, 1.3))
            
            # Get most recent count for short-term prediction
            recent_count = df.iloc[-1]["person_count"]
            
            return {
                "next_hour": round(recent_count * 1.05),  # More reliant on very recent data
                "next_day": round(newer_avg * trend_factor),  # Use trend for medium-term
                "next_week": round(avg_count * trend_factor)  # Use overall average for long-term
            }
        else:
            # Not enough data points for trend analysis
            return {
                "next_hour": round(avg_count * 1.1),
                "next_day": round(avg_count * 1.05),
                "next_week": round(avg_count)
            }
    except Exception as e:
        logging.error(f"Error in fallback prediction: {e}")
        # Ultimate fallback if everything else fails
        if df is not None and len(df) > 0:
            try:
                # Just use the mean if we can
                avg = df["person_count"].mean()
                return {
                    "next_hour": round(avg),
                    "next_day": round(avg),
                    "next_week": round(avg)
                }
            except:
                pass
        
        # Return conservative default values if all else fails
        return {
            "next_hour": 0,
            "next_day": 0,
            "next_week": 0
        }

def datetime_filter(timestamp, format="%Y-%m-%d %H:%M:%S"):
    if timestamp:
        return datetime.fromtimestamp(timestamp).strftime(format)
    return ""
    
async def send_email(email, subject: str, body: str):
    """Sends to the specified email address."""
    try:
        sender_email = config.get("otp_sender_email", "your-email@gmail.com")
        sender_password = config.get("smtp_password", "your-app-password")
        smtp_server = config.get("smtp_server")
        smtp_port = config.get("smtp_port")

        if not sender_email or not sender_password:
            logger.error("SMTP credentials not configured")
            return False

        message = MIMEMultipart("alternative")
        message["Subject"] = subject
        message["From"] =  sender_email
        message["To"] = email
        
        # Create the plain-text and HTML version of your message
        text_part = MIMEText(body, "plain")
        
        # HTML version of the message
        html_body = f"""
            <html>
                <body>
                <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px; border: 1px solid #ddd; border-radius: 5px;">
                    <h2 style="color: #333; text-align: center;">One-Time Password</h2>
                    <p>Your OTP code is:</p>
                    <div style="text-align: center; padding: 10px; background-color: #f5f5f5; font-size: 24px; font-weight: bold; letter-spacing: 5px; margin: 20px 0;">
                    body.strip().split(':')[1].strip().split('\n')[0]
                    </div>
                    <p>This code will expire in 10 minutes.</p>
                    <p>If you did not request this code, please ignore this email.</p>
                    <hr style="border: none; border-top: 1px solid #ddd; margin: 20px 0;">
                    <p style="font-size: 12px; color: #777; text-align: center;">This is an automated message, please do not reply to this email.</p>
                </div>
                </body>
            </html>
            """

        # html_part = MIMEText(html_body, "html")
        
        # Add HTML/plain-text parts to MIMEMultipart message
        message.attach(text_part)
        # message.attach(html_part)
        
        # Create secure connection and send email
        context = ssl.create_default_context()

        try:
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.ehlo()
                server.starttls(context=context)
                server.ehlo()
                server.login(sender_email, sender_password)
                server.sendmail(sender_email, email, message.as_string())
                logging.info(f"email sent successfully to {email}")
            return True
        except Exception as e:
            logging.error(f"Error sending email: {e}")
            return False
            
    except Exception as e:
        logger.error(f"Error in send_email: {str(e)}")
        return False

async def send_email_from_client(email, subject, body):
    """Sends to the specified email address."""

    sender_email = config.get("otp_sender_email", "your-email@gmail.com")
    sender_password = config.get("smtp_password", "your-app-password")

    message = MIMEMultipart()
    message["From"] =  sender_email
    message["To"] = email
    message["Subject"] = subject
    message.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP(config.get("smtp_server"), config.get("smtp_port")) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(email, sender_email, message.as_string())
            logging.info(f"email sent successfully to {email}")
        return True
    except Exception as e:
        logging.error(f"Error sending email: {e}")
        return False


def frame_to_base64(frame):
    _, buffer = cv2.imencode('.jpg', frame)
    return base64.b64encode(buffer).decode('utf-8')

def base64_to_frame(base64_string):
  try:
    img_bytes = base64.b64decode(base64_string)
    nparr = np.frombuffer(img_bytes, np.uint8)
    frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    return frame
  except Exception as e:
     logging.error(f"Error decoding base64: {e}")
     return None

# Database connection
def get_db_connection():
    """Establishes and returns a database connection."""
    try:
        conn = psycopg2.connect(
        dbname=config['database']['dbname'],
        user=config['database']['user'],
        password=config['database']['password'],
        host=config['database']['host'],
        port=config['database']['port']
    )
        return conn

    except psycopg2.OperationalError as e:
        raise HTTPException(status_code=500, detail=f"Database connection error: {e}")

def get_ChatOpenAI_model(model_id, base_url, temperature, num_predict, format_):
    """Get or create Llama model"""
    if model_id not in _models:
        _models[model_id] = ChatOpenAI(
            base_url=base_url,
            model=model_id,
            api_key="na",
            max_tokens=num_predict,
            temperature=temperature,
            top_p=1,
            frequency_penalty=1.1,
        )
    return _models[model_id]

def get_ChatOllama_model(model_id, temperature, num_predict, format_):
    """Get or create LLaVA model"""
    if model_id not in _models:
        _models[model_id] = ChatOllama(
            model=model_id,
            temperature = temperature,
            num_predict = num_predict,
            format=format_ 
        )
    return _models[model_id]

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
    # functions: Optional[List] = None
):
    """Core logic for generating a chat response"""
    if not image:
        
        model_id = config['models']['chat']['llama']
        # base_url = config['models']['chat']['base_url']
        model = get_ChatOllama_model(model_id, temperature, max_tokens, format_)
        # model = get_ChatOpenAI_model(model_id, base_url, temperature, max_tokens, format_)
        logger.info("Model loaded successfully")

        messages = format_chat_history(
            history,
            system_prompt=system_prompt,
            context=context
        )
        messages.append({"role": "human", "content": prompt})
    else:
        model_id = config['models']['chat']['llava']
        model = get_ChatOllama_model(model_id, temperature, max_tokens, format_)
        logger.info("Model loaded successfully")

        messages = format_chat_history(
            history,
            system_prompt=system_prompt,
            context=context
        )
        content_parts = []
        text_part = {"type": "text", "text": prompt}
        image_part = {
            "type": "image_url",
            "image_url": f"data:image/jpeg;base64,{image}",
        }
        
        content_parts.append(image_part)
        content_parts.append(text_part)

        messages.append({"role": "user", "content": content_parts})

    if stream:
        async def generate_response():
            async for chunk in model.astream(messages):
                yield chunk.content
                await asyncio.sleep(0.01)  # Yield periodically to keep the connection alive
        return generate_response()

    response = await model.ainvoke(messages)
    return response.content

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

def get_user_message(messages: List[Dict[str, str]]) -> str:
    """Get user message from messages"""
    return [msg['content'] for msg in messages if msg['role'] == 'human']

def get_assistant_message(messages: List[Dict[str, str]]) -> str:
    """Get assistant message from messages"""
    return [msg['content'] for msg in messages if msg['role'] == 'assistant']

def validate_prompt(prompt: str) -> str:
    """Clean and validate prompt"""
    if not prompt:
        return ""
    # return re.sub(r'[^\w\s,.!?-]', '', prompt).strip()
    # Remove unsafe characters
    prompt = ''.join(char for char in prompt if char.isprintable())
    return prompt.strip()

def validate_text(text: str) -> str:
    """Validate and clean text for TTS"""
    if not text:
        raise ValueError("Text cannot be empty")
    
    # Remove unsafe characters
    text = ''.join(char for char in text if char.isprintable())
    return text.strip()

"""
import yagmail

# Email credentials
sender_email = "your_email@gmail.com"
app_password = "your_app_password"  # Use an App Password

# Initialize Yagmail
yag = yagmail.SMTP(sender_email, app_password)

# Send email
yag.send(
    to="recipient@example.com",
    subject="Test Email",
    contents="Hello, this is a test email from yagmail!"
)
print("Email sent successfully!")
"""