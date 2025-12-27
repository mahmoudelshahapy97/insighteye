# app/utils/prediction_utils.py
"""Prediction utilities using XGBoost models."""

import logging
import os
from typing import List, Dict, Any

import numpy as np
import pandas as pd
import xgboost as xgb

logger = logging.getLogger(__name__)


def make_prediction(camera_id: str, data: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Makes predictions using an XGBoost model based on historical data.
    Returns predictions for next hour, day, and week.
    Falls back to average-based prediction if models aren't available or data is insufficient.
    """
    if not data or len(data) == 0:
        logging.info(f"No data available for camera {camera_id}. Returning zero predictions via fallback.")
        return use_fallback_prediction(pd.DataFrame())
    
    records = []
    for item in data:
        if 'metadata' in item and isinstance(item['metadata'], dict):
            if "timestamp" in item["metadata"] and "person_count" in item["metadata"]:
                records.append(item["metadata"])
            else:
                logging.warning(f"Skipping record for camera {camera_id} due to missing timestamp or person_count in metadata: {item}")
        else:
            logging.warning(f"Skipping record for camera {camera_id} due to missing or invalid metadata: {item}")

    if not records:
        logging.warning(f"No valid records with metadata found for camera {camera_id}. Using fallback.")
        return use_fallback_prediction(pd.DataFrame())
        
    df = pd.DataFrame(records)
    
    if 'person_count' not in df.columns or 'timestamp' not in df.columns:
        logging.warning(f"DataFrame for camera {camera_id} missing 'person_count' or 'timestamp' columns. Using fallback.")
        return use_fallback_prediction(df if 'person_count' in df.columns and 'timestamp' in df.columns else pd.DataFrame())

    model_path_base = f"models/{camera_id}"
    hourly_model_path = f"{model_path_base}_hourly.model"
    daily_model_path = f"{model_path_base}_daily.model"
    weekly_model_path = f"{model_path_base}_weekly.model"

    if not all(os.path.exists(p) for p in [hourly_model_path, daily_model_path, weekly_model_path]):
        logging.info(f"XGBoost models not found for camera {camera_id}. Using fallback prediction method.")
        return use_fallback_prediction(df)

    try:
        hourly_model = xgb.Booster()
        hourly_model.load_model(hourly_model_path)
        daily_model = xgb.Booster()
        daily_model.load_model(daily_model_path)
        weekly_model = xgb.Booster()
        weekly_model.load_model(weekly_model_path)

        df["datetime"] = pd.to_datetime(df["timestamp"], unit='s')
        df["hour"] = df["datetime"].dt.hour
        df["day_of_week"] = df["datetime"].dt.dayofweek
        df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)
        
        df = df.sort_values("timestamp")
        df["rolling_mean_3h"] = df["person_count"].rolling(window=3, min_periods=1).mean().fillna(0)
        df["rolling_mean_24h"] = df["person_count"].rolling(window=24, min_periods=1).mean().fillna(0)
        
        if df.empty:
            logging.warning(f"DataFrame became empty after processing for camera {camera_id}. Using fallback.")
            return use_fallback_prediction(df)

        latest_data = df.iloc[-1]
        
        features_np = np.array([
            latest_data.get("hour", 0), 
            latest_data.get("day_of_week", 0), 
            latest_data.get("is_weekend", 0),
            latest_data.get("rolling_mean_3h", 0), 
            latest_data.get("rolling_mean_24h", 0),
            latest_data.get("person_count", 0)
        ]).reshape(1, -1)
        
        dmatrix = xgb.DMatrix(features_np)
        
        pred_h = max(0, round(float(hourly_model.predict(dmatrix)[0])))
        pred_d = max(0, round(float(daily_model.predict(dmatrix)[0])))
        pred_w = max(0, round(float(weekly_model.predict(dmatrix)[0])))
        
        logging.info(f"Successfully used XGBoost models for camera {camera_id}")
        return {"next_hour": int(pred_h), "next_day": int(pred_d), "next_week": int(pred_w)}
    
    except Exception as e:
        logger.error(f"Error using XGBoost models for camera {camera_id}: {e}", exc_info=True)
        return use_fallback_prediction(df)


def use_fallback_prediction(df: pd.DataFrame) -> Dict[str, int]:
    """Fallback prediction method based on simple averages. Returns integers."""
    if df.empty or 'person_count' not in df.columns:
        logging.info("Fallback prediction: DataFrame is empty or missing 'person_count'. Returning zeros.")
        return {"next_hour": 0, "next_day": 0, "next_week": 0}
    
    df['person_count'] = pd.to_numeric(df['person_count'], errors='coerce').fillna(0)
    
    avg_count = df["person_count"].mean()
    if pd.isna(avg_count): 
        avg_count = 0.0

    if len(df) >= 4 and 'timestamp' in df.columns:
        df_sorted = df.sort_values("timestamp").reset_index(drop=True)
        half_point = len(df_sorted) // 2
        
        older_half = df_sorted.iloc[:half_point]
        newer_half = df_sorted.iloc[half_point:]

        older_avg = older_half["person_count"].mean() if not older_half.empty else avg_count
        newer_avg = newer_half["person_count"].mean() if not newer_half.empty else avg_count
        
        if pd.isna(newer_avg): 
            newer_avg = avg_count
        if pd.isna(older_avg) or older_avg == 0: 
            older_avg = avg_count if avg_count > 0 else 1.0
        
        trend_factor = newer_avg / older_avg if older_avg != 0 else 1.0
        trend_factor = max(0.8, min(trend_factor, 1.3))
        
        recent_count = df_sorted.iloc[-1]["person_count"] if not df_sorted.empty else avg_count
        if pd.isna(recent_count): 
            recent_count = avg_count

        return {
            "next_hour": round(max(0, recent_count * 1.05)),
            "next_day": round(max(0, newer_avg * trend_factor)),
            "next_week": round(max(0, avg_count * trend_factor))
        }
    else:
        logging.info("Fallback prediction: Not enough data for trend analysis or timestamp missing. Using simple averages.")
        return {
            "next_hour": round(max(0, avg_count * 1.1)),
            "next_day": round(max(0, avg_count * 1.05)),
            "next_week": round(max(0, avg_count))
        }


def make_prediction_all_cameras(camera_id: str, data: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Makes predictions using an XGBoost model based on historical data.
    For "all" camera_id, it combines data from all cameras.
    """
    if not data or len(data) == 0:
        return {"next_hour": 0, "next_day": 0, "next_week": 0}
    
    records = []
    for item in data:
        if 'metadata' in item and isinstance(item['metadata'], dict):
            if all(k in item['metadata'] for k in ["timestamp", "person_count", "camera_id"]):
                records.append({
                    "timestamp": item["metadata"]["timestamp"],
                    "person_count": item["metadata"]["person_count"],
                    "camera_id": item["metadata"]["camera_id"]
                })
    
    if not records:
        return {"next_hour": 0, "next_day": 0, "next_week": 0}

    df = pd.DataFrame(records)
    df['person_count'] = pd.to_numeric(df['person_count'], errors='coerce').fillna(0)
    
    df["datetime"] = pd.to_datetime(df["timestamp"], unit='s')
    df["hour"] = df["datetime"].dt.hour
    df["day_of_week"] = df["datetime"].dt.dayofweek
    df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)
    df = df.sort_values("timestamp")

    latest_data_source_df = df
    model_id_for_path = camera_id

    if camera_id.lower() == "all":
        model_id_for_path = "all_cameras"
        grouped_df = df.groupby("timestamp", as_index=False).agg(
            person_count=("person_count", "sum"),
            hour=("hour", "first"),
            day_of_week=("day_of_week", "first"),
            is_weekend=("is_weekend", "first")
        )
        if grouped_df.empty:
            logging.warning("Grouped DataFrame for 'all_cameras' is empty. Using fallback averages.")
            avg_count = df["person_count"].mean() if not df.empty else 0
            return {
                "next_hour": round(avg_count * 1.2), 
                "next_day": round(avg_count * 1.1), 
                "next_week": round(avg_count * 1.05)
            }

        grouped_df["rolling_mean_3h"] = grouped_df["person_count"].rolling(window=3, min_periods=1).mean().fillna(0)
        grouped_df["rolling_mean_24h"] = grouped_df["person_count"].rolling(window=24, min_periods=1).mean().fillna(0)
        latest_data_source_df = grouped_df
    else:
        df_camera = df[df["camera_id"] == camera_id].copy()
        if df_camera.empty:
            return {"next_hour": 0, "next_day": 0, "next_week": 0}
        
        df_camera["rolling_mean_3h"] = df_camera["person_count"].rolling(window=3, min_periods=1).mean().fillna(0)
        df_camera["rolling_mean_24h"] = df_camera["person_count"].rolling(window=24, min_periods=1).mean().fillna(0)
        latest_data_source_df = df_camera

    model_path = f"models/{model_id_for_path}"
    try:
        if not (os.path.exists(f"{model_path}_hourly.model") and \
                os.path.exists(f"{model_path}_daily.model") and \
                os.path.exists(f"{model_path}_weekly.model")):
            raise FileNotFoundError("One or more model files not found.")
            
        hourly_model = xgb.Booster()
        hourly_model.load_model(f"{model_path}_hourly.model")
        daily_model = xgb.Booster()
        daily_model.load_model(f"{model_path}_daily.model")
        weekly_model = xgb.Booster()
        weekly_model.load_model(f"{model_path}_weekly.model")
    except (FileNotFoundError, xgb.core.XGBoostError) as e:
        logging.error(f"Failed to load XGBoost models for '{model_id_for_path}': {e}")
        avg_val_df = latest_data_source_df if not latest_data_source_df.empty else df
        average_count = avg_val_df["person_count"].mean() if not avg_val_df.empty else 0
        return {
            "next_hour": round(average_count * 1.2),
            "next_day": round(average_count * 1.1),
            "next_week": round(average_count * 1.05)
        }
    
    if latest_data_source_df.empty:
        return {"next_hour": 0, "next_day": 0, "next_week": 0}
    
    latest_data = latest_data_source_df.iloc[-1]
    
    features = np.array([
        latest_data.get("hour", 0), 
        latest_data.get("day_of_week", 0), 
        latest_data.get("is_weekend", 0),
        latest_data.get("rolling_mean_3h", 0), 
        latest_data.get("rolling_mean_24h", 0),
        latest_data.get("person_count", 0)
    ]).reshape(1, -1)
    
    dmatrix = xgb.DMatrix(features)
    
    next_hour = max(0, round(float(hourly_model.predict(dmatrix)[0])))
    next_day = max(0, round(float(daily_model.predict(dmatrix)[0])))
    next_week = max(0, round(float(weekly_model.predict(dmatrix)[0])))
    
    return {"next_hour": int(next_hour), "next_day": int(next_day), "next_week": int(next_week)}


def make_prediction_default(camera_id: str, data: List[Dict[str, Any]]) -> Dict[str, int]:
    """Placeholder function to simulate making a prediction. Matches utils.py."""
    if data and len(data) > 0:
        counts = [item["metadata"]["person_count"] 
                  for item in data 
                  if "metadata" in item and isinstance(item["metadata"], dict) 
                  and "person_count" in item["metadata"]
                  and isinstance(item["metadata"]["person_count"], (int, float))]
        if not counts:
            average_count = 100
        else:
            total_count = sum(counts)
            average_count = total_count / len(counts)
    else:
        average_count = 100
    
    return {
        "next_hour": round(average_count * 1.2),
        "next_day": round(average_count * 1.1),
        "next_week": round(average_count * 1.05)
    }

