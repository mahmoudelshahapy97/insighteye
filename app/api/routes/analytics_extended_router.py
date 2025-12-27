# app/routes/analytics_extended_router.py
from fastapi import APIRouter, HTTPException, Query, Depends, Request
from typing import Optional, Dict, List
from datetime import date, datetime, time, timedelta
from uuid import UUID
import logging

from app.services.session_service import session_manager
from app.services.workspace_service import workspace_service
from app.services.database import db_manager
from app.utils import check_workspace_access

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/analytics/extended", tags=["Extended Analytics"])

# ==================== OCCUPANCY & TRAFFIC ANALYTICS ====================

@router.get("/occupancy/peak-vs-offpeak")
async def get_peak_vs_offpeak_analysis(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    business_start_hour: int = Query(9, ge=0, le=23),
    business_end_hour: int = Query(17, ge=0, le=23),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Compare occupancy during business hours vs non-business hours
    
    Chart: Peak vs Off-Peak Comparison (Bar Chart)
    Returns: Average occupancy during business hours vs off-peak hours
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj, business_start_hour, business_end_hour]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            SELECT
                camera_name,
                camera_id,
                location,
                CASE 
                    WHEN EXTRACT(HOUR FROM time) >= $2 AND EXTRACT(HOUR FROM time) < $3 
                    THEN 'Business Hours'
                    ELSE 'Off-Peak Hours'
                END AS period_type,
                AVG(person_count) AS avg_person_count,
                MAX(person_count) AS max_person_count,
                MIN(person_count) AS min_person_count,
                COUNT(*) AS sample_size
            FROM stream_results
            WHERE workspace_id = $1
              AND time IS NOT NULL
              AND person_count IS NOT NULL
              {date_filter}
            GROUP BY camera_name, camera_id, location, period_type
            ORDER BY camera_name, period_type
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'avg_person_count': float(row['avg_person_count']) if row['avg_person_count'] else 0
        } for row in results]
        
        return {
            "success": True,
            "business_hours": f"{business_start_hour}:00 - {business_end_hour}:00",
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in peak vs offpeak analysis: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/occupancy/heatmap")
async def get_occupancy_heatmap(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    2D heatmap showing location + hour of day
    
    Chart: Occupancy Heatmap (Heatmap)
    Returns: Average occupancy by location and hour
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            SELECT
                COALESCE(location, 'Unknown') AS location,
                EXTRACT(HOUR FROM time) AS hour,
                AVG(person_count) AS avg_person_count,
                MAX(person_count) AS max_person_count,
                COUNT(*) AS sample_size
            FROM stream_results
            WHERE workspace_id = $1
              AND time IS NOT NULL
              AND person_count IS NOT NULL
              {date_filter}
            GROUP BY location, EXTRACT(HOUR FROM time)
            ORDER BY location, hour
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'avg_person_count': float(row['avg_person_count']) if row['avg_person_count'] else 0
        } for row in results]
        
        return {
            "success": True,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in occupancy heatmap: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/occupancy/capacity-utilization")
async def get_capacity_utilization(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Compare actual occupancy vs threshold capacity
    
    Chart: Capacity Utilization (Bar Chart with threshold line)
    Returns: Percentage utilization by camera
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND sr.date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND sr.date <= ${len(params)}"
            
        query = f"""
            SELECT
                sr.camera_name,
                sr.camera_id,
                sr.location,
                vs.count_threshold_greater AS max_capacity,
                AVG(sr.person_count) AS avg_occupancy,
                MAX(sr.person_count) AS peak_occupancy,
                CASE 
                    WHEN vs.count_threshold_greater IS NOT NULL AND vs.count_threshold_greater > 0
                    THEN ROUND((AVG(sr.person_count) / vs.count_threshold_greater * 100)::numeric, 2)
                    ELSE NULL
                END AS avg_utilization_percentage,
                CASE 
                    WHEN vs.count_threshold_greater IS NOT NULL AND vs.count_threshold_greater > 0
                    THEN ROUND((MAX(sr.person_count) / vs.count_threshold_greater * 100)::numeric, 2)
                    ELSE NULL
                END AS peak_utilization_percentage,
                COUNT(CASE WHEN sr.person_count > vs.count_threshold_greater THEN 1 END) AS over_capacity_count,
                COUNT(*) AS total_samples
            FROM stream_results sr
            JOIN video_stream vs ON sr.stream_id = vs.stream_id
            WHERE sr.workspace_id = $1
              AND sr.person_count IS NOT NULL
              AND vs.count_threshold_greater IS NOT NULL
              {date_filter}
            GROUP BY sr.camera_name, sr.camera_id, sr.location, vs.count_threshold_greater
            ORDER BY avg_utilization_percentage DESC NULLS LAST
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'avg_occupancy': float(row['avg_occupancy']) if row['avg_occupancy'] else 0,
            'avg_utilization_percentage': float(row['avg_utilization_percentage']) if row['avg_utilization_percentage'] else 0,
            'peak_utilization_percentage': float(row['peak_utilization_percentage']) if row['peak_utilization_percentage'] else 0
        } for row in results]
        
        return {
            "success": True,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in capacity utilization: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# ==================== GENDER ANALYTICS EXPANSION ====================

@router.get("/gender/ratio-trends")
async def get_gender_ratio_trends(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    interval: str = Query("day", regex="^(hour|day|week|month)$"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Male/female ratio changes over time
    
    Chart: Gender Ratio Trends (Line Chart)
    Returns: Gender ratio over time intervals
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
        
        truncate_expr = {
            'hour': "DATE_TRUNC('hour', timestamp)",
            'day': "DATE_TRUNC('day', timestamp)",
            'week': "DATE_TRUNC('week', timestamp)",
            'month': "DATE_TRUNC('month', timestamp)"
        }[interval]
            
        query = f"""
            SELECT
                {truncate_expr} AS time_period,
                SUM(male_count) AS total_male,
                SUM(female_count) AS total_female,
                SUM(person_count) AS total_people,
                CASE 
                    WHEN SUM(female_count) > 0 
                    THEN ROUND((SUM(male_count)::NUMERIC / SUM(female_count))::numeric, 2)
                    ELSE NULL
                END AS male_to_female_ratio,
                ROUND((SUM(male_count)::NUMERIC / NULLIF(SUM(person_count), 0) * 100)::numeric, 2) AS male_percentage,
                ROUND((SUM(female_count)::NUMERIC / NULLIF(SUM(person_count), 0) * 100)::numeric, 2) AS female_percentage
            FROM stream_results
            WHERE workspace_id = $1
              AND timestamp IS NOT NULL
              {date_filter}
            GROUP BY {truncate_expr}
            ORDER BY time_period DESC
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            'time_period': row['time_period'].isoformat(),
            'total_male': row['total_male'],
            'total_female': row['total_female'],
            'total_people': row['total_people'],
            'male_to_female_ratio': float(row['male_to_female_ratio']) if row['male_to_female_ratio'] else 0,
            'male_percentage': float(row['male_percentage']) if row['male_percentage'] else 0,
            'female_percentage': float(row['female_percentage']) if row['female_percentage'] else 0
        } for row in results]
        
        return {
            "success": True,
            "interval": interval,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in gender ratio trends: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/gender/distribution-by-location")
async def get_gender_distribution_by_location(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    group_by: str = Query("location", regex="^(location|area|building|zone|floor_level)$"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Gender distribution by location
    
    Chart: Gender Distribution (Stacked Bar Chart)
    Returns: Male/female distribution by location grouping
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            SELECT
                COALESCE({group_by}, 'Unknown') AS group_name,
                AVG(male_count) AS avg_male_count,
                AVG(female_count) AS avg_female_count,
                SUM(male_count) AS total_male_count,
                SUM(female_count) AS total_female_count,
                ROUND((AVG(male_count)::NUMERIC / NULLIF(AVG(person_count), 0) * 100)::numeric, 2) AS male_percentage,
                ROUND((AVG(female_count)::NUMERIC / NULLIF(AVG(person_count), 0) * 100)::numeric, 2) AS female_percentage,
                COUNT(*) AS sample_size
            FROM stream_results
            WHERE workspace_id = $1
              {date_filter}
            GROUP BY COALESCE({group_by}, 'Unknown')
            ORDER BY group_name
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'avg_male_count': float(row['avg_male_count']) if row['avg_male_count'] else 0,
            'avg_female_count': float(row['avg_female_count']) if row['avg_female_count'] else 0,
            'male_percentage': float(row['male_percentage']) if row['male_percentage'] else 0,
            'female_percentage': float(row['female_percentage']) if row['female_percentage'] else 0,
            'group_type': group_by
        } for row in results]
        
        return {
            "success": True,
            "group_by": group_by,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in gender distribution: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/gender/peak-hours-by-gender")
async def get_peak_hours_by_gender(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Separate busiest hours for male vs female counts
    
    Chart: Peak Hours by Gender (Grouped Bar Chart)
    Returns: Busiest hour for males and females separately
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            SELECT
                EXTRACT(HOUR FROM time) AS hour,
                AVG(male_count) AS avg_male_count,
                AVG(female_count) AS avg_female_count,
                MAX(male_count) AS max_male_count,
                MAX(female_count) AS max_female_count,
                COUNT(*) AS sample_size
            FROM stream_results
            WHERE workspace_id = $1
              AND time IS NOT NULL
              {date_filter}
            GROUP BY EXTRACT(HOUR FROM time)
            ORDER BY hour
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'avg_male_count': float(row['avg_male_count']) if row['avg_male_count'] else 0,
            'avg_female_count': float(row['avg_female_count']) if row['avg_female_count'] else 0
        } for row in results]
        
        # Find peak hours
        peak_male_hour = max(data, key=lambda x: x['avg_male_count']) if data else None
        peak_female_hour = max(data, key=lambda x: x['avg_female_count']) if data else None
        
        return {
            "success": True,
            "peak_male_hour": peak_male_hour['hour'] if peak_male_hour else None,
            "peak_female_hour": peak_female_hour['hour'] if peak_female_hour else None,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in peak hours by gender: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# ==================== PREDICTIVE & ANOMALY DETECTION ====================

@router.get("/predictive/occupancy-forecast")
async def get_occupancy_forecast(
    request: Request,
    camera_id: Optional[UUID] = None,
    forecast_hours: int = Query(24, ge=1, le=168),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Predict expected occupancy based on historical patterns
    
    Chart: Occupancy Forecast (Line Chart with confidence bands)
    Returns: Forecasted occupancy for upcoming hours
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        camera_filter = ""
        
        if camera_id:
            params.append(camera_id)
            camera_filter = f" AND stream_id = ${len(params)}"
            
        # Get historical averages by hour of day and day of week
        query = f"""
            WITH historical_patterns AS (
                SELECT
                    COALESCE(camera_name, 'All Cameras') AS camera_name,
                    EXTRACT(DOW FROM date) AS day_of_week,
                    EXTRACT(HOUR FROM time) AS hour_of_day,
                    AVG(person_count) AS avg_count,
                    STDDEV(person_count) AS stddev_count,
                    COUNT(*) AS sample_size
                FROM stream_results
                WHERE workspace_id = $1
                  AND date >= CURRENT_DATE - INTERVAL '30 days'
                  AND person_count IS NOT NULL
                  {camera_filter}
                GROUP BY camera_name, day_of_week, hour_of_day
            )
            SELECT
                camera_name,
                day_of_week,
                hour_of_day,
                ROUND(avg_count::numeric, 2) AS forecast_count,
                ROUND(COALESCE(stddev_count, 0)::numeric, 2) AS uncertainty,
                sample_size,
                ROUND((avg_count - COALESCE(stddev_count, 0))::numeric, 2) AS lower_bound,
                ROUND((avg_count + COALESCE(stddev_count, 0))::numeric, 2) AS upper_bound
            FROM historical_patterns
            ORDER BY camera_name, day_of_week, hour_of_day
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        # Generate forecast for next N hours
        current_time = datetime.now()
        forecast_data = []
        
        for i in range(forecast_hours):
            future_time = current_time + timedelta(hours=i)
            dow = future_time.weekday()
            hour = future_time.hour
            
            # Find matching historical pattern
            for row in results:
                if int(row['day_of_week']) == (dow + 1) % 7 and int(row['hour_of_day']) == hour:
                    forecast_data.append({
                        'forecast_datetime': future_time.isoformat(),
                        'camera_name': row['camera_name'],
                        'hour': hour,
                        'day_of_week': dow,
                        'forecast_count': float(row['forecast_count']),
                        'lower_bound': max(0, float(row['lower_bound'])),
                        'upper_bound': float(row['upper_bound']),
                        'uncertainty': float(row['uncertainty']),
                        'confidence': 'high' if row['sample_size'] > 10 else 'medium' if row['sample_size'] > 5 else 'low'
                    })
        
        return {
            "success": True,
            "forecast_hours": forecast_hours,
            "forecast_start": current_time.isoformat(),
            "forecast_end": (current_time + timedelta(hours=forecast_hours)).isoformat(),
            "data": forecast_data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in occupancy forecast: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/anomaly/detection")
async def detect_anomalies(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    sensitivity: float = Query(2.0, ge=1.0, le=5.0, description="Standard deviations for anomaly threshold"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Identify unusual spikes or drops in person count
    
    Chart: Anomaly Detection (Scatter Plot with threshold bands)
    Returns: Data points that deviate significantly from normal patterns
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj, sensitivity]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            WITH camera_stats AS (
                SELECT
                    stream_id,
                    camera_name,
                    AVG(person_count) AS avg_count,
                    STDDEV(person_count) AS stddev_count
                FROM stream_results
                WHERE workspace_id = $1
                  AND person_count IS NOT NULL
                  {date_filter}
                GROUP BY stream_id, camera_name
            ),
            anomalies AS (
                SELECT
                    sr.result_id,
                    sr.stream_id,
                    sr.camera_name,
                    sr.timestamp,
                    sr.person_count,
                    sr.location,
                    cs.avg_count,
                    cs.stddev_count,
                    CASE 
                        WHEN sr.person_count > (cs.avg_count + ($2 * COALESCE(cs.stddev_count, 0)))
                        THEN 'High Spike'
                        WHEN sr.person_count < (cs.avg_count - ($2 * COALESCE(cs.stddev_count, 0)))
                        THEN 'Low Drop'
                        ELSE 'Normal'
                    END AS anomaly_type,
                    ROUND(ABS(sr.person_count - cs.avg_count)::numeric, 2) AS deviation,
                    ROUND((ABS(sr.person_count - cs.avg_count) / NULLIF(cs.stddev_count, 1))::numeric, 2) AS z_score
                FROM stream_results sr
                JOIN camera_stats cs ON sr.stream_id = cs.stream_id
                WHERE sr.workspace_id = $1
                  AND sr.person_count IS NOT NULL
                  {date_filter}
            )
            SELECT *
            FROM anomalies
            WHERE anomaly_type != 'Normal'
            ORDER BY z_score DESC, timestamp DESC
            LIMIT 1000
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            'result_id': str(row['result_id']),
            'stream_id': str(row['stream_id']),
            'camera_name': row['camera_name'],
            'timestamp': row['timestamp'].isoformat(),
            'person_count': row['person_count'],
            'location': row['location'],
            'avg_count': float(row['avg_count']) if row['avg_count'] else 0,
            'stddev_count': float(row['stddev_count']) if row['stddev_count'] else 0,
            'anomaly_type': row['anomaly_type'],
            'deviation': float(row['deviation']),
            'z_score': float(row['z_score']) if row['z_score'] else 0,
            'severity': 'critical' if row['z_score'] and float(row['z_score']) > 3 else 'high' if row['z_score'] and float(row['z_score']) > 2.5 else 'medium'
        } for row in results]
        
        # Summary stats
        high_spikes = len([d for d in data if d['anomaly_type'] == 'High Spike'])
        low_drops = len([d for d in data if d['anomaly_type'] == 'Low Drop'])
        
        return {
            "success": True,
            "sensitivity": sensitivity,
            "summary": {
                "total_anomalies": len(data),
                "high_spikes": high_spikes,
                "low_drops": low_drops,
                "critical_anomalies": len([d for d in data if d['severity'] == 'critical'])
            },
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in anomaly detection: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/predictive/threshold-breach-forecast")
async def forecast_threshold_breaches(
    request: Request,
    camera_id: Optional[UUID] = None,
    forecast_hours: int = Query(24, ge=1, le=168),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Forecast when thresholds are likely to be exceeded
    
    Chart: Threshold Breach Forecast (Timeline)
    Returns: Predicted times when thresholds will be breached
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        camera_filter = ""
        
        if camera_id:
            params.append(camera_id)
            camera_filter = f" AND sr.stream_id = ${len(params)}"
            
        # Get cameras with thresholds and their historical patterns
        query = f"""
            WITH camera_thresholds AS (
                SELECT
                    vs.stream_id,
                    vs.name AS camera_name,
                    vs.count_threshold_greater,
                    vs.count_threshold_less
                FROM video_stream vs
                WHERE vs.workspace_id = $1
                  AND vs.alert_enabled = TRUE
                  AND (vs.count_threshold_greater IS NOT NULL OR vs.count_threshold_less IS NOT NULL)
                  {camera_filter}
            ),
            historical_patterns AS (
                SELECT
                    sr.stream_id,
                    ct.camera_name,
                    ct.count_threshold_greater,
                    ct.count_threshold_less,
                    EXTRACT(DOW FROM sr.date) AS day_of_week,
                    EXTRACT(HOUR FROM sr.time) AS hour_of_day,
                    AVG(sr.person_count) AS avg_count,
                    MAX(sr.person_count) AS max_count
                FROM stream_results sr
                JOIN camera_thresholds ct ON sr.stream_id = ct.stream_id
                WHERE sr.workspace_id = $1
                  AND sr.date >= CURRENT_DATE - INTERVAL '30 days'
                  AND sr.person_count IS NOT NULL
                  {camera_filter}
                GROUP BY sr.stream_id, ct.camera_name, ct.count_threshold_greater, ct.count_threshold_less, day_of_week, hour_of_day
            )
            SELECT
                stream_id,
                camera_name,
                count_threshold_greater,
                count_threshold_less,
                day_of_week,
                hour_of_day,
                ROUND(avg_count::numeric, 2) AS avg_count,
                max_count,
                CASE 
                    WHEN count_threshold_greater IS NOT NULL AND avg_count > count_threshold_greater
                    THEN 'Likely Breach (Above Max)'
                    WHEN count_threshold_less IS NOT NULL AND avg_count < count_threshold_less
                    THEN 'Likely Breach (Below Min)'
                    WHEN count_threshold_greater IS NOT NULL AND max_count > count_threshold_greater
                    THEN 'Possible Breach (Above Max)'
                    ELSE 'No Breach Expected'
                END AS breach_prediction
            FROM historical_patterns
            WHERE (count_threshold_greater IS NOT NULL AND (avg_count > count_threshold_greater * 0.8 OR max_count > count_threshold_greater))
               OR (count_threshold_less IS NOT NULL AND avg_count < count_threshold_less * 1.2)
            ORDER BY stream_id, day_of_week, hour_of_day
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        # Generate forecast
        current_time = datetime.now()
        forecast_data = []
        
        for i in range(forecast_hours):
            future_time = current_time + timedelta(hours=i)
            dow = future_time.weekday()
            hour = future_time.hour
            
            for row in results:
                if int(row['day_of_week']) == (dow + 1) % 7 and int(row['hour_of_day']) == hour:
                    forecast_data.append({
                        'forecast_datetime': future_time.isoformat(),
                        'stream_id': str(row['stream_id']),
                        'camera_name': row['camera_name'],
                        'predicted_count': float(row['avg_count']),
                        'max_threshold': row['count_threshold_greater'],
                        'min_threshold': row['count_threshold_less'],
                        'breach_prediction': row['breach_prediction'],
                        'risk_level': 'high' if 'Likely' in row['breach_prediction'] else 'medium'
                    })
        
        return {
            "success": True,
            "forecast_hours": forecast_hours,
            "breach_forecasts": len(forecast_data),
            "data": forecast_data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in threshold breach forecast: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# ==================== CAMERA PERFORMANCE & HEALTH ====================

@router.get("/camera/uptime-reliability")
async def get_camera_uptime_reliability(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Camera uptime and data collection consistency
    
    Chart: Camera Reliability (Bar Chart)
    Returns: Frame capture consistency and data gaps
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            WITH camera_activity AS (
                SELECT
                    stream_id,
                    camera_name,
                    location,
                    COUNT(*) AS total_frames,
                    COUNT(DISTINCT date) AS active_days,
                    MIN(timestamp) AS first_frame,
                    MAX(timestamp) AS last_frame,
                    EXTRACT(EPOCH FROM (MAX(timestamp) - MIN(timestamp))) / 3600 AS hours_span,
                    COUNT(*) / NULLIF(EXTRACT(EPOCH FROM (MAX(timestamp) - MIN(timestamp))) / 3600, 0) AS frames_per_hour
                FROM stream_results
                WHERE workspace_id = $1
                  {date_filter}
                GROUP BY stream_id, camera_name, location
            ),
            expected_days AS (
                SELECT 
                    CASE 
                        WHEN ${len(params)} > 1 THEN 
                            EXTRACT(DAY FROM (${len(params)}::date - ${len(params) - 1}::date)) + 1
                        ELSE 
                            EXTRACT(DAY FROM (CURRENT_DATE - (SELECT MIN(date) FROM stream_results WHERE workspace_id = $1))) + 1
                    END AS expected_days
            )
            SELECT
                ca.stream_id,
                ca.camera_name,
                ca.location,
                ca.total_frames,
                ca.active_days,
                ed.expected_days,
                ROUND((ca.active_days::NUMERIC / NULLIF(ed.expected_days, 0) * 100)::numeric, 2) AS uptime_percentage,
                ca.first_frame,
                ca.last_frame,
                ROUND(ca.frames_per_hour::numeric, 2) AS avg_frames_per_hour,
                CASE 
                    WHEN ca.active_days::NUMERIC / NULLIF(ed.expected_days, 0) >= 0.95 THEN 'Excellent'
                    WHEN ca.active_days::NUMERIC / NULLIF(ed.expected_days, 0) >= 0.80 THEN 'Good'
                    WHEN ca.active_days::NUMERIC / NULLIF(ed.expected_days, 0) >= 0.60 THEN 'Fair'
                    ELSE 'Poor'
                END AS reliability_rating
            FROM camera_activity ca
            CROSS JOIN expected_days ed
            ORDER BY uptime_percentage DESC
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            'stream_id': str(row['stream_id']),
            'camera_name': row['camera_name'],
            'location': row['location'],
            'total_frames': row['total_frames'],
            'active_days': row['active_days'],
            'expected_days': float(row['expected_days']) if row['expected_days'] else 0,
            'uptime_percentage': float(row['uptime_percentage']) if row['uptime_percentage'] else 0,
            'first_frame': row['first_frame'].isoformat() if row['first_frame'] else None,
            'last_frame': row['last_frame'].isoformat() if row['last_frame'] else None,
            'avg_frames_per_hour': float(row['avg_frames_per_hour']) if row['avg_frames_per_hour'] else 0,
            'reliability_rating': row['reliability_rating']
        } for row in results]
        
        return {
            "success": True,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in camera uptime: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/camera/data-quality")
async def get_data_quality_metrics(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Data quality metrics per camera
    
    Chart: Data Quality Dashboard (Scorecard)
    Returns: Missing data, null values, data completeness
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            SELECT
                stream_id,
                camera_name,
                location,
                COUNT(*) AS total_records,
                COUNT(CASE WHEN person_count IS NULL THEN 1 END) AS null_person_count,
                COUNT(CASE WHEN male_count IS NULL THEN 1 END) AS null_male_count,
                COUNT(CASE WHEN female_count IS NULL THEN 1 END) AS null_female_count,
                COUNT(CASE WHEN location IS NULL OR location = 'Unknown' THEN 1 END) AS null_location,
                COUNT(CASE WHEN timestamp IS NULL THEN 1 END) AS null_timestamp,
                ROUND((COUNT(CASE WHEN person_count IS NOT NULL THEN 1 END)::NUMERIC / COUNT(*) * 100)::numeric, 2) AS person_count_completeness,
                ROUND((COUNT(CASE WHEN male_count IS NOT NULL THEN 1 END)::NUMERIC / COUNT(*) * 100)::numeric, 2) AS male_count_completeness,
                ROUND((COUNT(CASE WHEN female_count IS NOT NULL THEN 1 END)::NUMERIC / COUNT(*) * 100)::numeric, 2) AS female_count_completeness,
                ROUND((COUNT(CASE WHEN location IS NOT NULL AND location != 'Unknown' THEN 1 END)::NUMERIC / COUNT(*) * 100)::numeric, 2) AS location_completeness,
                CASE 
                    WHEN COUNT(CASE WHEN person_count IS NOT NULL AND location IS NOT NULL THEN 1 END)::NUMERIC / COUNT(*) >= 0.95 THEN 'Excellent'
                    WHEN COUNT(CASE WHEN person_count IS NOT NULL AND location IS NOT NULL THEN 1 END)::NUMERIC / COUNT(*) >= 0.80 THEN 'Good'
                    WHEN COUNT(CASE WHEN person_count IS NOT NULL AND location IS NOT NULL THEN 1 END)::NUMERIC / COUNT(*) >= 0.60 THEN 'Fair'
                    ELSE 'Poor'
                END AS data_quality_rating
            FROM stream_results
            WHERE workspace_id = $1
              {date_filter}
            GROUP BY stream_id, camera_name, location
            ORDER BY data_quality_rating DESC, camera_name
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            'stream_id': str(row['stream_id']),
            'camera_name': row['camera_name'],
            'location': row['location'],
            'total_records': row['total_records'],
            'null_person_count': row['null_person_count'],
            'null_male_count': row['null_male_count'],
            'null_female_count': row['null_female_count'],
            'null_location': row['null_location'],
            'null_timestamp': row['null_timestamp'],
            'person_count_completeness': float(row['person_count_completeness']) if row['person_count_completeness'] else 0,
            'male_count_completeness': float(row['male_count_completeness']) if row['male_count_completeness'] else 0,
            'female_count_completeness': float(row['female_count_completeness']) if row['female_count_completeness'] else 0,
            'location_completeness': float(row['location_completeness']) if row['location_completeness'] else 0,
            'data_quality_rating': row['data_quality_rating']
        } for row in results]
        
        return {
            "success": True,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in data quality metrics: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/camera/comparison-matrix")
async def get_camera_comparison_matrix(
    request: Request,
    camera_ids: List[UUID] = Query(None, description="List of camera IDs to compare"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Side-by-side comparison of multiple cameras
    
    Chart: Camera Comparison Matrix (Table)
    Returns: Normalized performance metrics for selected cameras
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        date_filter = ""
        camera_filter = ""
        
        if camera_ids:
            camera_filter = f" AND stream_id = ANY(${len(params) + 1})"
            params.append(camera_ids)
            
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            SELECT
                sr.stream_id,
                sr.camera_name,
                sr.location,
                vs.status,
                COUNT(*) AS total_frames,
                AVG(sr.person_count) AS avg_occupancy,
                MAX(sr.person_count) AS peak_occupancy,
                MIN(sr.person_count) AS min_occupancy,
                STDDEV(sr.person_count) AS occupancy_variance,
                COUNT(DISTINCT sr.date) AS active_days,
                COUNT(CASE WHEN sr.fire_status != 'no detection' THEN 1 END) AS fire_detections,
                AVG(sr.male_count) AS avg_male,
                AVG(sr.female_count) AS avg_female,
                vs.count_threshold_greater,
                vs.count_threshold_less,
                vs.alert_enabled
            FROM stream_results sr
            JOIN video_stream vs ON sr.stream_id = vs.stream_id
            WHERE sr.workspace_id = $1
              {camera_filter}
              {date_filter}
            GROUP BY sr.stream_id, sr.camera_name, sr.location, vs.status, vs.count_threshold_greater, vs.count_threshold_less, vs.alert_enabled
            ORDER BY sr.camera_name
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            'stream_id': str(row['stream_id']),
            'camera_name': row['camera_name'],
            'location': row['location'],
            'status': row['status'],
            'total_frames': row['total_frames'],
            'avg_occupancy': float(row['avg_occupancy']) if row['avg_occupancy'] else 0,
            'peak_occupancy': row['peak_occupancy'],
            'min_occupancy': row['min_occupancy'],
            'occupancy_variance': float(row['occupancy_variance']) if row['occupancy_variance'] else 0,
            'active_days': row['active_days'],
            'fire_detections': row['fire_detections'],
            'avg_male': float(row['avg_male']) if row['avg_male'] else 0,
            'avg_female': float(row['avg_female']) if row['avg_female'] else 0,
            'max_threshold': row['count_threshold_greater'],
            'min_threshold': row['count_threshold_less'],
            'alert_enabled': row['alert_enabled']
        } for row in results]
        
        return {
            "success": True,
            "cameras_compared": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in camera comparison: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# ==================== LOCATION & ZONE INTELLIGENCE ====================

@router.get("/zones/popularity-rankings")
async def get_zone_popularity_rankings(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    group_by: str = Query("zone", regex="^(location|area|building|zone|floor_level)$"),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Most/least visited zones over time
    
    Chart: Zone Popularity (Bar Chart)
    Returns: Ranked zones by total visits/occupancy
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            WITH zone_stats AS (
                SELECT
                    COALESCE({group_by}, 'Unknown') AS zone_name,
                    SUM(person_count) AS total_visits,
                    AVG(person_count) AS avg_occupancy,
                    MAX(person_count) AS peak_occupancy,
                    COUNT(*) AS total_observations,
                    COUNT(DISTINCT camera_id) AS cameras_in_zone,
                    COUNT(DISTINCT date) AS active_days
                FROM stream_results
                WHERE workspace_id = $1
                  AND person_count IS NOT NULL
                  {date_filter}
                GROUP BY COALESCE({group_by}, 'Unknown')
            ),
            ranked_zones AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (ORDER BY total_visits DESC) AS popularity_rank,
                    ROUND((total_visits::NUMERIC / SUM(total_visits) OVER () * 100)::numeric, 2) AS percentage_of_total_visits
                FROM zone_stats
            )
            SELECT
                zone_name,
                total_visits,
                ROUND(avg_occupancy::numeric, 2) AS avg_occupancy,
                peak_occupancy,
                total_observations,
                cameras_in_zone,
                active_days,
                popularity_rank,
                percentage_of_total_visits,
                CASE 
                    WHEN popularity_rank <= 3 THEN 'Top Tier'
                    WHEN popularity_rank <= 10 THEN 'High Traffic'
                    WHEN popularity_rank <= 20 THEN 'Medium Traffic'
                    ELSE 'Low Traffic'
                END AS traffic_tier
            FROM ranked_zones
            ORDER BY popularity_rank
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'avg_occupancy': float(row['avg_occupancy']) if row['avg_occupancy'] else 0,
            'percentage_of_total_visits': float(row['percentage_of_total_visits']) if row['percentage_of_total_visits'] else 0
        } for row in results]
        
        return {
            "success": True,
            "group_by": group_by,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in zone popularity: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/locations/cross-analysis")
async def get_cross_location_analysis(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Compare metrics across buildings/floors/zones
    
    Chart: Cross-Location Comparison (Multi-bar Chart)
    Returns: Benchmark locations against each other
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            WITH location_metrics AS (
                SELECT
                    COALESCE(building, 'Unknown Building') AS building,
                    COALESCE(floor_level, 'Unknown Floor') AS floor,
                    COALESCE(zone, 'Unknown Zone') AS zone,
                    AVG(person_count) AS avg_occupancy,
                    MAX(person_count) AS peak_occupancy,
                    COUNT(*) AS total_observations,
                    COUNT(DISTINCT camera_id) AS camera_count,
                    COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END) AS fire_incidents,
                    AVG(male_count) AS avg_male,
                    AVG(female_count) AS avg_female
                FROM stream_results
                WHERE workspace_id = $1
                  {date_filter}
                GROUP BY building, floor_level, zone
            )
            SELECT
                building,
                floor,
                zone,
                ROUND(avg_occupancy::numeric, 2) AS avg_occupancy,
                peak_occupancy,
                total_observations,
                camera_count,
                fire_incidents,
                ROUND(avg_male::numeric, 2) AS avg_male,
                ROUND(avg_female::numeric, 2) AS avg_female,
                ROUND((avg_occupancy / NULLIF((SELECT AVG(avg_occupancy) FROM location_metrics), 0) * 100)::numeric, 2) AS occupancy_vs_average
            FROM location_metrics
            ORDER BY building, floor, zone
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            **dict(row),
            'avg_occupancy': float(row['avg_occupancy']) if row['avg_occupancy'] else 0,
            'avg_male': float(row['avg_male']) if row['avg_male'] else 0,
            'avg_female': float(row['avg_female']) if row['avg_female'] else 0,
            'occupancy_vs_average': float(row['occupancy_vs_average']) if row['occupancy_vs_average'] else 0
        } for row in results]
        
        return {
            "success": True,
            "count": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in cross-location analysis: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/locations/hierarchy-drilldown")
async def get_location_hierarchy_drilldown(
    request: Request,
    building: Optional[str] = None,
    floor_level: Optional[str] = None,
    zone: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Drill-down from building → floor → zone → area → camera
    
    Chart: Hierarchical Drill-down (Tree View)
    Returns: Aggregated metrics at each hierarchy level
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        filters = ["workspace_id = $1"]
        
        if building:
            params.append(building)
            filters.append(f"building = ${len(params)}")
        if floor_level:
            params.append(floor_level)
            filters.append(f"floor_level = ${len(params)}")
        if zone:
            params.append(zone)
            filters.append(f"zone = ${len(params)}")
        if start_date:
            params.append(start_date)
            filters.append(f"date >= ${len(params)}")
        if end_date:
            params.append(end_date)
            filters.append(f"date <= ${len(params)}")
            
        where_clause = " AND ".join(filters)
            
        query = f"""
            SELECT
                COALESCE(building, 'Unknown') AS building,
                COALESCE(floor_level, 'Unknown') AS floor_level,
                COALESCE(zone, 'Unknown') AS zone,
                COALESCE(area, 'Unknown') AS area,
                COALESCE(location, 'Unknown') AS location,
                camera_name,
                camera_id,
                COUNT(*) AS total_records,
                AVG(person_count) AS avg_occupancy,
                MAX(person_count) AS peak_occupancy,
                SUM(person_count) AS total_people_counted
            FROM stream_results
            WHERE {where_clause}
            GROUP BY building, floor_level, zone, area, location, camera_name, camera_id
            ORDER BY building, floor_level, zone, area, location, camera_name
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        # Build hierarchical structure
        hierarchy = {}
        for row in results:
            bldg = row['building']
            flr = row['floor_level']
            zn = row['zone']
            ar = row['area']
            loc = row['location']
            cam = row['camera_name']
            
            if bldg not in hierarchy:
                hierarchy[bldg] = {'floors': {}, 'metrics': {'total_records': 0, 'avg_occupancy': 0, 'peak_occupancy': 0}}
            
            if flr not in hierarchy[bldg]['floors']:
                hierarchy[bldg]['floors'][flr] = {'zones': {}, 'metrics': {'total_records': 0, 'avg_occupancy': 0, 'peak_occupancy': 0}}
            
            if zn not in hierarchy[bldg]['floors'][flr]['zones']:
                hierarchy[bldg]['floors'][flr]['zones'][zn] = {'areas': {}, 'metrics': {'total_records': 0, 'avg_occupancy': 0, 'peak_occupancy': 0}}
            
            if ar not in hierarchy[bldg]['floors'][flr]['zones'][zn]['areas']:
                hierarchy[bldg]['floors'][flr]['zones'][zn]['areas'][ar] = {'locations': {}, 'metrics': {'total_records': 0, 'avg_occupancy': 0, 'peak_occupancy': 0}}
            
            if loc not in hierarchy[bldg]['floors'][flr]['zones'][zn]['areas'][ar]['locations']:
                hierarchy[bldg]['floors'][flr]['zones'][zn]['areas'][ar]['locations'][loc] = {'cameras': [], 'metrics': {'total_records': 0, 'avg_occupancy': 0, 'peak_occupancy': 0}}
            
            hierarchy[bldg]['floors'][flr]['zones'][zn]['areas'][ar]['locations'][loc]['cameras'].append({
                'camera_name': cam,
                'camera_id': row['camera_id'],
                'total_records': row['total_records'],
                'avg_occupancy': float(row['avg_occupancy']) if row['avg_occupancy'] else 0,
                'peak_occupancy': row['peak_occupancy'],
                'total_people_counted': row['total_people_counted']
            })
        
        return {
            "success": True,
            "hierarchy": hierarchy,
            "filters_applied": {
                "building": building,
                "floor_level": floor_level,
                "zone": zone
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in hierarchy drilldown: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# ==================== TIME-BASED ANALYTICS ====================

@router.get("/time/month-over-month")
async def get_month_over_month_comparison(
    request: Request,
    months_back: int = Query(6, ge=2, le=12),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Compare current month trends vs previous months
    
    Chart: Month-over-Month Comparison (Line Chart)
    Returns: Monthly trends with growth percentages
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        query = """
            WITH monthly_stats AS (
                SELECT
                    DATE_TRUNC('month', date) AS month,
                    AVG(person_count) AS avg_occupancy,
                    MAX(person_count) AS peak_occupancy,
                    SUM(person_count) AS total_people,
                    COUNT(*) AS total_observations,
                    COUNT(DISTINCT camera_id) AS active_cameras,
                    COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END) AS fire_incidents
                FROM stream_results
                WHERE workspace_id = $1
                  AND date >= DATE_TRUNC('month', CURRENT_DATE) - ($2 || ' months')::INTERVAL
                GROUP BY DATE_TRUNC('month', date)
            ),
            monthly_comparison AS (
                SELECT
                    month,
                    avg_occupancy,
                    peak_occupancy,
                    total_people,
                    total_observations,
                    active_cameras,
                    fire_incidents,
                    LAG(avg_occupancy) OVER (ORDER BY month) AS prev_avg_occupancy,
                    LAG(total_people) OVER (ORDER BY month) AS prev_total_people,
                    LAG(fire_incidents) OVER (ORDER BY month) AS prev_fire_incidents
                FROM monthly_stats
            )
            SELECT
                month,
                ROUND(avg_occupancy::numeric, 2) AS avg_occupancy,
                peak_occupancy,
                total_people,
                total_observations,
                active_cameras,
                fire_incidents,
                ROUND(prev_avg_occupancy::numeric, 2) AS prev_avg_occupancy,
                CASE 
                    WHEN prev_avg_occupancy IS NOT NULL AND prev_avg_occupancy > 0
                    THEN ROUND(((avg_occupancy - prev_avg_occupancy) / prev_avg_occupancy * 100)::numeric, 2)
                    ELSE NULL
                END AS occupancy_growth_percentage,
                CASE 
                    WHEN prev_total_people IS NOT NULL AND prev_total_people > 0
                    THEN ROUND(((total_people - prev_total_people)::NUMERIC / prev_total_people * 100)::numeric, 2)
                    ELSE NULL
                END AS people_growth_percentage,
                CASE 
                    WHEN prev_fire_incidents IS NOT NULL
                    THEN (fire_incidents - prev_fire_incidents)
                    ELSE NULL
                END AS fire_incidents_change
            FROM monthly_comparison
            ORDER BY month DESC
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, workspace_id_obj, months_back)
            
        data = [{
            'month': row['month'].isoformat(),
            'avg_occupancy': float(row['avg_occupancy']) if row['avg_occupancy'] else 0,
            'peak_occupancy': row['peak_occupancy'],
            'total_people': row['total_people'],
            'total_observations': row['total_observations'],
            'active_cameras': row['active_cameras'],
            'fire_incidents': row['fire_incidents'],
            'prev_avg_occupancy': float(row['prev_avg_occupancy']) if row['prev_avg_occupancy'] else None,
            'occupancy_growth_percentage': float(row['occupancy_growth_percentage']) if row['occupancy_growth_percentage'] else None,
            'people_growth_percentage': float(row['people_growth_percentage']) if row['people_growth_percentage'] else None,
            'fire_incidents_change': row['fire_incidents_change'],
            'trend': 'increasing' if row['occupancy_growth_percentage'] and float(row['occupancy_growth_percentage']) > 0 else 'decreasing' if row['occupancy_growth_percentage'] and float(row['occupancy_growth_percentage']) < 0 else 'stable'
        } for row in results]
        
        return {
            "success": True,
            "months_analyzed": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in month-over-month: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/time/seasonal-patterns")
async def get_seasonal_patterns(
    request: Request,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Weekly, monthly, quarterly patterns
    
    Chart: Seasonal Patterns (Multi-line Chart)
    Returns: Patterns by different time periods
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        query = """
            SELECT
                EXTRACT(QUARTER FROM date) AS quarter,
                EXTRACT(MONTH FROM date) AS month,
                EXTRACT(WEEK FROM date) AS week,
                TO_CHAR(date, 'Day') AS weekday_name,
                EXTRACT(DOW FROM date) AS weekday_num,
                AVG(person_count) AS avg_occupancy,
                MAX(person_count) AS peak_occupancy,
                COUNT(*) AS observations
            FROM stream_results
            WHERE workspace_id = $1
              AND date >= CURRENT_DATE - INTERVAL '1 year'
              AND person_count IS NOT NULL
            GROUP BY quarter, month, week, TO_CHAR(date, 'Day'), EXTRACT(DOW FROM date)
            ORDER BY quarter, month, week, weekday_num
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, workspace_id_obj)
            
        # Group by different periods
        quarterly = {}
        monthly = {}
        weekly = {}
        
        for row in results:
            q = int(row['quarter'])
            m = int(row['month'])
            w = int(row['week'])
            
            if q not in quarterly:
                quarterly[q] = {'avg_occupancy': [], 'peak_occupancy': []}
            quarterly[q]['avg_occupancy'].append(float(row['avg_occupancy']) if row['avg_occupancy'] else 0)
            quarterly[q]['peak_occupancy'].append(row['peak_occupancy'])
            
            if m not in monthly:
                monthly[m] = {'avg_occupancy': [], 'peak_occupancy': []}
            monthly[m]['avg_occupancy'].append(float(row['avg_occupancy']) if row['avg_occupancy'] else 0)
            monthly[m]['peak_occupancy'].append(row['peak_occupancy'])
            
            if w not in weekly:
                weekly[w] = {'avg_occupancy': [], 'peak_occupancy': []}
            weekly[w]['avg_occupancy'].append(float(row['avg_occupancy']) if row['avg_occupancy'] else 0)
            weekly[w]['peak_occupancy'].append(row['peak_occupancy'])
        
        # Calculate averages
        quarterly_data = [{
            'quarter': f"Q{q}",
            'avg_occupancy': sum(data['avg_occupancy']) / len(data['avg_occupancy']) if data['avg_occupancy'] else 0,
            'peak_occupancy': max(data['peak_occupancy']) if data['peak_occupancy'] else 0
        } for q, data in quarterly.items()]
        
        monthly_data = [{
            'month': m,
            'month_name': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][m-1],
            'avg_occupancy': sum(data['avg_occupancy']) / len(data['avg_occupancy']) if data['avg_occupancy'] else 0,
            'peak_occupancy': max(data['peak_occupancy']) if data['peak_occupancy'] else 0
        } for m, data in monthly.items()]
        
        return {
            "success": True,
            "quarterly_patterns": quarterly_data,
            "monthly_patterns": sorted(monthly_data, key=lambda x: x['month']),
            "total_weeks_analyzed": len(weekly)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in seasonal patterns: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# ==================== COMPARATIVE & BENCHMARKING ====================

@router.get("/benchmark/camera-vs-average")
async def get_camera_vs_average_benchmark(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Individual camera performance vs workspace average
    
    Chart: Camera vs Average (Deviation Chart)
    Returns: How each camera compares to workspace average
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            WITH workspace_avg AS (
                SELECT
                    AVG(person_count) AS workspace_avg_occupancy,
                    STDDEV(person_count) AS workspace_stddev,
                    AVG(male_count) AS workspace_avg_male,
                    AVG(female_count) AS workspace_avg_female
                FROM stream_results
                WHERE workspace_id = $1
                  AND person_count IS NOT NULL
                  {date_filter}
            ),
            camera_stats AS (
                SELECT
                    stream_id,
                    camera_name,
                    location,
                    AVG(person_count) AS camera_avg_occupancy,
                    AVG(male_count) AS camera_avg_male,
                    AVG(female_count) AS camera_avg_female,
                    COUNT(*) AS observations
                FROM stream_results
                WHERE workspace_id = $1
                  AND person_count IS NOT NULL
                  {date_filter}
                GROUP BY stream_id, camera_name, location
            )
            SELECT
                cs.stream_id,
                cs.camera_name,
                cs.location,
                ROUND(cs.camera_avg_occupancy::numeric, 2) AS camera_avg_occupancy,
                ROUND(wa.workspace_avg_occupancy::numeric, 2) AS workspace_avg_occupancy,
                ROUND((cs.camera_avg_occupancy - wa.workspace_avg_occupancy)::numeric, 2) AS deviation_from_avg,
                ROUND(((cs.camera_avg_occupancy - wa.workspace_avg_occupancy) / NULLIF(wa.workspace_avg_occupancy, 0) * 100)::numeric, 2) AS percentage_deviation,
                CASE 
                    WHEN cs.camera_avg_occupancy > wa.workspace_avg_occupancy + wa.workspace_stddev THEN 'Significantly Above Average'
                    WHEN cs.camera_avg_occupancy > wa.workspace_avg_occupancy THEN 'Above Average'
                    WHEN cs.camera_avg_occupancy < wa.workspace_avg_occupancy - wa.workspace_stddev THEN 'Significantly Below Average'
                    WHEN cs.camera_avg_occupancy < wa.workspace_avg_occupancy THEN 'Below Average'
                    ELSE 'Average'
                END AS performance_category,
                cs.observations
            FROM camera_stats cs
            CROSS JOIN workspace_avg wa
            ORDER BY deviation_from_avg DESC
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            'stream_id': str(row['stream_id']),
            'camera_name': row['camera_name'],
            'location': row['location'],
            'camera_avg_occupancy': float(row['camera_avg_occupancy']) if row['camera_avg_occupancy'] else 0,
            'workspace_avg_occupancy': float(row['workspace_avg_occupancy']) if row['workspace_avg_occupancy'] else 0,
            'deviation_from_avg': float(row['deviation_from_avg']) if row['deviation_from_avg'] else 0,
            'percentage_deviation': float(row['percentage_deviation']) if row['percentage_deviation'] else 0,
            'performance_category': row['performance_category'],
            'observations': row['observations']
        } for row in results]
        
        return {
            "success": True,
            "cameras_analyzed": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in camera vs average: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/benchmark/best-worst-performers")
async def get_best_worst_performers(
    request: Request,
    metric: str = Query("occupancy", regex="^(occupancy|reliability|data_quality|fire_safety)$"),
    top_n: int = Query(10, ge=1, le=50),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Top performers and cameras needing attention
    
    Chart: Best/Worst Performers (Ranked List)
    Returns: Cameras ranked by selected metric
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND sr.date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND sr.date <= ${len(params)}"
        
        # Different queries based on metric
        if metric == "occupancy":
            order_col = "avg_occupancy"
            query = f"""
                SELECT
                    sr.stream_id,
                    sr.camera_name,
                    sr.location,
                    AVG(sr.person_count) AS avg_occupancy,
                    MAX(sr.person_count) AS peak_occupancy,
                    COUNT(*) AS observations
                FROM stream_results sr
                WHERE sr.workspace_id = $1
                  AND sr.person_count IS NOT NULL
                  {date_filter}
                GROUP BY sr.stream_id, sr.camera_name, sr.location
                ORDER BY avg_occupancy DESC
            """
        elif metric == "reliability":
            order_col = "uptime_percentage"
            query = f"""
                SELECT
                    sr.stream_id,
                    sr.camera_name,
                    sr.location,
                    COUNT(DISTINCT sr.date) AS active_days,
                    COUNT(*) AS observations,
                    ROUND((COUNT(DISTINCT sr.date)::NUMERIC / 
                           NULLIF(EXTRACT(DAY FROM (MAX(sr.date) - MIN(sr.date))) + 1, 0) * 100)::numeric, 2) AS uptime_percentage
                FROM stream_results sr
                WHERE sr.workspace_id = $1
                  {date_filter}
                GROUP BY sr.stream_id, sr.camera_name, sr.location
                ORDER BY uptime_percentage DESC
            """
        elif metric == "data_quality":
            order_col = "data_quality_score"
            query = f"""
                SELECT
                    sr.stream_id,
                    sr.camera_name,
                    sr.location,
                    COUNT(*) AS total_records,
                    ROUND((COUNT(CASE WHEN sr.person_count IS NOT NULL THEN 1 END)::NUMERIC / COUNT(*) * 100)::numeric, 2) AS data_quality_score,
                    COUNT(CASE WHEN sr.person_count IS NULL THEN 1 END) AS null_records
                FROM stream_results sr
                WHERE sr.workspace_id = $1
                  {date_filter}
                GROUP BY sr.stream_id, sr.camera_name, sr.location
                ORDER BY data_quality_score DESC
            """
        else:  # fire_safety
            order_col = "fire_detection_rate"
            query = f"""
                SELECT
                    sr.stream_id,
                    sr.camera_name,
                    sr.location,
                    COUNT(*) AS total_checks,
                    COUNT(CASE WHEN sr.fire_status != 'no detection' THEN 1 END) AS fire_detections,
                    ROUND((COUNT(CASE WHEN sr.fire_status != 'no detection' THEN 1 END)::NUMERIC / COUNT(*) * 100)::numeric, 2) AS fire_detection_rate
                FROM stream_results sr
                WHERE sr.workspace_id = $1
                  {date_filter}
                GROUP BY sr.stream_id, sr.camera_name, sr.location
                ORDER BY fire_detection_rate ASC
            """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        # Get top and bottom performers
        top_performers = []
        worst_performers = []
        
        for i, row in enumerate(results):
            row_dict = dict(row)
            row_dict['stream_id'] = str(row_dict['stream_id'])
            row_dict['rank'] = i + 1
            
            # Convert numeric fields to float
            for key, value in row_dict.items():
                if isinstance(value, type(None)):
                    continue
                if key not in ['stream_id', 'camera_name', 'location', 'rank', 'total_records', 'observations', 'null_records', 'active_days', 'total_checks', 'fire_detections', 'peak_occupancy']:
                    try:
                        row_dict[key] = float(value)
                    except:
                        pass
            
            if i < top_n:
                top_performers.append(row_dict)
            if i >= len(results) - top_n:
                worst_performers.append(row_dict)
        
        return {
            "success": True,
            "metric": metric,
            "top_n": top_n,
            "top_performers": top_performers,
            "worst_performers": list(reversed(worst_performers))
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in best/worst performers: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# ==================== ADVANCED STATISTICAL ANALYTICS ====================

@router.get("/statistics/correlation-analysis")
async def get_correlation_analysis(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Correlation between different locations
    
    Chart: Correlation Matrix (Heatmap)
    Returns: Correlation coefficients between cameras
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        # Get time-series data for all cameras
        query = f"""
            WITH time_series AS (
                SELECT
                    DATE_TRUNC('hour', timestamp) AS time_bucket,
                    camera_name,
                    AVG(person_count) AS avg_count
                FROM stream_results
                WHERE workspace_id = $1
                  AND person_count IS NOT NULL
                  {date_filter}
                GROUP BY DATE_TRUNC('hour', timestamp), camera_name
            ),
            camera_pairs AS (
                SELECT DISTINCT
                    a.camera_name AS camera_a,
                    b.camera_name AS camera_b
                FROM time_series a
                CROSS JOIN time_series b
                WHERE a.camera_name < b.camera_name
            )
            SELECT
                cp.camera_a,
                cp.camera_b,
                ROUND(CORR(a.avg_count, b.avg_count)::numeric, 3) AS correlation,
                COUNT(*) AS sample_size
            FROM camera_pairs cp
            JOIN time_series a ON cp.camera_a = a.camera_name
            JOIN time_series b ON cp.camera_b = b.camera_name AND a.time_bucket = b.time_bucket
            GROUP BY cp.camera_a, cp.camera_b
            HAVING COUNT(*) >= 10
            ORDER BY ABS(CORR(a.avg_count, b.avg_count)) DESC
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            'camera_a': row['camera_a'],
            'camera_b': row['camera_b'],
            'correlation': float(row['correlation']) if row['correlation'] else 0,
            'sample_size': row['sample_size'],
            'correlation_strength': (
                'Strong Positive' if row['correlation'] and float(row['correlation']) > 0.7 else
                'Moderate Positive' if row['correlation'] and float(row['correlation']) > 0.4 else
                'Weak Positive' if row['correlation'] and float(row['correlation']) > 0 else
                'Weak Negative' if row['correlation'] and float(row['correlation']) > -0.4 else
                'Moderate Negative' if row['correlation'] and float(row['correlation']) > -0.7 else
                'Strong Negative'
            )
        } for row in results]
        
        return {
            "success": True,
            "pairs_analyzed": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in correlation analysis: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/statistics/variance-analysis")
async def get_variance_analysis(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Occupancy variance and consistency scores
    
    Chart: Variance Analysis (Box Plot)
    Returns: Statistical variance metrics for cameras
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        query = f"""
            SELECT
                stream_id,
                camera_name,
                location,
                COUNT(*) AS observations,
                ROUND(AVG(person_count)::numeric, 2) AS mean_occupancy,
                ROUND(STDDEV(person_count)::numeric, 2) AS std_deviation,
                ROUND(VARIANCE(person_count)::numeric, 2) AS variance,
                ROUND((STDDEV(person_count) / NULLIF(AVG(person_count), 0))::numeric, 2) AS coefficient_of_variation,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY person_count) AS percentile_25,
                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY person_count) AS median,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY person_count) AS percentile_75,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY person_count) AS percentile_95,
                MIN(person_count) AS min_occupancy,
                MAX(person_count) AS max_occupancy,
                CASE 
                    WHEN STDDEV(person_count) / NULLIF(AVG(person_count), 0) < 0.3 THEN 'Very Consistent'
                    WHEN STDDEV(person_count) / NULLIF(AVG(person_count), 0) < 0.5 THEN 'Consistent'
                    WHEN STDDEV(person_count) / NULLIF(AVG(person_count), 0) < 0.8 THEN 'Moderate Variability'
                    ELSE 'High Variability'
                END AS consistency_rating
            FROM stream_results
            WHERE workspace_id = $1
              AND person_count IS NOT NULL
              {date_filter}
            GROUP BY stream_id, camera_name, location
            HAVING COUNT(*) >= 10
            ORDER BY coefficient_of_variation ASC
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            'stream_id': str(row['stream_id']),
            'camera_name': row['camera_name'],
            'location': row['location'],
            'observations': row['observations'],
            'mean_occupancy': float(row['mean_occupancy']) if row['mean_occupancy'] else 0,
            'std_deviation': float(row['std_deviation']) if row['std_deviation'] else 0,
            'variance': float(row['variance']) if row['variance'] else 0,
            'coefficient_of_variation': float(row['coefficient_of_variation']) if row['coefficient_of_variation'] else 0,
            'percentile_25': float(row['percentile_25']) if row['percentile_25'] else 0,
            'median': float(row['median']) if row['median'] else 0,
            'percentile_75': float(row['percentile_75']) if row['percentile_75'] else 0,
            'percentile_95': float(row['percentile_95']) if row['percentile_95'] else 0,
            'min_occupancy': row['min_occupancy'],
            'max_occupancy': row['max_occupancy'],
            'consistency_rating': row['consistency_rating']
        } for row in results]
        
        return {
            "success": True,
            "cameras_analyzed": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in variance analysis: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# ==================== REAL-TIME ANALYTICS ====================

@router.get("/realtime/current-occupancy")
async def get_current_occupancy_dashboard(
    request: Request,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Live view of all camera current counts
    
    Chart: Real-time Occupancy Dashboard
    Returns: Most recent data from each active camera
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        query = """
            WITH latest_data AS (
                SELECT DISTINCT ON (sr.stream_id)
                    sr.stream_id,
                    sr.camera_name,
                    sr.location,
                    sr.building,
                    sr.floor_level,
                    sr.zone,
                    sr.timestamp,
                    sr.person_count,
                    sr.male_count,
                    sr.female_count,
                    sr.fire_status,
                    vs.count_threshold_greater,
                    vs.count_threshold_less,
                    vs.alert_enabled,
                    vs.status AS camera_status,
                    vs.is_streaming
                FROM stream_results sr
                JOIN video_stream vs ON sr.stream_id = vs.stream_id
                WHERE sr.workspace_id = $1
                ORDER BY sr.stream_id, sr.timestamp DESC
            )
            SELECT
                stream_id,
                camera_name,
                location,
                building,
                floor_level,
                zone,
                timestamp,
                person_count,
                male_count,
                female_count,
                fire_status,
                count_threshold_greater,
                count_threshold_less,
                alert_enabled,
                camera_status,
                is_streaming,
                EXTRACT(EPOCH FROM (NOW() - timestamp)) / 60 AS minutes_since_last_update,
                CASE 
                    WHEN count_threshold_greater IS NOT NULL AND person_count > count_threshold_greater THEN 'ALERT: Above Max'
                    WHEN count_threshold_less IS NOT NULL AND person_count < count_threshold_less THEN 'ALERT: Below Min'
                    WHEN fire_status != 'no detection' THEN 'ALERT: Fire Detected'
                    ELSE 'Normal'
                END AS current_status
            FROM latest_data
            WHERE timestamp >= NOW() - INTERVAL '15 minutes'
            ORDER BY timestamp DESC
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, workspace_id_obj)
            
        data = [{
            'stream_id': str(row['stream_id']),
            'camera_name': row['camera_name'],
            'location': row['location'],
            'building': row['building'],
            'floor_level': row['floor_level'],
            'zone': row['zone'],
            'timestamp': row['timestamp'].isoformat(),
            'person_count': row['person_count'],
            'male_count': row['male_count'],
            'female_count': row['female_count'],
            'fire_status': row['fire_status'],
            'max_threshold': row['count_threshold_greater'],
            'min_threshold': row['count_threshold_less'],
            'alert_enabled': row['alert_enabled'],
            'camera_status': row['camera_status'],
            'is_streaming': row['is_streaming'],
            'minutes_since_last_update': float(row['minutes_since_last_update']) if row['minutes_since_last_update'] else 0,
            'current_status': row['current_status'],
            'is_stale': float(row['minutes_since_last_update']) > 5 if row['minutes_since_last_update'] else False
        } for row in results]
        
        # Summary stats
        total_current_occupancy = sum(d['person_count'] for d in data if d['person_count'])
        cameras_in_alert = len([d for d in data if 'ALERT' in d['current_status']])
        
        return {
            "success": True,
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "active_cameras": len(data),
                "total_current_occupancy": total_current_occupancy,
                "cameras_in_alert": cameras_in_alert,
                "cameras_with_fire": len([d for d in data if d['fire_status'] != 'no detection'])
            },
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in current occupancy dashboard: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/realtime/live-alerts-feed")
async def get_live_alerts_feed(
    request: Request,
    minutes_back: int = Query(60, ge=1, le=1440),
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Stream of recent threshold breaches and alerts
    
    Chart: Live Alerts Timeline
    Returns: Recent alert events
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        query = """
            WITH alert_events AS (
                SELECT
                    sr.result_id,
                    sr.stream_id,
                    sr.camera_name,
                    sr.location,
                    sr.timestamp,
                    sr.person_count,
                    sr.fire_status,
                    vs.count_threshold_greater,
                    vs.count_threshold_less,
                    CASE 
                        WHEN sr.fire_status != 'no detection' THEN 'Fire Detection'
                        WHEN vs.count_threshold_greater IS NOT NULL AND sr.person_count > vs.count_threshold_greater THEN 'Occupancy Over Limit'
                        WHEN vs.count_threshold_less IS NOT NULL AND sr.person_count < vs.count_threshold_less THEN 'Occupancy Under Limit'
                        ELSE NULL
                    END AS alert_type,
                    CASE 
                        WHEN sr.fire_status = 'fire' THEN 'critical'
                        WHEN sr.fire_status = 'smoke' THEN 'high'
                        WHEN vs.count_threshold_greater IS NOT NULL AND sr.person_count > vs.count_threshold_greater * 1.2 THEN 'high'
                        ELSE 'medium'
                    END AS severity
                FROM stream_results sr
                JOIN video_stream vs ON sr.stream_id = vs.stream_id
                WHERE sr.workspace_id = $1
                  AND sr.timestamp >= NOW() - ($2 || ' minutes')::INTERVAL
                  AND vs.alert_enabled = TRUE
                  AND (
                      sr.fire_status != 'no detection'
                      OR (vs.count_threshold_greater IS NOT NULL AND sr.person_count > vs.count_threshold_greater)
                      OR (vs.count_threshold_less IS NOT NULL AND sr.person_count < vs.count_threshold_less)
                  )
            )
            SELECT
                result_id,
                stream_id,
                camera_name,
                location,
                timestamp,
                person_count,
                fire_status,
                count_threshold_greater,
                count_threshold_less,
                alert_type,
                severity
            FROM alert_events
            ORDER BY timestamp DESC, severity DESC
            LIMIT 500
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, workspace_id_obj, minutes_back)
            
        data = [{
            'result_id': str(row['result_id']),
            'stream_id': str(row['stream_id']),
            'camera_name': row['camera_name'],
            'location': row['location'],
            'timestamp': row['timestamp'].isoformat(),
            'person_count': row['person_count'],
            'fire_status': row['fire_status'],
            'max_threshold': row['count_threshold_greater'],
            'min_threshold': row['count_threshold_less'],
            'alert_type': row['alert_type'],
            'severity': row['severity']
        } for row in results]
        
        return {
            "success": True,
            "minutes_back": minutes_back,
            "total_alerts": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in live alerts feed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# ==================== ALERT & NOTIFICATION ANALYTICS ====================

@router.get("/alerts/effectiveness-metrics")
async def get_alert_effectiveness_metrics(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Alert response times and effectiveness
    
    Chart: Alert Effectiveness Dashboard
    Returns: Alert performance metrics
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND n.timestamp::date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND n.timestamp::date <= ${len(params)}"
            
        query = f"""
            SELECT
                COUNT(*) AS total_notifications,
                COUNT(CASE WHEN n.is_read = TRUE THEN 1 END) AS read_notifications,
                COUNT(CASE WHEN n.is_read = FALSE THEN 1 END) AS unread_notifications,
                ROUND((COUNT(CASE WHEN n.is_read = TRUE THEN 1 END)::NUMERIC / NULLIF(COUNT(*), 0) * 100)::numeric, 2) AS read_rate,
                AVG(EXTRACT(EPOCH FROM (n.updated_at - n.timestamp)) / 60) AS avg_response_time_minutes,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (n.updated_at - n.timestamp)) / 60) AS median_response_time,
                COUNT(DISTINCT n.stream_id) AS cameras_with_alerts,
                COUNT(DISTINCT n.user_id) AS users_receiving_alerts,
                n.status AS alert_status,
                COUNT(*) AS alerts_by_status
            FROM notifications n
            WHERE n.workspace_id = $1
              {date_filter}
            GROUP BY n.status
            ORDER BY alerts_by_status DESC
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        # Overall summary
        total_notifs = sum(row['total_notifications'] for row in results)
        read_notifs = sum(row['read_notifications'] for row in results)
        
        data = [{
            'alert_status': row['alert_status'],
            'total_notifications': row['total_notifications'],
            'read_notifications': row['read_notifications'],
            'unread_notifications': row['unread_notifications'],
            'read_rate': float(row['read_rate']) if row['read_rate'] else 0,
            'avg_response_time_minutes': float(row['avg_response_time_minutes']) if row['avg_response_time_minutes'] else 0,
            'median_response_time': float(row['median_response_time']) if row['median_response_time'] else 0,
            'cameras_with_alerts': row['cameras_with_alerts'],
            'users_receiving_alerts': row['users_receiving_alerts']
        } for row in results]
        
        return {
            "success": True,
            "summary": {
                "total_notifications": total_notifs,
                "read_notifications": read_notifs,
                "unread_notifications": total_notifs - read_notifs,
                "overall_read_rate": round((read_notifs / total_notifs * 100), 2) if total_notifs > 0 else 0
            },
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in alert effectiveness: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# ==================== WORKSPACE & USER ANALYTICS ====================

@router.get("/workspace/activity-summary")
async def get_workspace_activity_summary(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    Workspace-wide activity metrics
    
    Chart: Workspace Dashboard
    Returns: Overall workspace statistics
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND date <= ${len(params)}"
            
        # Get comprehensive workspace stats
        query = f"""
            WITH workspace_stats AS (
                SELECT
                    COUNT(DISTINCT stream_id) AS total_cameras,
                    COUNT(*) AS total_records,
                    SUM(person_count) AS total_people_counted,
                    AVG(person_count) AS avg_occupancy,
                    MAX(person_count) AS peak_occupancy,
                    COUNT(DISTINCT date) AS active_days,
                    COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END) AS fire_incidents,
                    COUNT(DISTINCT location) AS unique_locations,
                    COUNT(DISTINCT building) AS unique_buildings
                FROM stream_results
                WHERE workspace_id = $1
                  {date_filter}
            ),
            camera_stats AS (
                SELECT
                    COUNT(*) AS configured_cameras,
                    COUNT(CASE WHEN status = 'active' THEN 1 END) AS active_cameras,
                    COUNT(CASE WHEN is_streaming = TRUE THEN 1 END) AS streaming_cameras,
                    COUNT(CASE WHEN alert_enabled = TRUE THEN 1 END) AS cameras_with_alerts
                FROM video_stream
                WHERE workspace_id = $1
            )
            SELECT
                ws.*,
                cs.configured_cameras,
                cs.active_cameras,
                cs.streaming_cameras,
                cs.cameras_with_alerts
            FROM workspace_stats ws
            CROSS JOIN camera_stats cs
        """
        
        async with db_manager.get_connection() as conn:
            result = await conn.fetchrow(query, *params)
            
        data = {
            'total_cameras': result['total_cameras'],
            'configured_cameras': result['configured_cameras'],
            'active_cameras': result['active_cameras'],
            'streaming_cameras': result['streaming_cameras'],
            'cameras_with_alerts': result['cameras_with_alerts'],
            'total_records': result['total_records'],
            'total_people_counted': result['total_people_counted'],
            'avg_occupancy': float(result['avg_occupancy']) if result['avg_occupancy'] else 0,
            'peak_occupancy': result['peak_occupancy'],
            'active_days': result['active_days'],
            'fire_incidents': result['fire_incidents'],
            'unique_locations': result['unique_locations'],
            'unique_buildings': result['unique_buildings'],
            'camera_utilization_rate': round((result['streaming_cameras'] / result['configured_cameras'] * 100), 2) if result['configured_cameras'] > 0 else 0,
            'data_coverage_rate': round((result['active_cameras'] / result['configured_cameras'] * 100), 2) if result['configured_cameras'] > 0 else 0
        }
        
        return {
            "success": True,
            "workspace_id": str(workspace_id_obj),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in workspace activity summary: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/workspace/user-engagement")
async def get_user_engagement_metrics(
    request: Request,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    User activity and engagement metrics
    
    Chart: User Engagement Dashboard
    Returns: User login patterns, camera configuration activity
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        params = [workspace_id_obj]
        date_filter = ""
        
        if start_date:
            params.append(start_date)
            date_filter += f" AND created_at::date >= ${len(params)}"
        if end_date:
            params.append(end_date)
            date_filter += f" AND created_at::date <= ${len(params)}"
            
        # Get user activity stats
        query = f"""
            WITH user_activity AS (
                SELECT
                    u.user_id,
                    u.username,
                    u.role,
                    u.last_login,
                    COUNT(DISTINCT vs.stream_id) AS cameras_owned,
                    COUNT(CASE WHEN vs.status = 'active' THEN 1 END) AS active_cameras,
                    COUNT(CASE WHEN vs.alert_enabled = TRUE THEN 1 END) AS cameras_with_alerts
                FROM users u
                LEFT JOIN video_stream vs ON u.user_id = vs.user_id AND vs.workspace_id = $1
                WHERE u.user_id IN (
                    SELECT user_id FROM workspace_members WHERE workspace_id = $1
                )
                GROUP BY u.user_id, u.username, u.role, u.last_login
            ),
            user_logs AS (
                SELECT
                    user_id,
                    COUNT(*) AS total_actions,
                    MAX(created_at) AS last_action
                FROM logs
                WHERE workspace_id = $1
                  {date_filter}
                GROUP BY user_id
            )
            SELECT
                ua.user_id,
                ua.username,
                ua.role,
                ua.last_login,
                ua.cameras_owned,
                ua.active_cameras,
                ua.cameras_with_alerts,
                COALESCE(ul.total_actions, 0) AS total_actions,
                ul.last_action,
                EXTRACT(EPOCH FROM (NOW() - ua.last_login)) / 86400 AS days_since_last_login
            FROM user_activity ua
            LEFT JOIN user_logs ul ON ua.user_id = ul.user_id
            ORDER BY ua.last_login DESC NULLS LAST
        """
        
        async with db_manager.get_connection() as conn:
            results = await conn.fetch(query, *params)
            
        data = [{
            'user_id': str(row['user_id']),
            'username': row['username'],
            'role': row['role'],
            'last_login': row['last_login'].isoformat() if row['last_login'] else None,
            'cameras_owned': row['cameras_owned'],
            'active_cameras': row['active_cameras'],
            'cameras_with_alerts': row['cameras_with_alerts'],
            'total_actions': row['total_actions'],
            'last_action': row['last_action'].isoformat() if row['last_action'] else None,
            'days_since_last_login': float(row['days_since_last_login']) if row['days_since_last_login'] else None,
            'engagement_level': (
                'High' if row['total_actions'] and row['total_actions'] > 100 else
                'Medium' if row['total_actions'] and row['total_actions'] > 20 else
                'Low'
            )
        } for row in results]
        
        return {
            "success": True,
            "total_users": len(data),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in user engagement: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# ==================== EXECUTIVE SUMMARY ====================

@router.get("/executive/summary")
async def get_executive_summary(
    request: Request,
    current_user_data: Dict = Depends(session_manager.get_current_user_full_data_dependency)
):
    """
    High-level KPIs for management
    
    Chart: Executive Dashboard
    Returns: Key metrics with trend indicators
    """
    user_id_obj = current_user_data["user_id"]
    username = current_user_data["username"]
    
    try:
        _, workspace_id_obj = await workspace_service.get_user_and_workspace(username)
        if not workspace_id_obj:
            raise HTTPException(status_code=400, detail="No active workspace")

        await check_workspace_access(db_manager, user_id_obj, workspace_id_obj, required_role=None)
        
        # Get comprehensive executive summary
        query = """
            WITH current_month AS (
                SELECT
                    AVG(person_count) AS avg_occupancy,
                    SUM(person_count) AS total_visits,
                    COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END) AS fire_incidents,
                    COUNT(DISTINCT stream_id) AS active_cameras
                FROM stream_results
                WHERE workspace_id = $1
                  AND date >= DATE_TRUNC('month', CURRENT_DATE)
            ),
            previous_month AS (
                SELECT
                    AVG(person_count) AS avg_occupancy,
                    SUM(person_count) AS total_visits,
                    COUNT(CASE WHEN fire_status != 'no detection' THEN 1 END) AS fire_incidents
                FROM stream_results
                WHERE workspace_id = $1
                  AND date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
                  AND date < DATE_TRUNC('month', CURRENT_DATE)
            ),
            camera_health AS (
                SELECT
                    COUNT(*) AS total_cameras,
                    COUNT(CASE WHEN status = 'active' THEN 1 END) AS healthy_cameras,
                    COUNT(CASE WHEN status = 'error' THEN 1 END) AS error_cameras,
                    COUNT(CASE WHEN alert_enabled = TRUE THEN 1 END) AS monitored_cameras
                FROM video_stream
                WHERE workspace_id = $1
            ),
            alert_summary AS (
                SELECT
                    COUNT(*) AS total_alerts,
                    COUNT(CASE WHEN is_read = TRUE THEN 1 END) AS resolved_alerts
                FROM notifications
                WHERE workspace_id = $1
                  AND timestamp >= DATE_TRUNC('month', CURRENT_DATE)
            )
            SELECT
                cm.avg_occupancy AS current_avg_occupancy,
                pm.avg_occupancy AS previous_avg_occupancy,
                ROUND(((cm.avg_occupancy - pm.avg_occupancy) / NULLIF(pm.avg_occupancy, 0) * 100)::numeric, 2) AS occupancy_trend,
                cm.total_visits AS current_total_visits,
                pm.total_visits AS previous_total_visits,
                cm.fire_incidents AS current_fire_incidents,
                pm.fire_incidents AS previous_fire_incidents,
                cm.active_cameras,
                ch.total_cameras,
                ch.healthy_cameras,
                ch.error_cameras,
                ch.monitored_cameras,
                ROUND((ch.healthy_cameras::NUMERIC / NULLIF(ch.total_cameras, 0) * 100)::numeric, 2) AS system_health_percentage,
                als.total_alerts,
                als.resolved_alerts,
                ROUND((als.resolved_alerts::NUMERIC / NULLIF(als.total_alerts, 0) * 100)::numeric, 2) AS alert_resolution_rate
            FROM current_month cm
            CROSS JOIN previous_month pm
            CROSS JOIN camera_health ch
            CROSS JOIN alert_summary als
        """
        
        async with db_manager.get_connection() as conn:
            result = await conn.fetchrow(query, workspace_id_obj)
            
        data = {
            'occupancy_metrics': {
                'current_avg': float(result['current_avg_occupancy']) if result['current_avg_occupancy'] else 0,
                'previous_avg': float(result['previous_avg_occupancy']) if result['previous_avg_occupancy'] else 0,
                'trend_percentage': float(result['occupancy_trend']) if result['occupancy_trend'] else 0,
                'trend_direction': 'up' if result['occupancy_trend'] and float(result['occupancy_trend']) > 0 else 'down' if result['occupancy_trend'] and float(result['occupancy_trend']) < 0 else 'stable',
                'current_total_visits': result['current_total_visits'],
                'previous_total_visits': result['previous_total_visits']
            },
            'safety_metrics': {
                'fire_incidents_current_month': result['current_fire_incidents'],
                'fire_incidents_previous_month': result['previous_fire_incidents'],
                'fire_incident_trend': 'improved' if result['current_fire_incidents'] < result['previous_fire_incidents'] else 'worsened' if result['current_fire_incidents'] > result['previous_fire_incidents'] else 'stable'
            },
            'system_health': {
                'total_cameras': result['total_cameras'],
                'healthy_cameras': result['healthy_cameras'],
                'error_cameras': result['error_cameras'],
                'active_cameras': result['active_cameras'],
                'monitored_cameras': result['monitored_cameras'],
                'health_percentage': float(result['system_health_percentage']) if result['system_health_percentage'] else 0,
                'status': 'excellent' if result['system_health_percentage'] and float(result['system_health_percentage']) > 95 else 'good' if result['system_health_percentage'] and float(result['system_health_percentage']) > 80 else 'needs attention'
            },
            'alert_performance': {
                'total_alerts': result['total_alerts'],
                'resolved_alerts': result['resolved_alerts'],
                'resolution_rate': float(result['alert_resolution_rate']) if result['alert_resolution_rate'] else 0,
                'performance': 'excellent' if result['alert_resolution_rate'] and float(result['alert_resolution_rate']) > 90 else 'good' if result['alert_resolution_rate'] and float(result['alert_resolution_rate']) > 70 else 'needs improvement'
            }
        }
        
        return {
            "success": True,
            "generated_at": datetime.now().isoformat(),
            "data": data
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in executive summary: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
