# app/api/monitoring.py - NEW FILE

from fastapi import APIRouter, Depends
from typing import Dict, Any
from app.services.stream_service import stream_manager
from app.services.distributed_stream_manager import distributed_stream_manager
from app.services.shared_stream_service import video_file_manager

router = APIRouter(prefix="/monitoring", tags=["monitoring"])


@router.get("/camera-health")
async def get_camera_health_report():
    """
    Get detailed health report for all cameras.
    
    Returns:
    - Overall system health
    - Per-camera health scores
    - Zombie detection results
    - Performance metrics
    """
    try:
        # Get health report from stream manager
        health_report = await stream_manager.get_camera_health_report()
        
        # Get zombie detection results
        zombies = await distributed_stream_manager.detect_and_cleanup_zombie_streams()
        
        # Get shared stream stats
        shared_stream_stats = await video_file_manager.get_all_stats()
        
        # Calculate system health
        total_cameras = health_report['total_cameras']
        healthy = health_report['healthy']
        degraded = health_report['degraded']
        critical = health_report['critical']
        
        system_health_score = 0
        if total_cameras > 0:
            system_health_score = (
                (healthy * 100 + degraded * 50 + critical * 0) / total_cameras
            )
        
        return {
            'timestamp': health_report['timestamp'],
            'system': {
                'health_score': round(system_health_score, 1),
                'status': (
                    'healthy' if system_health_score > 75 else
                    'degraded' if system_health_score > 50 else
                    'critical'
                ),
                'total_cameras': total_cameras,
                'healthy_cameras': healthy,
                'degraded_cameras': degraded,
                'critical_cameras': critical,
                'zombie_cameras': len(zombies)
            },
            'cameras': health_report['cameras'],
            'zombies': zombies,
            'shared_streams': shared_stream_stats
        }
    
    except Exception as e:
        return {
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }


@router.get("/stream-diagnostics/{stream_id}")
async def get_stream_diagnostics(stream_id: str):
    """
    Get detailed diagnostics for a specific stream.
    
    Helps debug why a camera stopped or isn't working.
    """
    try:
        from uuid import UUID
        from app.services.video_stream_service import video_stream_service
        
        stream_id_uuid = UUID(stream_id)
        
        # Get database state
        db_state = await video_stream_service.get_video_stream_by_id(stream_id_uuid)
        
        if not db_state:
            return {'error': 'Camera not found'}
        
        # Get memory state
        async with stream_manager._lock:
            mem_state = stream_manager.active_streams.get(stream_id)
        
        # Get shared stream state
        shared_stream_state = None
        if db_state.get('path'):
            try:
                shared_stream = await video_file_manager.get_shared_stream(
                    db_state['path']
                )
                shared_stream_state = await shared_stream.get_stats()
            except:
                pass
        
        # Get error history
        error_history = stream_manager.stream_errors.get(stream_id, [])
        
        # Get processing stats
        processing_stats = stream_manager.stream_processing_stats.get(stream_id, {})
        
        # Determine issues
        issues = []
        recommendations = []
        
        # Check database vs memory mismatch
        if db_state['is_streaming'] and not mem_state:
            issues.append('Camera should be running but not in memory')
            recommendations.append('Camera will auto-restart within 30 seconds')
        elif not db_state['is_streaming'] and mem_state:
            issues.append('Camera in memory but database says not streaming')
            recommendations.append('Memory state will be cleaned up')
        
        # Check for stale frames
        if mem_state:
            last_frame_time = mem_state.get('last_frame_time')
            if last_frame_time:
                age = (datetime.now(ZoneInfo("Africa/Cairo")) - last_frame_time).total_seconds()
                if age > 120:
                    issues.append(f'No frames for {age:.0f} seconds')
                    recommendations.append('Camera may be frozen - will auto-recover')
        
        # Check for errors
        if len(error_history) > 5:
            issues.append(f'High error count: {len(error_history)} errors')
            recent_errors = [e['error'] for e in error_history[-5:]]
            recommendations.append(f'Recent errors: {", ".join(set(recent_errors))}')
        
        # Check shared stream health
        if shared_stream_state:
            if shared_stream_state.get('state') == 'failed':
                issues.append('Shared stream failed')
                recommendations.append('Check camera connection and credentials')
        
        return {
            'stream_id': stream_id,
            'camera_name': db_state['name'],
            'database': {
                'is_streaming': db_state['is_streaming'],
                'status': db_state['status'],
                'stop_reason': db_state.get('stop_reason'),
                'locked_by_server': str(db_state.get('locked_by_server')) if db_state.get('locked_by_server') else None,
                'retry_count': db_state.get('retry_count', 0),
                'next_retry_at': db_state.get('next_retry_at').isoformat() if db_state.get('next_retry_at') else None
            },
            'memory': {
                'active': mem_state is not None,
                'status': mem_state.get('status') if mem_state else None,
                'last_frame_time': mem_state.get('last_frame_time').isoformat() if mem_state and mem_state.get('last_frame_time') else None,
                'frame_age_seconds': (
                    (datetime.now(ZoneInfo("Africa/Cairo")) - mem_state['last_frame_time']).total_seconds()
                    if mem_state and mem_state.get('last_frame_time') else None
                )
            },
            'shared_stream': shared_stream_state,
            'processing_stats': processing_stats,
            'error_history': error_history[-10:],  # Last 10 errors
            'issues': issues,
            'recommendations': recommendations,
            'timestamp': datetime.now(ZoneInfo("Africa/Cairo")).isoformat()
        }
    
    except Exception as e:
        return {
            'error': str(e),
            'stream_id': stream_id,
            'timestamp': datetime.now().isoformat()
        }


@router.post("/force-recovery/{stream_id}")
async def force_camera_recovery(stream_id: str):
    """
    Manually trigger recovery for a stuck camera.
    """
    try:
        from uuid import UUID
        from app.services.retry_service import retry_service
        
        stream_id_uuid = UUID(stream_id)
        
        # Force schedule retry
        await retry_service.schedule_retry(
            stream_id=stream_id_uuid,
            stop_reason='manual_recovery',
            current_retry_count=0,
            error_context='User requested manual recovery',
            immediate=True  # Start immediately
        )
        
        return {
            'success': True,
            'message': f'Recovery scheduled for camera {stream_id}',
            'timestamp': datetime.now().isoformat()
        }
    
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }