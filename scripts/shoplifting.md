Shoplifting Feature Integration Walkthrough
The Shoplifting Detection feature has now been fully integrated into the backend video analytics pipeline!

Here is a summary of the work completed and the components that were modified to support "full video" continuous processing for shoplifting.

What Was Completed
1. Database and Schemas
The SQL Schema script (scripts/apply_shoplifting_schema.sql) contains tables and triggers to capture real-time frames and aggregate them into shoplifting_events. You have already handled the execution of this script against the remote PostgreSQL instance.
Pydantic Models have been defined in app/schemas/shoplifting_schema.py to structure the API event requests, dashboard summaries, and event responses.
2. Services and Data Access
Created ShopliftingService (app/services/shoplifting_service.py) to manage asynchronous execution using the application's db_manager.
Functions such as get_events, resolve_event, get_active_dashboard, and get_daily_summary are now available.
Created insert_surveillance_frame to funnel raw GPU/CPU AI detections into the database, automating the creation of shoplifting_events via PostgreSQL triggers.
3. API Endpoints
Developed and registered the new shoplifting_router (app/api/routes/shoplifting_router.py), exposing the necessary endpoints to manage events in the system. The router is now secured and restricts data retrieval specifically to the user's active workspace. This has been hooked into the main application.
4. Continuous Video Analysis AI Engine
To satisfy the requirement of "full video" monitoring, major updates were applied to the StreamProcessingService (app/services/stream_processing_service.py):

Added configuration options for shoplifting AI model endpoints (ONNX, PyTorch, TensorRT, OpenVINO) inside app/config/settings.py so the correct backend weights can be loaded.
Modified the inference loop detect_objects_with_threshold so every consecutive frame dynamically evaluates for immediate threat triggers (shoplifting behavior).
AI confidence scores, bounding boxes, and object detection alerts are now rendered natively on the annotated video streams.
Any resulting shoplifting behavior observations are buffered directly into the PostgreSQL Database dynamically!
TIP

Next Steps for You:

Ensure you place the actual AI Model weights (e.g. shoplifting.onnx or shoplifting.pt) into your base models/ directory alongside people/gender AI models so ModelFactory can properly load and utilize them.
Once the models are in place and the remote Database has the schema up to date, you merely need to restart the API service container to kickstart the full pipeline!