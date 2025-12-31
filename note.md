insighteye_app  | 2025-12-31 07:04:44,655 [INFO] uvicorn.access: 127.0.0.1:33662 - "GET /health HTTP/1.1" 200
insighteye_app  | 2025-12-31 07:04:45,841 [INFO] app.services.stream_service_3: ✅ User 7b1bb653-9686-4540-a4b9-ad11f772cccb requested start for stream 688b1cb1-0ce3-473c-9b5d-8d1d4631c1b6
insighteye_app  | 2025-12-31 07:04:45,841 [INFO] app.services.stream_service_3: 🎬 START REQUEST: stream=688b1cb1-0ce3-473c-9b5d-8d1d4631c1b6, camera=Test 1, workspace=f14766ad-a36e-47e1-9c54-ed9fa6a14a00, source=./videos/fire1.mp4
insighteye_app  | 2025-12-31 07:04:45,848 [INFO] app.services.stream_service_3: ✅ QUOTA CHECK PASSED for stream 688b1cb1-0ce3-473c-9b5d-8d1d4631c1b6
insighteye_app  | 2025-12-31 07:04:45,849 [INFO] app.services.stream_service_3: 🔄 Starting fresh stream instance for 688b1cb1-0ce3-473c-9b5d-8d1d4631c1b6
insighteye_app  | 2025-12-31 07:04:45,849 [INFO] app.services.stream_service_3: ✅ Registered stream in workspace registry
insighteye_app  | 2025-12-31 07:04:45,849 [INFO] app.services.stream_service_3: ✅ Initialized stream entry in active_streams
insighteye_app  | 2025-12-31 07:04:45,849 [INFO] app.services.stream_service_3: ✅ Initialized fire detection state
insighteye_app  | 2025-12-31 07:04:45,849 [INFO] app.services.stream_service_3: 💾 Updating database: is_streaming=TRUE, clearing stop_reason
insighteye_app  | 2025-12-31 07:04:45,852 [INFO] app.services.stream_service_3: ✅ Database updated successfully
insighteye_app  | 2025-12-31 07:04:45,854 [INFO] app.services.stream_service_3: 📊 Database verification: is_streaming=True, status=processing, stop_reason=None
insighteye_app  | 2025-12-31 07:04:45,855 [INFO] app.services.stream_service_3: ✅ Initialized processing stats
insighteye_app  | 2025-12-31 07:04:45,855 [INFO] app.services.stream_service_3: 🗄️ Ensuring Qdrant collection for workspace f14766ad-a36e-47e1-9c54-ed9fa6a14a00
insighteye_app  | 2025-12-31 07:04:45,855 [INFO] app.services.stream_service_3: ✅ Qdrant collection verified
insighteye_app  | 2025-12-31 07:04:45,855 [INFO] app.services.stream_service_3: ✅ Created stop event
insighteye_app  | 2025-12-31 07:04:45,855 [INFO] app.services.stream_service_3: ✅ Processing task created: process_stream_688b1cb1-0ce3-473c-9b5d-8d1d4631c1b6
insighteye_app  | 2025-12-31 07:04:45,855 [INFO] app.services.stream_service_3: ✅ Registered with shared stream manager
insighteye_app  | 2025-12-31 07:04:45,855 [INFO] app.services.stream_service_3: ✅ Transitioned to ACTIVE state
insighteye_app  | 2025-12-31 07:04:45,856 [INFO] app.services.stream_processing_service: Starting stream processing for 688b1cb1-0ce3-473c-9b5d-8d1d4631c1b6 (Test 1)
insighteye_app  | 2025-12-31 07:04:45,863 [INFO] app.services.stream_service_3: ✅ Workspace members notified
insighteye_app  | 2025-12-31 07:04:45,864 [INFO] app.services.stream_service_3: ✅ ✅ ✅ Stream 688b1cb1-0ce3-473c-9b5d-8d1d4631c1b6 SUCCESSFULLY STARTED
insighteye_app  | 2025-12-31 07:04:45,864 [INFO] app.services.stream_service_3: ✅ Stream 688b1cb1-0ce3-473c-9b5d-8d1d4631c1b6 started in workspace f14766ad-a36e-47e1-9c54-ed9fa6a14a00
insighteye_app  | 2025-12-31 07:04:45,864 [INFO] app.api.routes.stream_router_3: Started stream 688b1cb1-0ce3-473c-9b5d-8d1d4631c1b6 (Test 1) in workspace f14766ad-a36e-47e1-9c54-ed9fa6a14a00
insighteye_app  | 2025-12-31 07:04:45,874 [INFO] app.services.stream_processing_service: Stream 688b1cb1-0ce3-473c-9b5d-8d1d4631c1b6 parameters: frame_skip=30, frame_delay=0.0, conf=0.4000000059604645
insighteye_app  | 2025-12-31 07:04:45,982 [INFO] app.services.stream_service_3: ✅ User 7b1bb653-9686-4540-a4b9-ad11f772cccb requested start for stream 75b6794f-9fb1-4599-91ff-b373bc1aed9e
insighteye_app  | 2025-12-31 07:04:45,982 [INFO] app.services.stream_service_3: 🎬 START REQUEST: stream=75b6794f-9fb1-4599-91ff-b373bc1aed9e, camera=Test 2, workspace=f14766ad-a36e-47e1-9c54-ed9fa6a14a00, source=./videos/5.mp4
insighteye_app  | 2025-12-31 07:04:45,988 [INFO] app.services.stream_service_3: ✅ QUOTA CHECK PASSED for stream 75b6794f-9fb1-4599-91ff-b373bc1aed9e
insighteye_app  | 2025-12-31 07:04:45,989 [INFO] app.services.stream_service_3: 🔄 Starting fresh stream instance for 75b6794f-9fb1-4599-91ff-b373bc1aed9e
insighteye_app  | 2025-12-31 07:04:45,989 [INFO] app.services.stream_service_3: ✅ Registered stream in workspace registry
insighteye_app  | 2025-12-31 07:04:45,989 [INFO] app.services.stream_service_3: ✅ Initialized stream entry in active_streams
insighteye_app  | 2025-12-31 07:04:45,989 [INFO] app.services.stream_service_3: ✅ Initialized fire detection state
insighteye_app  | 2025-12-31 07:04:45,989 [INFO] app.services.stream_service_3: 💾 Updating database: is_streaming=TRUE, clearing stop_reason
insighteye_app  | 2025-12-31 07:04:45,991 [INFO] app.services.stream_service_3: ✅ Database updated successfully
insighteye_app  | 2025-12-31 07:04:45,993 [INFO] app.services.stream_service_3: 📊 Database verification: is_streaming=True, status=processing, stop_reason=None
insighteye_app  | 2025-12-31 07:04:45,993 [INFO] app.services.stream_service_3: ✅ Initialized processing stats
insighteye_app  | 2025-12-31 07:04:45,993 [INFO] app.services.stream_service_3: 🗄️ Ensuring Qdrant collection for workspace f14766ad-a36e-47e1-9c54-ed9fa6a14a00
insighteye_app  | 2025-12-31 07:04:45,994 [INFO] app.services.stream_service_3: ✅ Qdrant collection verified
insighteye_app  | 2025-12-31 07:04:45,994 [INFO] app.services.stream_service_3: ✅ Created stop event
insighteye_app  | 2025-12-31 07:04:45,994 [INFO] app.services.stream_service_3: ✅ Processing task created: process_stream_75b6794f-9fb1-4599-91ff-b373bc1aed9e
insighteye_app  | 2025-12-31 07:04:45,994 [INFO] app.services.stream_service_3: ✅ Registered with shared stream manager
insighteye_app  | 2025-12-31 07:04:45,995 [INFO] app.services.stream_service_3: ✅ Transitioned to ACTIVE state
insighteye_app  | 2025-12-31 07:04:45,995 [INFO] app.services.stream_processing_service: Starting stream processing for 75b6794f-9fb1-4599-91ff-b373bc1aed9e (Test 2)
insighteye_app  | 2025-12-31 07:04:46,059 [INFO] app.services.stream_processing_service: Stream 75b6794f-9fb1-4599-91ff-b373bc1aed9e parameters: frame_skip=30, frame_delay=0.0, conf=0.4000000059604645
insighteye_app  | 2025-12-31 07:04:46,059 [INFO] app.services.stream_service_3: ✅ Workspace members notified
insighteye_app  | 2025-12-31 07:04:46,059 [INFO] app.services.stream_service_3: ✅ ✅ ✅ Stream 75b6794f-9fb1-4599-91ff-b373bc1aed9e SUCCESSFULLY STARTED
insighteye_app  | 2025-12-31 07:04:46,060 [INFO] app.services.stream_service_3: ✅ Stream 75b6794f-9fb1-4599-91ff-b373bc1aed9e started in workspace f14766ad-a36e-47e1-9c54-ed9fa6a14a00
insighteye_app  | 2025-12-31 07:04:46,060 [INFO] app.api.routes.stream_router_3: Started stream 75b6794f-9fb1-4599-91ff-b373bc1aed9e (Test 2) in workspace f14766ad-a36e-47e1-9c54-ed9fa6a14a00
insighteye_app  | 2025-12-31 07:04:46,175 [INFO] app.services.stream_service_3: ✅ User 7b1bb653-9686-4540-a4b9-ad11f772cccb requested start for stream 7658cb9b-6225-45f8-8383-3869b5a1e4b9
insighteye_app  | 2025-12-31 07:04:46,175 [INFO] app.services.stream_service_3: 🎬 START REQUEST: stream=7658cb9b-6225-45f8-8383-3869b5a1e4b9, camera=Test 3, workspace=f14766ad-a36e-47e1-9c54-ed9fa6a14a00, source=./videos/5.mp4
insighteye_app  | 2025-12-31 07:04:46,180 [INFO] app.services.stream_service_3: ✅ QUOTA CHECK PASSED for stream 7658cb9b-6225-45f8-8383-3869b5a1e4b9
insighteye_app  | 2025-12-31 07:04:46,181 [INFO] app.services.stream_service_3: 🔄 Starting fresh stream instance for 7658cb9b-6225-45f8-8383-3869b5a1e4b9
insighteye_app  | 2025-12-31 07:04:46,181 [INFO] app.services.stream_service_3: ✅ Registered stream in workspace registry
insighteye_app  | 2025-12-31 07:04:46,181 [INFO] app.services.stream_service_3: ✅ Initialized stream entry in active_streams
insighteye_app  | 2025-12-31 07:04:46,181 [INFO] app.services.stream_service_3: ✅ Initialized fire detection state
insighteye_app  | 2025-12-31 07:04:46,182 [INFO] app.services.stream_service_3: 💾 Updating database: is_streaming=TRUE, clearing stop_reason
insighteye_app  | 2025-12-31 07:04:46,188 [INFO] app.services.stream_service_3: ✅ Database updated successfully
insighteye_app  | 2025-12-31 07:04:46,192 [INFO] app.services.stream_service_3: 📊 Database verification: is_streaming=True, status=processing, stop_reason=None
insighteye_app  | 2025-12-31 07:04:46,192 [INFO] app.services.stream_service_3: ✅ Initialized processing stats
insighteye_app  | 2025-12-31 07:04:46,193 [INFO] app.services.stream_service_3: 🗄️ Ensuring Qdrant collection for workspace f14766ad-a36e-47e1-9c54-ed9fa6a14a00
insighteye_app  | 2025-12-31 07:04:46,193 [INFO] app.services.stream_service_3: ✅ Qdrant collection verified
insighteye_app  | 2025-12-31 07:04:46,193 [INFO] app.services.stream_service_3: ✅ Created stop event
insighteye_app  | 2025-12-31 07:04:46,193 [INFO] app.services.stream_service_3: ✅ Processing task created: process_stream_7658cb9b-6225-45f8-8383-3869b5a1e4b9
insighteye_app  | 2025-12-31 07:04:46,194 [INFO] app.services.stream_service_3: ✅ Registered with shared stream manager
insighteye_app  | 2025-12-31 07:04:46,194 [INFO] app.services.stream_service_3: ✅ Transitioned to ACTIVE state
insighteye_app  | 2025-12-31 07:04:46,194 [INFO] app.services.stream_processing_service: Starting stream processing for 7658cb9b-6225-45f8-8383-3869b5a1e4b9 (Test 3)
insighteye_app  | 2025-12-31 07:04:46,201 [INFO] app.services.stream_service_3: ✅ Workspace members notified
insighteye_app  | 2025-12-31 07:04:46,201 [INFO] app.services.stream_service_3: ✅ ✅ ✅ Stream 7658cb9b-6225-45f8-8383-3869b5a1e4b9 SUCCESSFULLY STARTED
insighteye_app  | 2025-12-31 07:04:46,201 [INFO] app.services.stream_service_3: ✅ Stream 7658cb9b-6225-45f8-8383-3869b5a1e4b9 started in workspace f14766ad-a36e-47e1-9c54-ed9fa6a14a00
insighteye_app  | 2025-12-31 07:04:46,201 [INFO] app.api.routes.stream_router_3: Started stream 7658cb9b-6225-45f8-8383-3869b5a1e4b9 (Test 3) in workspace f14766ad-a36e-47e1-9c54-ed9fa6a14a00
insighteye_app  | 2025-12-31 07:04:46,211 [INFO] app.services.stream_processing_service: Stream 7658cb9b-6225-45f8-8383-3869b5a1e4b9 parameters: frame_skip=30, frame_delay=0.0, conf=0.4000000059604645
insighteye_app  | 2025-12-31 07:04:46,325 [INFO] app.services.stream_service_3: ✅ User 7b1bb653-9686-4540-a4b9-ad11f772cccb requested start for stream 78b9a9d5-f0a8-42f8-abc6-dadd4ac53b34
insighteye_app  | 2025-12-31 07:04:46,325 [INFO] app.services.stream_service_3: 🎬 START REQUEST: stream=78b9a9d5-f0a8-42f8-abc6-dadd4ac53b34, camera=Test 4, workspace=f14766ad-a36e-47e1-9c54-ed9fa6a14a00, source=./videos/fire1.mp4
insighteye_app  | 2025-12-31 07:04:46,335 [INFO] app.services.stream_service_3: ✅ QUOTA CHECK PASSED for stream 78b9a9d5-f0a8-42f8-abc6-dadd4ac53b34
insighteye_app  | 2025-12-31 07:04:46,336 [INFO] app.services.stream_service_3: 🔄 Starting fresh stream instance for 78b9a9d5-f0a8-42f8-abc6-dadd4ac53b34
insighteye_app  | 2025-12-31 07:04:46,336 [INFO] app.services.stream_service_3: ✅ Registered stream in workspace registry
insighteye_app  | 2025-12-31 07:04:46,336 [INFO] app.services.stream_service_3: ✅ Initialized stream entry in active_streams
insighteye_app  | 2025-12-31 07:04:46,336 [INFO] app.services.stream_service_3: ✅ Initialized fire detection state
insighteye_app  | 2025-12-31 07:04:46,337 [INFO] app.services.stream_service_3: 💾 Updating database: is_streaming=TRUE, clearing stop_reason
insighteye_app  | 2025-12-31 07:04:46,340 [INFO] app.services.stream_service_3: ✅ Database updated successfully
insighteye_app  | 2025-12-31 07:04:46,342 [INFO] app.services.stream_service_3: 📊 Database verification: is_streaming=True, status=processing, stop_reason=None
insighteye_app  | 2025-12-31 07:04:46,343 [INFO] app.services.stream_service_3: ✅ Initialized processing stats
insighteye_app  | 2025-12-31 07:04:46,343 [INFO] app.services.stream_service_3: 🗄️ Ensuring Qdrant collection for workspace f14766ad-a36e-47e1-9c54-ed9fa6a14a00
insighteye_app  | 2025-12-31 07:04:46,343 [INFO] app.services.stream_service_3: ✅ Qdrant collection verified
insighteye_app  | 2025-12-31 07:04:46,343 [INFO] app.services.stream_service_3: ✅ Created stop event
insighteye_app  | 2025-12-31 07:04:46,343 [INFO] app.services.stream_service_3: ✅ Processing task created: process_stream_78b9a9d5-f0a8-42f8-abc6-dadd4ac53b34
insighteye_app  | 2025-12-31 07:04:46,343 [INFO] app.services.stream_service_3: ✅ Registered with shared stream manager
insighteye_app  | 2025-12-31 07:04:46,344 [INFO] app.services.stream_service_3: ✅ Transitioned to ACTIVE state
insighteye_app  | 2025-12-31 07:04:46,344 [INFO] app.services.stream_processing_service: Starting stream processing for 78b9a9d5-f0a8-42f8-abc6-dadd4ac53b34 (Test 4)
insighteye_app  | 2025-12-31 07:04:46,359 [INFO] app.services.stream_service_3: ✅ Workspace members notified
insighteye_app  | 2025-12-31 07:04:46,359 [INFO] app.services.stream_service_3: ✅ ✅ ✅ Stream 78b9a9d5-f0a8-42f8-abc6-dadd4ac53b34 SUCCESSFULLY STARTED
insighteye_app  | 2025-12-31 07:04:46,359 [INFO] app.services.stream_service_3: ✅ Stream 78b9a9d5-f0a8-42f8-abc6-dadd4ac53b34 started in workspace f14766ad-a36e-47e1-9c54-ed9fa6a14a00
insighteye_app  | 2025-12-31 07:04:46,360 [INFO] app.api.routes.stream_router_3: Started stream 78b9a9d5-f0a8-42f8-abc6-dadd4ac53b34 (Test 4) in workspace f14766ad-a36e-47e1-9c54-ed9fa6a14a00
insighteye_app  | 2025-12-31 07:04:46,362 [INFO] app.services.stream_processing_service: Stream 78b9a9d5-f0a8-42f8-abc6-dadd4ac53b34 parameters: frame_skip=30, frame_delay=0.0, conf=0.4000000059604645
insighteye_app  | 2025-12-31 07:04:46,460 [INFO] uvicorn.access: 172.18.0.1:55234 - "POST /insighteye/workspace/f14766ad-a36e-47e1-9c54-ed9fa6a14a00/start-all HTTP/1.1" 200


insighteye_app  | 2025-12-31 07:06:48,599 [INFO] app.services.stream_service_3: 📊 Resource Monitor: {'timestamp': '2025-12-31T09:06:48.599457+02:00', 'active_streams': 4, 'total_workspaces': 1, 'shared_sources': 2, 'notification_subscribers': 0, 'error_records': 0, 'performance': {'total_started': 5, 'total_stopped': 1, 'total_errors': 0, 'avg_duration': 386.284055}}










