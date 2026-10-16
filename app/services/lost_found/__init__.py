"""Lost & found: abandoned-object detection, the found gallery and lost-report matching.

Pure logic (geometry, colour, visual features, the abandonment state machine,
match scoring) lives here so it can be unit-tested without a camera, a model or
a database. The engine is ``app.services.lost_found_inference`` and persistence
is ``app.services.lost_found_service``.
"""
