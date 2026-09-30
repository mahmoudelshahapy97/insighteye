"""Chart queries shared by the no-entry-zone and blocked-exit services.

Both features store one row per event with ``stream_id``, ``confidence`` and ``status``
and filter it with ``video_stream vs`` joined in, so the per-camera incident counts and
the confidence audit are the same SQL over a different ``FROM`` clause.
"""

from typing import Any, Dict, List, Optional, Sequence

HISTOGRAM_BUCKETS = 10
PER_EVENT_LIMIT = 200


def csv_values(value: Any) -> List[str]:
    """``"a, b"`` -> ``["a", "b"]``; lists pass through. Filters from the hierarchy picker
    arrive comma-separated."""
    items = value if isinstance(value, (list, tuple)) else str(value).split(",")
    return [s for s in (str(i).strip() for i in items) if s]


async def incidents_per_camera(
    db, *, from_sql: str, where: str, params: Sequence[Any], alias: str, incident_expr: str,
) -> List[Dict[str, Any]]:
    """Events, incidents and resolved events per camera, most incidents first."""
    return await db.execute_query(
        f"""
        SELECT {alias}.stream_id,
               COALESCE(vs.name, MAX({alias}.camera_name))  AS camera_name,
               vs.location, vs.area, vs.building, vs.floor_level, vs.zone,
               COUNT(*)                                     AS events,
               {incident_expr}                              AS incidents,
               COUNT(*) FILTER (WHERE {alias}.status = 'resolved') AS resolved
        FROM {from_sql}
        WHERE {where}
        GROUP BY {alias}.stream_id, vs.name, vs.location, vs.area, vs.building, vs.floor_level, vs.zone
        ORDER BY incidents DESC, events DESC, camera_name
        """,
        tuple(params),
        fetch_all=True,
    ) or []


async def confidence_audit(
    db, *, from_sql: str, where: str, params: Sequence[Any], alias: str, threshold: float,
) -> Dict[str, Any]:
    """Model accuracy & confidence audit: totals, per-camera spread, a 10-bucket
    histogram over [0, 1] and the latest events. ``low_confidence`` counts events within
    0.1 of the detector threshold — the ones most worth a human look."""
    conf = f"{alias}.confidence"
    low_cutoff = round(min(1.0, threshold + 0.1), 4)
    p = len(params)
    scored = f"({where}) AND {conf} IS NOT NULL"

    summary = await db.execute_query(
        f"""
        SELECT COUNT(*)                                            AS events,
               COUNT({conf})                                       AS scored_events,
               AVG({conf})::float                                  AS avg_confidence,
               MIN({conf})::float                                  AS min_confidence,
               MAX({conf})::float                                  AS max_confidence,
               COUNT(*) FILTER (WHERE {conf} < ${p + 1})           AS low_confidence,
               COUNT(*) FILTER (WHERE {alias}.status = 'acknowledged') AS acknowledged,
               COUNT(*) FILTER (WHERE {alias}.status = 'resolved') AS resolved
        FROM {from_sql}
        WHERE {where}
        """,
        (*params, low_cutoff),
        fetch_one=True,
    ) or {}

    per_camera = await db.execute_query(
        f"""
        SELECT {alias}.stream_id,
               COALESCE(vs.name, MAX({alias}.camera_name)) AS camera_name,
               COUNT(*)                                    AS events,
               AVG({conf})::float                          AS avg_confidence,
               MIN({conf})::float                          AS min_confidence,
               MAX({conf})::float                          AS max_confidence,
               COUNT(*) FILTER (WHERE {conf} < ${p + 1})   AS low_confidence
        FROM {from_sql}
        WHERE {scored}
        GROUP BY {alias}.stream_id, vs.name
        ORDER BY avg_confidence DESC NULLS LAST, camera_name
        """,
        (*params, low_cutoff),
        fetch_all=True,
    ) or []

    rows = await db.execute_query(
        f"""
        SELECT LEAST(width_bucket({conf}, 0, 1, {HISTOGRAM_BUCKETS}), {HISTOGRAM_BUCKETS}) AS b,
               COUNT(*) AS count
        FROM {from_sql}
        WHERE {scored}
        GROUP BY 1
        """,
        tuple(params),
        fetch_all=True,
    ) or []
    counts = {r["b"]: r["count"] for r in rows}
    step = 1 / HISTOGRAM_BUCKETS
    histogram = [
        {
            "bucket": f"{(i - 1) * step:.1f}-{i * step:.1f}",
            "min": round((i - 1) * step, 2),
            "max": round(i * step, 2),
            "count": counts.get(i, 0),
        }
        for i in range(1, HISTOGRAM_BUCKETS + 1)
    ]

    per_event = await db.execute_query(
        f"""
        SELECT {alias}.event_id, {alias}.event_timestamp, {alias}.stream_id,
               COALESCE(vs.name, {alias}.camera_name) AS camera_name,
               {conf}::float AS confidence, {alias}.status
        FROM {from_sql}
        WHERE {scored}
        ORDER BY {alias}.event_timestamp DESC, {alias}.event_id DESC
        LIMIT {PER_EVENT_LIMIT}
        """,
        tuple(params),
        fetch_all=True,
    ) or []

    scored_events = summary.get("scored_events") or 0
    return {
        "summary": {
            "events": summary.get("events") or 0,
            "scored_events": scored_events,
            "avg_confidence": summary.get("avg_confidence"),
            "min_confidence": summary.get("min_confidence"),
            "max_confidence": summary.get("max_confidence"),
            "low_confidence": summary.get("low_confidence") or 0,
            "low_confidence_share": (
                round((summary.get("low_confidence") or 0) / scored_events, 4) if scored_events else None
            ),
            "acknowledged": summary.get("acknowledged") or 0,
            "resolved": summary.get("resolved") or 0,
            "threshold": threshold,
            "low_confidence_cutoff": low_cutoff,
        },
        "per_camera": per_camera,
        "histogram": histogram,
        "per_event": per_event,
    }


TZ = "Africa/Cairo"
TREND_BUCKETS = ("hour", "day")
AUTO_HOURLY_SPAN_S = 2 * 86400


async def trend(
    db, *, from_sql: str, where: str, params: Sequence[Any], alias: str, incident_key: str,
    bucket: Optional[str] = None,
) -> Dict[str, Any]:
    """Incidents and resolved incidents per hour or day (local time), with empty buckets
    filled in. ``bucket=None`` picks hourly for spans of two days or less, else daily."""
    local = f"({alias}.event_timestamp AT TIME ZONE '{TZ}')"
    if bucket not in TREND_BUCKETS:
        span = await db.execute_query(
            f"SELECT EXTRACT(EPOCH FROM MAX({alias}.event_timestamp) - MIN({alias}.event_timestamp))::float AS s "
            f"FROM {from_sql} WHERE {where}",
            tuple(params),
            fetch_one=True,
        ) or {}
        seconds = span.get("s")
        bucket = "hour" if seconds is not None and seconds <= AUTO_HOURLY_SPAN_S else "day"
    rows = await db.execute_query(
        f"""
        WITH ev AS (
            SELECT date_trunc('{bucket}', {local}) AS b, {incident_key} AS k, {alias}.status
            FROM {from_sql}
            WHERE {where}
        ),
        series AS (
            SELECT generate_series(MIN(b), MAX(b), INTERVAL '1 {bucket}') AS b FROM ev
        )
        SELECT series.b                                                AS bucket_start,
               COUNT(DISTINCT ev.k)                                    AS incidents,
               COUNT(DISTINCT ev.k) FILTER (WHERE ev.status = 'resolved') AS resolved
        FROM series LEFT JOIN ev ON ev.b = series.b
        GROUP BY series.b
        ORDER BY series.b
        """,
        tuple(params),
        fetch_all=True,
    ) or []
    return {"bucket": bucket, "points": rows}


async def heatmap(
    db, *, from_sql: str, where: str, params: Sequence[Any], alias: str, incident_key: str,
) -> List[Dict[str, Any]]:
    """Incidents per weekday (0 = Sunday, like ``/blocked-exit/analytics/hourly``) and
    local hour. Only non-empty cells are returned."""
    local = f"({alias}.event_timestamp AT TIME ZONE '{TZ}')"
    return await db.execute_query(
        f"""
        SELECT EXTRACT(DOW FROM {local})::int  AS weekday,
               EXTRACT(HOUR FROM {local})::int AS hour,
               COUNT(DISTINCT {incident_key})   AS incidents
        FROM {from_sql}
        WHERE {where}
        GROUP BY 1, 2
        ORDER BY 1, 2
        """,
        tuple(params),
        fetch_all=True,
    ) or []


_RESPONSE_FIELDS = """
    COUNT(*)                                                               AS incidents,
    COUNT(a)                                                               AS acknowledged,
    COUNT(r)                                                               AS resolved,
    COUNT(*) FILTER (WHERE r IS NULL)                                      AS open,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM a - t0)) AS ack_median_s,
    percentile_cont(0.9) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM a - t0)) AS ack_p90_s,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM r - t0)) AS resolve_median_s,
    percentile_cont(0.9) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM r - t0)) AS resolve_p90_s
"""


async def response_times(
    db, *, from_sql: str, where: str, params: Sequence[Any], alias: str, incident_key: str,
) -> Dict[str, Any]:
    """How long operators take to acknowledge and resolve an incident, measured from its
    first event. Median and 90th percentile, overall and per camera."""
    incidents_cte = f"""
        WITH inc AS (
            SELECT {incident_key}                                     AS k,
                   {alias}.stream_id,
                   MAX(COALESCE(vs.name, {alias}.camera_name))        AS camera_name,
                   MIN({alias}.event_timestamp)                       AS t0,
                   MIN({alias}.acknowledged_at)                       AS a,
                   MIN({alias}.resolved_at)                           AS r
            FROM {from_sql}
            WHERE {where}
            GROUP BY {incident_key}, {alias}.stream_id
        )
    """
    summary = await db.execute_query(
        f"{incidents_cte} SELECT {_RESPONSE_FIELDS} FROM inc",
        tuple(params),
        fetch_one=True,
    ) or {}
    per_camera = await db.execute_query(
        f"""{incidents_cte}
        SELECT stream_id, MAX(camera_name) AS camera_name, {_RESPONSE_FIELDS}
        FROM inc
        GROUP BY stream_id
        ORDER BY incidents DESC, camera_name
        """,
        tuple(params),
        fetch_all=True,
    ) or []
    empty = {"incidents": 0, "acknowledged": 0, "resolved": 0, "open": 0}
    return {"summary": {**empty, **{k: v for k, v in summary.items() if v is not None}}, "per_camera": per_camera}


async def histogram(
    db, *, from_sql: str, where: str, params: Sequence[Any], value_sql: str,
    edges: Sequence[float],
) -> List[Dict[str, Any]]:
    """Count rows of ``value_sql`` into ``[edges[i], edges[i+1])`` buckets; the last
    bucket is open-ended (``max`` is None)."""
    rows = await db.execute_query(
        f"""
        SELECT width_bucket(({value_sql})::float, ARRAY[{', '.join(str(float(e)) for e in edges)}]::float[]) AS b,
               COUNT(*) AS count
        FROM {from_sql}
        WHERE ({where}) AND ({value_sql}) IS NOT NULL
        GROUP BY 1
        """,
        tuple(params),
        fetch_all=True,
    ) or []
    counts = {r["b"]: r["count"] for r in rows}
    return [
        {
            "min": float(edges[i]),
            "max": float(edges[i + 1]) if i + 1 < len(edges) else None,
            "count": counts.get(i + 1, 0),
        }
        for i in range(len(edges))
    ]
