from __future__ import annotations

import textwrap
from typing import Any, Dict, Iterable, Optional, Tuple

import dlt

from connectors.runtime.protocol import ReadSelection

from .constants import (
    CONNECTOR_NAME,
    DEFAULT_LOOKBACK_MINUTES,
    DEFAULT_MAX_PAGES,
    DEFAULT_MAX_RECORDS,
)
from .events import info, warn
from .gql import FirefliesClient, build_transcripts_list_query, get_nested
from .selection import is_stream_enabled, normalize_selection
from .state import FirefliesStateManager
from .streaming import TranscriptStreamer


def _require_api_key(creds: Dict[str, Any]) -> str:
    api_key = (creds or {}).get("api_key") or creds.get("token")
    if not api_key:
        raise Exception("Fireflies API key missing (expected creds.api_key)")
    return str(api_key)


def test_connection(creds: Dict[str, Any]) -> str:
    """
    Light-weight probe: fetch 1 transcript to validate the API key and endpoint.
    """
    api_key = _require_api_key(creds)
    client = FirefliesClient(api_key)

    query = build_transcripts_list_query(include_participants=False, include_summary=False)
    variables = {"limit": 1, "skip": 0}

    payload = client.execute(query, variables, stream="transcripts", op="test_connection")
    first_id = get_nested(payload, ["data", "transcripts", 0, "id"])
    if not first_id:
        warn("test_connection.no_transcripts", stream="transcripts")
    return "Fireflies Connected"


def run_pipeline(
    creds: Dict[str, Any],
    schema: str,
    state: Dict[str, Any],
    selection: Optional[ReadSelection] = None,
) -> Tuple[str, Optional[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
    """
    Execute Fireflies sync via the transcript streamer and load to Postgres with DLT.
    """
    api_key = _require_api_key(creds)
    client = FirefliesClient(api_key)

    selected = normalize_selection(selection)
    want_transcripts = is_stream_enabled("transcripts", selected, set())
    want_participants = is_stream_enabled("transcript_participants", selected, set())
    want_meeting_index = is_stream_enabled("meeting_index", selected, set())
    want_meetings = is_stream_enabled("meetings", selected, set())

    if not (want_transcripts or want_participants or want_meeting_index or want_meetings):
        warn("sync.no_streams_selected", stream="fireflies")
        return "No streams selected", None, {}, {}

    state_mgr = FirefliesStateManager(state or {})
    date_cursor = state_mgr.get_date_cursor()

    streamer = TranscriptStreamer(
        client=client,
        date_cursor_iso=date_cursor,
        lookback_minutes=DEFAULT_LOOKBACK_MINUTES,
        max_pages=DEFAULT_MAX_PAGES,
        max_records=DEFAULT_MAX_RECORDS,
        include_participants=want_participants,
        include_raw_participants=False,
        include_summary=True,
        enable_meeting_dedupe=True,
        dedupe_identical_transcripts=True,
    )

    @dlt.resource(name="fireflies_transcripts_raw", write_disposition="append")
    def transcripts_raw() -> Iterable[Dict[str, Any]]:
        for bundle in streamer:
            state_mgr.track_record_date(bundle.transcript.get("date"))
            yield {
                "transcript": bundle.transcript,
                "participants": bundle.participants,
                "meeting_index": bundle.meeting_index,
                "meeting": bundle.meeting,
            }

    @dlt.transformer(
        data_from=transcripts_raw,
        write_disposition="merge",
        primary_key="id",
        table_name="transcripts",
    )
    def transcripts_table(bundle: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        if not want_transcripts:
            return
        t = (bundle or {}).get("transcript") or {}
        if t.get("_skip_write"):
            return
        yield t

    @dlt.transformer(
        data_from=transcripts_raw,
        write_disposition="merge",
        primary_key=("transcript_id", "participant_key"),
        table_name="transcript_participants",
    )
    def participants_table(bundle: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        if not want_participants:
            return
        t = (bundle or {}).get("transcript") or {}
        if t.get("_skip_write"):
            return
        ps = (bundle or {}).get("participants") or []
        for p in ps:
            yield p

    @dlt.transformer(
        data_from=transcripts_raw,
        write_disposition="merge",
        primary_key=("meeting_key", "transcript_id"),
        table_name="meeting_index",
    )
    def meeting_index_table(bundle: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        if not want_meeting_index:
            return
        idx = (bundle or {}).get("meeting_index")
        if idx:
            yield idx

    @dlt.transformer(
        data_from=transcripts_raw,
        write_disposition="merge",
        primary_key="meeting_key",
        table_name="meetings",
    )
    def meetings_table(bundle: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        if not want_meetings:
            return
        m = (bundle or {}).get("meeting")
        if m:
            yield m

    resources = [transcripts_raw, transcripts_table]
    if want_participants:
        resources.append(participants_table)
    if want_meeting_index:
        resources.append(meeting_index_table)
    if want_meetings:
        resources.append(meetings_table)

    info(
        "pipeline.run.start",
        stream="fireflies",
        cursor=date_cursor,
        lookback_minutes=DEFAULT_LOOKBACK_MINUTES,
        max_pages=DEFAULT_MAX_PAGES,
        max_records=DEFAULT_MAX_RECORDS,
        want_transcripts=want_transcripts,
        want_participants=want_participants,
        want_meeting_index=want_meeting_index,
        want_meetings=want_meetings,
    )

    pipeline = dlt.pipeline(pipeline_name=CONNECTOR_NAME, destination="postgres", dataset_name=schema)
    info_obj = pipeline.run(resources)

    state_mgr.finalize()
    state_updates = state_mgr.updates()

    stats: Dict[str, Any] = dict(streamer.stats)
    rows_inserted = stats.get("unique_transcript_ids", 0)
    if want_participants:
        rows_inserted += stats.get("participants", 0)
    if want_meeting_index:
        rows_inserted += stats.get("unique_transcript_ids", 0)
    if want_meetings:
        rows_inserted += stats.get("meeting_keys", 0)
    stats["rows_inserted"] = rows_inserted
    report_lines = [
        "Fireflies sync completed.",
        f"Pages: {stats.get('pages', 0)}",
        f"Transcripts: {stats.get('unique_transcript_ids', 0)} "
        f"(dupes skipped: {stats.get('duplicates_skipped', 0)}, "
        f"content-collapsed: {stats.get('content_duplicates_collapsed', 0)})",
    ]
    if want_participants:
        report_lines.append(f"Participants: {stats.get('participants', 0)}")
    if want_meeting_index or want_meetings:
        report_lines.append(
            f"Meeting keys: {stats.get('meeting_keys', 0)} (collisions: {stats.get('meeting_collisions', 0)})"
        )
    report_lines.append(f"Estimated rows inserted: {rows_inserted}")
    if date_cursor:
        report_lines.append(f"Cursor in: {date_cursor}")
    cursor_out = (
        state_updates.get("streams", {})
        .get("transcripts", {})
        .get("cursor_date")
    )
    if cursor_out:
        report_lines.append(f"Cursor out: {cursor_out}")
    if info_obj:
        report_lines.append(str(info_obj))

    report_text = textwrap.dedent("\n".join(report_lines)).strip()

    return report_text, None, state_updates, stats
