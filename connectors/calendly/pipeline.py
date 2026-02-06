from __future__ import annotations

import textwrap
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set, Tuple

import dlt
import requests

from connectors.runtime.protocol import ReadSelection
from connectors.utils import add_metadata, requests_retry_session

from .constants import (
    CONNECTOR_NAME,
    DEFAULT_BACKFILL_START_AT,
    DEFAULT_BACKFILL_WINDOW_DAYS,
    DEFAULT_BASE_URL,
    DEFAULT_FUTURE_DAYS,
    DEFAULT_LOOKBACK_DAYS,
    DEFAULT_PAGE_SIZE,
    INVITEE_WORKERS,
    MIN_INTERVAL_SECONDS,
)
from .events import info, records_seen, warn
from .http import CalendlyClient, rate_limiter_from_min_interval, unwrap_resource
from .selection import is_selected, normalize_selection


def _require_token(creds: Dict[str, Any]) -> str:
    token = (creds or {}).get("access_token") or (creds or {}).get("token") or (creds or {}).get("pat")
    if not token or not isinstance(token, str):
        raise ValueError("Calendly creds missing access_token/token/pat")
    return token


def _base_url(creds: Dict[str, Any]) -> str:
    base = (creds or {}).get("base_url") or DEFAULT_BASE_URL
    return str(base).rstrip("/")


def _parse_iso(s: Any) -> Optional[datetime]:
    if not isinstance(s, str) or not s.strip():
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _get_stream_state(state: Dict[str, Any], stream: str) -> Dict[str, Any]:
    streams = state.get("streams") if isinstance(state.get("streams"), dict) else {}
    s = streams.get(stream)
    return s if isinstance(s, dict) else {}


def test_connection(creds: Dict[str, Any]) -> str:
    token = _require_token(creds)
    session = requests_retry_session()
    client = CalendlyClient(
        base_url=_base_url(creds),
        token=token,
        session=session,
        rate_limiter=None,
    )
    me = client.get_resource("/users/me", stream=CONNECTOR_NAME, op="test_connection")
    if not me:
        return "Calendly Connected"
    who = me.get("name") or me.get("email") or me.get("uri") or "Calendly"
    return f"Connected as {who}"


def _safe_get(d: Any, key: str) -> Optional[Any]:
    if isinstance(d, dict):
        return d.get(key)
    return None


def _extract_uri(v: Any) -> Optional[str]:
    if isinstance(v, str) and v.strip():
        return v.strip()
    if isinstance(v, dict):
        u = v.get("uri")
        if isinstance(u, str) and u.strip():
            return u.strip()
    return None


def _list_org_memberships(client: CalendlyClient, *, org_uri: str) -> List[Dict[str, Any]]:
    # Some tenants have multiple pages; we materialize in-memory because it is typically small
    out: List[Dict[str, Any]] = []
    for m in client.iter_collection(
        "/organization_memberships",
        stream="organization_memberships",
        op="list",
        params={"organization": org_uri, "count": DEFAULT_PAGE_SIZE},
        page_size=DEFAULT_PAGE_SIZE,
    ):
        out.append(m)
    return out


def _list_users_via_endpoint(client: CalendlyClient, *, org_uri: str) -> Optional[List[Dict[str, Any]]]:
    """
    Prefer list endpoint if available; fall back to per-user GET otherwise.
    """
    try:
        out: List[Dict[str, Any]] = []
        for u in client.iter_collection(
            "/users",
            stream="users",
            op="list",
            params={"organization": org_uri, "count": DEFAULT_PAGE_SIZE},
            page_size=DEFAULT_PAGE_SIZE,
        ):
            out.append(u)
        return out
    except requests.HTTPError:
        return None


def _fetch_user(client: CalendlyClient, user_uri: str) -> Optional[Dict[str, Any]]:
    try:
        return client.get_resource(user_uri, stream="users", op="get", allow_404=True)
    except requests.HTTPError:
        return None


def _iter_event_types(
    client: CalendlyClient,
    *,
    org_uris: List[str],
    user_uris: List[str],
) -> Iterator[Dict[str, Any]]:
    seen: Set[str] = set()

    # Organization-wide listing (if supported)
    for org_uri in org_uris:
        for et in client.iter_collection(
            "/event_types",
            stream="event_types",
            op="list_by_org",
            params={"organization": org_uri, "count": DEFAULT_PAGE_SIZE},
            page_size=DEFAULT_PAGE_SIZE,
            allow_404=True,
        ):
            uri = _extract_uri(et) or ""
            if uri and uri in seen:
                continue
            if uri:
                seen.add(uri)
            yield et

    # Per-user listing (commonly supported)
    for user_uri in user_uris:
        for et in client.iter_collection(
            "/event_types",
            stream="event_types",
            op="list_by_user",
            params={"user": user_uri, "count": DEFAULT_PAGE_SIZE},
            page_size=DEFAULT_PAGE_SIZE,
            allow_404=True,
        ):
            uri = _extract_uri(et) or ""
            if uri and uri in seen:
                continue
            if uri:
                seen.add(uri)
            yield et


def _iter_availability_schedules(client: CalendlyClient, *, user_uris: List[str]) -> Iterator[Dict[str, Any]]:
    for user_uri in user_uris:
        for s in client.iter_collection(
            "/availability_schedules",
            stream="availability_schedules",
            op="list",
            params={"user": user_uri, "count": DEFAULT_PAGE_SIZE},
            page_size=DEFAULT_PAGE_SIZE,
            allow_404=True,
        ):
            yield s


def _iter_webhook_subscriptions(client: CalendlyClient, *, org_uris: List[str]) -> Iterator[Dict[str, Any]]:
    for org_uri in org_uris:
        for w in client.iter_collection(
            "/webhook_subscriptions",
            stream="webhook_subscriptions",
            op="list",
            params={"organization": org_uri, "scope": "organization", "count": DEFAULT_PAGE_SIZE},
            page_size=DEFAULT_PAGE_SIZE,
            allow_404=True,
        ):
            yield w


def _iter_routing_forms(client: CalendlyClient, *, org_uris: List[str]) -> Iterator[Dict[str, Any]]:
    for org_uri in org_uris:
        for rf in client.iter_collection(
            "/routing_forms",
            stream="routing_forms",
            op="list",
            params={"organization": org_uri, "count": DEFAULT_PAGE_SIZE},
            page_size=DEFAULT_PAGE_SIZE,
            allow_404=True,
        ):
            yield rf


def _iter_scheduled_events_window(
    client: CalendlyClient,
    *,
    org_uri: str,
    min_start_time: str,
    max_start_time: str,
    status: Optional[str],
) -> Iterator[Dict[str, Any]]:
    params: Dict[str, Any] = {
        "organization": org_uri,
        "count": DEFAULT_PAGE_SIZE,
        "min_start_time": min_start_time,
        "max_start_time": max_start_time,
        "sort": "start_time:asc",
    }
    if status:
        params["status"] = status

    yield from client.iter_collection(
        "/scheduled_events",
        stream="scheduled_events",
        op="list",
        params=params,
        page_size=DEFAULT_PAGE_SIZE,
    )


def _iter_scheduled_events_window_all_status(
    client: CalendlyClient,
    *,
    org_uri: str,
    min_start_time: str,
    max_start_time: str,
) -> Iterator[Dict[str, Any]]:
    """
    Best-effort to include canceled events while staying compatible with tenants where
    the `status` filter is unavailable.
    """
    seen: Set[str] = set()

    def _yield_unique(it: Iterator[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
        for ev in it:
            if not isinstance(ev, dict):
                continue
            uri = _extract_uri(ev.get("uri")) or ""
            if uri and uri in seen:
                continue
            if uri:
                seen.add(uri)
            yield ev

    # Probe status support using "active"
    try:
        yield from _yield_unique(
            _iter_scheduled_events_window(
                client,
                org_uri=org_uri,
                min_start_time=min_start_time,
                max_start_time=max_start_time,
                status="active",
            )
        )
    except requests.HTTPError as e:
        code = getattr(getattr(e, "response", None), "status_code", None)
        if code in (400, 404):
            yield from _yield_unique(
                _iter_scheduled_events_window(
                    client,
                    org_uri=org_uri,
                    min_start_time=min_start_time,
                    max_start_time=max_start_time,
                    status=None,
                )
            )
            return
        raise

    # Status is supported; fetch canceled events (try both spellings)
    canceled_ok = False
    for st in ("canceled", "cancelled"):
        try:
            yield from _yield_unique(
                _iter_scheduled_events_window(
                    client,
                    org_uri=org_uri,
                    min_start_time=min_start_time,
                    max_start_time=max_start_time,
                    status=st,
                )
            )
            canceled_ok = True
            break
        except requests.HTTPError as e:
            code = getattr(getattr(e, "response", None), "status_code", None)
            if code in (400, 404):
                continue
            raise

    if not canceled_ok:
        warn(
            "scheduled_events.canceled_filter_unavailable",
            stream="scheduled_events",
            min_start_time=min_start_time,
            max_start_time=max_start_time,
        )


def _chunked_windows(start: datetime, end: datetime, *, window_days: int) -> Iterator[Tuple[datetime, datetime]]:
    cur = start
    step = timedelta(days=max(1, int(window_days)))
    while cur < end:
        nxt = min(end, cur + step)
        yield cur, nxt
        cur = nxt


def _thread_local_client_factory(base_url: str, token: str) -> CalendlyClient:
    """
    Per-thread client for concurrent hydration.
    """
    session = requests_retry_session()
    return CalendlyClient(base_url=base_url, token=token, session=session, rate_limiter=None)


def run_pipeline(
    creds: Dict[str, Any],
    schema: str,
    state: Dict[str, Any],
    selection: Optional[ReadSelection] = None,
) -> Tuple[str, Optional[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
    token = _require_token(creds)
    base_url = _base_url(creds)

    selected = normalize_selection(selection)
    want_users = is_selected(selected, "users")
    want_orgs = is_selected(selected, "organizations")
    want_memberships = is_selected(selected, "organization_memberships")
    want_event_types = is_selected(selected, "event_types")
    want_availability = is_selected(selected, "availability_schedules")
    want_events = is_selected(selected, "scheduled_events") or is_selected(selected, "scheduled_event_invitees") or is_selected(selected, "scheduled_event_cancellations")
    want_invitees = is_selected(selected, "scheduled_event_invitees")
    want_cancellations = is_selected(selected, "scheduled_event_cancellations")
    want_webhooks = is_selected(selected, "webhook_subscriptions")
    want_routing_forms = is_selected(selected, "routing_forms")

    if not (
        want_users
        or want_orgs
        or want_memberships
        or want_event_types
        or want_availability
        or want_events
        or want_webhooks
        or want_routing_forms
    ):
        warn("sync.no_streams_selected", stream=CONNECTOR_NAME)
        return "No streams selected", None, {}, {}

    session = requests_retry_session()
    min_interval = float(creds.get("min_interval_seconds") or MIN_INTERVAL_SECONDS or 0.0)
    rate_limiter = rate_limiter_from_min_interval(min_interval) or None
    client = CalendlyClient(base_url=base_url, token=token, session=session, rate_limiter=rate_limiter)

    # ----------------------------
    # Discovery (me, orgs, memberships, users)
    # ----------------------------
    me = client.get_resource("/users/me", stream=CONNECTOR_NAME, op="users.me")
    if not me:
        raise RuntimeError("Calendly: /users/me returned empty response")

    me_user_uri = _extract_uri(me.get("uri")) or _extract_uri(me) or ""
    current_org_uri = _extract_uri(me.get("current_organization")) or ""

    if not current_org_uri:
        # best-effort fallbacks (depends on API version/shape)
        current_org_uri = _extract_uri(me.get("organization")) or ""

    if not current_org_uri:
        raise RuntimeError("Calendly: could not determine organization from /users/me")

    org_uris: List[str] = [current_org_uri]

    # Fetch org memberships for the tenant (usually all users)
    memberships = _list_org_memberships(client, org_uri=current_org_uri) if want_memberships or want_users or want_event_types else []

    user_uris: Set[str] = set()
    if me_user_uri:
        user_uris.add(me_user_uri)
    for m in memberships:
        u = _extract_uri(m.get("user"))
        if u:
            user_uris.add(u)
    user_uri_list = sorted(user_uris)

    # Organizations: include current org resource
    org_resources: List[Dict[str, Any]] = []
    if want_orgs:
        org = client.get_resource(current_org_uri, stream="organizations", op="get_org")
        if org:
            org_resources.append(org)

    # Users: prefer list endpoint (if available), otherwise GET each user uri
    user_resources: List[Dict[str, Any]] = []
    if want_users:
        listed = _list_users_via_endpoint(client, org_uri=current_org_uri)
        if listed:
            # list endpoint may return wrapped items or full resources directly; keep as-is
            for u in listed:
                user_resources.append(unwrap_resource(u))
        else:
            for uuri in user_uri_list:
                u = _fetch_user(client, uuri)
                if u:
                    user_resources.append(u)

    info(
        "sync.discovery",
        stream=CONNECTOR_NAME,
        organization=current_org_uri,
        users=len(user_uri_list),
        memberships=len(memberships),
    )

    # ----------------------------
    # State (scheduled events backfill)
    # ----------------------------
    se_state = _get_stream_state(state or {}, "scheduled_events")
    backfill_complete = bool(se_state.get("backfill_complete"))
    backfill_start_at = _parse_iso(se_state.get("backfill_start_at")) or _parse_iso(DEFAULT_BACKFILL_START_AT) or datetime(2010, 1, 1, tzinfo=timezone.utc)
    backfill_window_days = int(se_state.get("backfill_window_days") or DEFAULT_BACKFILL_WINDOW_DAYS)
    lookback_days = int(se_state.get("lookback_days") or DEFAULT_LOOKBACK_DAYS)
    future_days = int(se_state.get("future_days") or DEFAULT_FUTURE_DAYS)

    now = _utc_now()
    backfill_end = now + timedelta(days=future_days)
    incremental_min = now - timedelta(days=lookback_days)
    incremental_max = now + timedelta(days=future_days)

    # ----------------------------
    # Stream counters / stats
    # ----------------------------
    counts: Dict[str, int] = {}
    _last_emit_at: Dict[str, float] = {}
    _emit_every_n = 250

    def bump(stream: str, n: int = 1) -> None:
        cur = int(counts.get(stream, 0) + n)
        counts[stream] = cur
        if cur % _emit_every_n == 0:
            records_seen(stream=stream, count=cur)

    # ----------------------------
    # DLT resources
    # ----------------------------
    @dlt.resource(table_name="organizations", write_disposition="merge", primary_key="uri")
    def organizations() -> Iterable[Dict[str, Any]]:
        if not want_orgs:
            return
        for o in org_resources:
            bump("organizations")
            yield add_metadata(dict(o), CONNECTOR_NAME)

    @dlt.resource(table_name="organization_memberships", write_disposition="merge", primary_key="uri")
    def organization_memberships() -> Iterable[Dict[str, Any]]:
        if not want_memberships:
            return
        for m in memberships:
            bump("organization_memberships")
            rec = dict(m)
            rec["_organization_uri"] = current_org_uri
            rec["_user_uri"] = _extract_uri(m.get("user"))
            yield add_metadata(rec, CONNECTOR_NAME)

    @dlt.resource(table_name="users", write_disposition="merge", primary_key="uri")
    def users() -> Iterable[Dict[str, Any]]:
        if not want_users:
            return
        for u in user_resources:
            bump("users")
            rec = dict(u)
            rec["_organization_uri"] = current_org_uri
            yield add_metadata(rec, CONNECTOR_NAME)

    @dlt.resource(table_name="event_types", write_disposition="merge", primary_key="uri")
    def event_types() -> Iterable[Dict[str, Any]]:
        if not want_event_types:
            return
        for et in _iter_event_types(client, org_uris=org_uris, user_uris=user_uri_list):
            bump("event_types")
            rec = dict(et)
            rec["_organization_uri"] = current_org_uri
            yield add_metadata(rec, CONNECTOR_NAME)

    @dlt.resource(table_name="availability_schedules", write_disposition="merge", primary_key="uri")
    def availability_schedules() -> Iterable[Dict[str, Any]]:
        if not want_availability:
            return
        for s in _iter_availability_schedules(client, user_uris=user_uri_list):
            bump("availability_schedules")
            rec = dict(s)
            rec["_organization_uri"] = current_org_uri
            yield add_metadata(rec, CONNECTOR_NAME)

    @dlt.resource(name="scheduled_events_raw", write_disposition="append")
    def scheduled_events_raw() -> Iterable[Dict[str, Any]]:
        if not want_events:
            return

        def _hydrate_batch(batch: List[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
            if (not want_invitees) and (not want_cancellations):
                for ev in batch:
                    yield {"scheduled_event": ev, "invitees": [], "cancellation": None, "_organization_uri": current_org_uri}
                return

            with ThreadPoolExecutor(max_workers=int(INVITEE_WORKERS)) as pool:
                # Per-thread session/client to avoid sharing requests.Session across threads.
                import threading

                tl = threading.local()

                def get_client() -> CalendlyClient:
                    c = getattr(tl, "client", None)
                    if c is None:
                        c = _thread_local_client_factory(base_url, token)
                        tl.client = c
                    return c

                def list_invitees(event_uri: str) -> List[Dict[str, Any]]:
                    cli = get_client()
                    return list(
                        cli.iter_collection(
                            f"{event_uri.rstrip('/')}/invitees",
                            stream="scheduled_event_invitees",
                            op="list",
                            params={"count": DEFAULT_PAGE_SIZE},
                            page_size=DEFAULT_PAGE_SIZE,
                            allow_404=True,
                        )
                    )

                def get_cancellation(event_uri: str) -> Optional[Dict[str, Any]]:
                    cli = get_client()
                    return cli.get_resource(
                        f"{event_uri.rstrip('/')}/cancellation",
                        stream="scheduled_event_cancellations",
                        op="get",
                        allow_404=True,
                    )

                futures: Dict[Future[Any], Tuple[str, str]] = {}

                for ev in batch:
                    ev_uri = _extract_uri(ev.get("uri")) or ""
                    if not ev_uri:
                        continue
                    if want_invitees:
                        fut = pool.submit(list_invitees, ev_uri)
                        futures[fut] = ("invitees", ev_uri)
                    if want_cancellations and str(ev.get("status") or "").lower() in ("canceled", "cancelled"):
                        fut2 = pool.submit(get_cancellation, ev_uri)
                        futures[fut2] = ("cancellation", ev_uri)

                # Attach results
                invitees_map: Dict[str, List[Dict[str, Any]]] = {}
                cancellation_map: Dict[str, Optional[Dict[str, Any]]] = {}

                for fut in as_completed(list(futures.keys())):
                    kind, ev_uri = futures[fut]
                    try:
                        res = fut.result()
                    except Exception as e:
                        warn("hydrate.failed", stream="scheduled_events", kind=kind, event_uri=ev_uri, error=str(e))
                        continue
                    if kind == "invitees":
                        invitees_map[ev_uri] = res if isinstance(res, list) else []
                    else:
                        cancellation_map[ev_uri] = res if isinstance(res, dict) else None

            for ev in batch:
                ev_uri = _extract_uri(ev.get("uri")) or ""
                yield {
                    "scheduled_event": ev,
                    "invitees": invitees_map.get(ev_uri, []),
                    "cancellation": cancellation_map.get(ev_uri),
                    "_organization_uri": current_org_uri,
                }

        def _emit_window(kind: str, start: datetime, end: datetime) -> None:
            info(
                "scheduled_events.window",
                stream="scheduled_events",
                kind=kind,
                min_start_time=_iso_z(start),
                max_start_time=_iso_z(end),
            )

        # Phase A: one-time backfill across history (start_at -> now+future)
        if not backfill_complete:
            _emit_window("backfill.start", backfill_start_at, backfill_end)
            for w_start, w_end in _chunked_windows(backfill_start_at, backfill_end, window_days=backfill_window_days):
                _emit_window("backfill.window", w_start, w_end)
                batch: List[Dict[str, Any]] = []
                for ev in _iter_scheduled_events_window_all_status(
                    client,
                    org_uri=current_org_uri,
                    min_start_time=_iso_z(w_start),
                    max_start_time=_iso_z(w_end),
                ):
                    batch.append(ev)
                    if len(batch) >= DEFAULT_PAGE_SIZE:
                        yield from _hydrate_batch(batch)
                        batch = []
                if batch:
                    yield from _hydrate_batch(batch)

        # Phase B: rolling incremental window (lookback -> now+future)
        _emit_window("incremental.window", incremental_min, incremental_max)
        batch2: List[Dict[str, Any]] = []
        for ev in _iter_scheduled_events_window_all_status(
            client,
            org_uri=current_org_uri,
            min_start_time=_iso_z(incremental_min),
            max_start_time=_iso_z(incremental_max),
        ):
            batch2.append(ev)
            if len(batch2) >= DEFAULT_PAGE_SIZE:
                yield from _hydrate_batch(batch2)
                batch2 = []
        if batch2:
            yield from _hydrate_batch(batch2)

    @dlt.transformer(data_from=scheduled_events_raw, write_disposition="merge", primary_key="uri", table_name="scheduled_events")
    def scheduled_events(row: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        if not is_selected(selected, "scheduled_events"):
            return
        ev = _safe_get(row, "scheduled_event") or {}
        if not isinstance(ev, dict):
            return
        bump("scheduled_events")
        rec = dict(ev)
        rec["_organization_uri"] = row.get("_organization_uri") or current_org_uri
        yield add_metadata(rec, CONNECTOR_NAME)

    @dlt.transformer(
        data_from=scheduled_events_raw,
        write_disposition="merge",
        primary_key="uri",
        table_name="scheduled_event_invitees",
    )
    def scheduled_event_invitees(row: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        if not want_invitees:
            return
        ev = _safe_get(row, "scheduled_event") or {}
        ev_uri = _extract_uri(_safe_get(ev, "uri")) or ""
        invitees = _safe_get(row, "invitees") or []
        if not isinstance(invitees, list):
            return
        for inv in invitees:
            if not isinstance(inv, dict):
                continue
            bump("scheduled_event_invitees")
            rec = dict(inv)
            rec["_scheduled_event_uri"] = ev_uri
            rec["_organization_uri"] = row.get("_organization_uri") or current_org_uri
            yield add_metadata(rec, CONNECTOR_NAME)

    @dlt.transformer(
        data_from=scheduled_events_raw,
        write_disposition="merge",
        primary_key="_scheduled_event_uri",
        table_name="scheduled_event_cancellations",
    )
    def scheduled_event_cancellations(row: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        if not want_cancellations:
            return
        ev = _safe_get(row, "scheduled_event") or {}
        ev_uri = _extract_uri(_safe_get(ev, "uri")) or ""
        canc = _safe_get(row, "cancellation")
        if not isinstance(canc, dict):
            return
        bump("scheduled_event_cancellations")
        rec = dict(canc)
        rec["_scheduled_event_uri"] = ev_uri
        rec["_organization_uri"] = row.get("_organization_uri") or current_org_uri
        yield add_metadata(rec, CONNECTOR_NAME)

    @dlt.resource(table_name="webhook_subscriptions", write_disposition="merge", primary_key="uri")
    def webhook_subscriptions() -> Iterable[Dict[str, Any]]:
        if not want_webhooks:
            return
        for w in _iter_webhook_subscriptions(client, org_uris=org_uris):
            bump("webhook_subscriptions")
            rec = dict(w)
            rec["_organization_uri"] = current_org_uri
            yield add_metadata(rec, CONNECTOR_NAME)

    @dlt.resource(table_name="routing_forms", write_disposition="merge", primary_key="uri")
    def routing_forms() -> Iterable[Dict[str, Any]]:
        if not want_routing_forms:
            return
        for rf in _iter_routing_forms(client, org_uris=org_uris):
            bump("routing_forms")
            rec = dict(rf)
            rec["_organization_uri"] = current_org_uri
            yield add_metadata(rec, CONNECTOR_NAME)

    resources: List[Any] = []
    if want_orgs:
        resources.append(organizations)
    if want_memberships:
        resources.append(organization_memberships)
    if want_users:
        resources.append(users)
    if want_event_types:
        resources.append(event_types)
    if want_availability:
        resources.append(availability_schedules)
    if want_events:
        resources.extend([scheduled_events_raw, scheduled_events])
        if want_invitees:
            resources.append(scheduled_event_invitees)
        if want_cancellations:
            resources.append(scheduled_event_cancellations)
    if want_webhooks:
        resources.append(webhook_subscriptions)
    if want_routing_forms:
        resources.append(routing_forms)

    pipeline = dlt.pipeline(pipeline_name=CONNECTOR_NAME, destination="postgres", dataset_name=schema)
    info_obj = pipeline.run(resources)

    # State updates
    state_updates: Dict[str, Any] = {"version": 1, "global": {}, "streams": {}}
    if want_events and (not backfill_complete):
        state_updates["streams"]["scheduled_events"] = {
            "backfill_complete": True,
            "backfill_completed_at": _iso_z(_utc_now()),
            "backfill_start_at": _iso_z(backfill_start_at),
            "backfill_window_days": int(backfill_window_days),
            "lookback_days": int(lookback_days),
            "future_days": int(future_days),
        }

    report_lines = [
        "Calendly sync completed.",
        f"Organization: {current_org_uri}",
    ]
    for k in sorted(counts.keys()):
        report_lines.append(f"{k}: {counts[k]}")
    if info_obj:
        report_lines.append(str(info_obj))
    report_text = textwrap.dedent("\n".join(report_lines)).strip()

    stats: Dict[str, Any] = {"counts": dict(counts)}
    return report_text, None, state_updates, stats
