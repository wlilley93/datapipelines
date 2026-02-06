"""
Fetch PandaDoc line items with actual HubSpot prices.

Data path:
  analytics.pandadoc_completed_docs  (document metadata)
  -> pandadoc.documents              (hubspot_deal_id)
  -> HubSpot API v4 associations     (deal -> line_items)
  -> hubspot.line_items              (sku, price, quantity)
"""

import os
from pathlib import Path
from typing import Any, Dict, Mapping

import pandas as pd
import tomlkit
from sqlalchemy import create_engine, text

from connectors.hubspot.client import HubSpotClient


def _get_database_url() -> str:
    """Resolve Postgres connection URL.

    Prefers DATABASE_URL if set, otherwise builds from POSTGRES_* env vars.
    """
    if os.getenv("DATABASE_URL"):
        return str(os.getenv("DATABASE_URL"))

    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5434")
    database = os.getenv("POSTGRES_DB", "dlt_db")
    username = os.getenv("POSTGRES_USER", "dlt_user")
    password = os.getenv("POSTGRES_PASSWORD")

    if not password:
        raise RuntimeError(
            "Missing POSTGRES_PASSWORD (or DATABASE_URL). "
            "Set it in your environment or an .env file."
        )

    return f"postgresql://{username}:{password}@{host}:{port}/{database}"


def _as_plain_dict(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, Mapping):
        return dict(value)
    return {}


# Cache directory relative to this file's parent (financial_analysis/output/.cache/)
_CACHE_DIR = Path(__file__).resolve().parent.parent / "output" / ".cache"


def _get_db_engine():
    return create_engine(_get_database_url())


def _get_hubspot_client():
    secrets_path = (
        Path(__file__).resolve().parent.parent.parent / ".dlt" / "secrets.toml"
    )
    with open(secrets_path, "r", encoding="utf-8") as f:
        doc = tomlkit.load(f)

    sources = _as_plain_dict(doc.get("sources"))
    hubspot = _as_plain_dict(sources.get("hubspot"))
    token = hubspot.get("access_token")
    if not token:
        raise RuntimeError("Missing HubSpot access_token in .dlt/secrets.toml")

    return HubSpotClient(token=str(token))


def _get_cache_path(start_date, end_date):
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    start_label = start_date or "all"
    end_label = end_date or "all"
    return _CACHE_DIR / f"line_items_{start_label}_{end_label}.csv"


def _fetch_documents_with_deals(engine, start_date=None, end_date=None):
    """Query documents that have a HubSpot deal ID, optionally filtered by date."""
    conditions = ["d._details__metadata__hubspot_deal_id IS NOT NULL"]
    params = {}

    if start_date:
        conditions.append("a.date_completed >= :start_date")
        params["start_date"] = start_date
    if end_date:
        conditions.append("a.date_completed <= :end_date")
        params["end_date"] = end_date

    where_clause = " AND ".join(conditions)

    query = text(f"""
        SELECT
            a.document_id,
            a.document_name,
            a.segment,
            a.date_completed,
            a.template_name,
            d._details__metadata__hubspot_deal_id as hubspot_deal_id
        FROM analytics.pandadoc_completed_docs a
        JOIN pandadoc.documents d ON a.document_id = d.id
        WHERE {where_clause}
        ORDER BY a.date_completed DESC
    """)

    return pd.read_sql(query, engine, params=params)


def _fetch_line_items_for_deals(engine, client, documents_df):
    """For each document's deal, fetch line item associations via HubSpot API,
    then look up line item details from the local DB."""
    all_line_items = []
    total = len(documents_df)

    for idx, row in documents_df.iterrows():
        deal_id = row["hubspot_deal_id"]
        if not deal_id:
            continue

        doc_name = row["document_name"]
        display_name = doc_name[:50] if doc_name else "Unknown"
        print(f"  [{idx + 1}/{total}] {display_name}...")

        try:
            assoc = client.get_associations(
                stream="fetch_assoc",
                from_object_type="deals",
                object_id=str(deal_id),
                to_object_type="line_items",
            )

            line_item_ids = [r["toObjectId"] for r in assoc.get("results", [])]

            if not line_item_ids:
                continue

            # Use parameterized query for line item lookup
            placeholders = ", ".join([f":id_{i}" for i in range(len(line_item_ids))])
            params = {f"id_{i}": str(lid) for i, lid in enumerate(line_item_ids)}

            li_query = text(f"""
                SELECT
                    hs_object_id,
                    name,
                    hs_sku,
                    CAST(amount AS NUMERIC) as price,
                    CAST(quantity AS NUMERIC) as quantity
                FROM hubspot.line_items
                WHERE hs_object_id IN ({placeholders})
            """)

            line_items_df = pd.read_sql(li_query, engine, params=params)

            for _, li in line_items_df.iterrows():
                all_line_items.append(
                    {
                        "hs_object_id": str(li["hs_object_id"]),
                        "document_id": row["document_id"],
                        "document_name": row["document_name"],
                        "segment": row["segment"],
                        "date_completed": row["date_completed"],
                        "template_name": row["template_name"],
                        "item_name": li["name"],
                        "sku": li["hs_sku"],
                        "price": li["price"],
                        "quantity": li["quantity"],
                    }
                )

        except Exception as e:
            print(f"    Error: {e}")
            continue

    return pd.DataFrame(all_line_items)


def _fetch_legacy_line_items(engine, start_date=None, end_date=None, exclude_ids=None):
    """Fetch SPV incorporation items directly from hubspot.line_items.

    Covers legacy items (NULL SKUs) and alternate SKU formats that aren't
    linked through PandaDoc documents.
    """
    conditions = [
        """(
            li.name ILIKE 'ADGM Formations%%Incorporation%%ADGM SPV%%'
            OR (li.hs_sku ILIKE '%%adgm-incorp-001' AND li.name ILIKE '%%Incorporation%%')
            OR (li.hs_sku ILIKE '%%adgm-incorp-002' AND li.name ILIKE '%%Incorporation%%')
            OR (li.hs_sku ILIKE '%%adgm-subsc-004' AND li.name ILIKE '%%Incorporation%%')
        )"""
    ]
    params = {}

    if start_date:
        conditions.append("li.createdate >= :start_date")
        params["start_date"] = start_date
    if end_date:
        conditions.append("li.createdate <= :end_date")
        params["end_date"] = end_date

    where_clause = " AND ".join(conditions)

    query = text(f"""
        SELECT
            li.hs_object_id,
            li.name as item_name,
            li.hs_sku as sku,
            CAST(li.amount AS NUMERIC) as price,
            CAST(li.quantity AS NUMERIC) as quantity,
            li.createdate as date_completed
        FROM hubspot.line_items li
        WHERE {where_clause}
        ORDER BY li.createdate DESC
    """)

    df = pd.read_sql(query, engine, params=params)

    # Exclude items already fetched via PandaDoc path
    if exclude_ids is not None and len(exclude_ids) > 0:
        df = df[~df["hs_object_id"].astype(str).isin(exclude_ids)]

    df["document_id"] = None
    df["document_name"] = None
    df["segment"] = None
    df["template_name"] = None
    df["source"] = "HubSpot only"

    return df[
        [
            "document_id",
            "document_name",
            "segment",
            "date_completed",
            "template_name",
            "item_name",
            "sku",
            "price",
            "quantity",
            "source",
        ]
    ]


_OUTPUT_COLUMNS = [
    "document_id",
    "document_name",
    "segment",
    "date_completed",
    "template_name",
    "item_name",
    "sku",
    "price",
    "quantity",
    "source",
]


def fetch_line_items_with_prices(start_date=None, end_date=None, refresh=False):
    """Fetch all line items with actual HubSpot prices for the given date range.

    Combines two data sources:
    1. PandaDoc-linked items (via deal associations) - have document metadata
    2. Legacy HubSpot line items (direct DB query) - SPV incorporation items
       that predate the PandaDoc integration or use legacy SKU formats

    Uses a CSV cache to avoid repeated API calls. Pass refresh=True to bypass cache.

    Args:
        start_date: YYYY-MM-DD string or None for no lower bound
        end_date: YYYY-MM-DD string or None for no upper bound
        refresh: If True, ignore cache and re-fetch from DB + HubSpot API

    Returns:
        DataFrame with columns: document_id, document_name, segment,
        date_completed, template_name, item_name, sku, price, quantity, source
    """
    cache_path = _get_cache_path(start_date, end_date)

    if not refresh and cache_path.exists():
        print(f"  Using cached data: {cache_path.name}")
        return pd.read_csv(cache_path)

    if not start_date and not end_date:
        print("  Warning: No date filter. This may take a while for large datasets.")

    engine = _get_db_engine()
    client = _get_hubspot_client()

    # Step 1: PandaDoc-linked items (have full document metadata)
    print("  Querying documents with HubSpot deal IDs...")
    documents_df = _fetch_documents_with_deals(engine, start_date, end_date)
    print(f"  Found {len(documents_df)} documents")

    pandadoc_df = pd.DataFrame(columns=_OUTPUT_COLUMNS)
    pandadoc_item_ids = set()

    if len(documents_df) > 0:
        print("  Fetching line item prices via HubSpot API...")
        pandadoc_df = _fetch_line_items_for_deals(engine, client, documents_df)
        if len(pandadoc_df) > 0:
            pandadoc_df["source"] = "PandaDoc + HubSpot"
            if "hs_object_id" in pandadoc_df.columns:
                pandadoc_item_ids = set(pandadoc_df["hs_object_id"].astype(str))

    # Step 2: Legacy items directly from HubSpot line_items table
    print("  Fetching legacy/unlinked items from HubSpot...")
    legacy_df = _fetch_legacy_line_items(
        engine, start_date, end_date, exclude_ids=pandadoc_item_ids
    )
    print(f"  Found {len(legacy_df)} additional legacy items")

    # Merge both sources
    result_df = pd.concat([pandadoc_df, legacy_df], ignore_index=True)

    # Cache the results
    result_df.to_csv(cache_path, index=False)
    print(f"  Cached {len(result_df)} total line items to {cache_path.name}")

    return result_df
