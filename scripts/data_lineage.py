"""
5-Level Data Lineage Extraction & Publishing

Extracts full data lineage from PROD Strategy server across multiple projects
and publishes each level as a separate cube on DEV server.

Levels:
  L1: Dashboards / Documents / Dossiers
  L2: Datasets (Reports, OLAP Cubes, Super Cubes)
  L3: Metrics / Attributes (with formulas/expressions)
  L4: Facts / Schema Attribute definitions (with expressions)
  L5: Tables / Columns / Database Sources

Usage:
  python scripts/data_lineage.py
  python scripts/data_lineage.py --project-ids <id1>,<id2>

Based on Robert Prochowicz's 5-level lineage approach (Strategy community).
"""

import sys
import os
import argparse
import logging
import json
import base64
import time
from datetime import datetime

import urllib3
import requests
import pandas as pd

# Suppress all SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Add project root to path so we can import core
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.auth import StrategySession
from core.config import get_prod_config, get_dev_config, get_prod_project_ids

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("lineage")

# ---------------------------------------------------------------------------
# Strategy object type IDs (EnumDSSXMLObjectTypes)
# ---------------------------------------------------------------------------
TYPE_REPORT = 3
TYPE_METRIC = 4
TYPE_ATTRIBUTE = 12
TYPE_FACT = 13
TYPE_TABLE = 15
TYPE_DOSSIER = 55
TYPE_DOCUMENT = 14768
TYPE_OLAP_CUBE = 776
TYPE_SUPER_CUBE = 779

# Human-readable names
TYPE_NAMES = {
    3: "Report", 4: "Metric", 12: "Attribute", 13: "Fact",
    15: "Table", 55: "Dossier", 14768: "Document",
    776: "OLAP Cube", 779: "Super Cube",
}


def type_name(type_id):
    return TYPE_NAMES.get(type_id, f"Type_{type_id}")


# ===========================================================================
# METADATA SEARCH — Core lineage traversal (Robert Prochowicz approach)
# ===========================================================================

def metadata_search(session, used_by_id=None, used_by_type=None,
                    result_types=None, domain=2, recursive=False):
    """
    Two-step metadata search:
      1) POST /api/metadataSearches/results → creates search instance
      2) GET  /api/metadataSearches/results → retrieves results

    Args:
        session: authenticated StrategySession (PROD)
        used_by_id: object ID to find components of (what does this object use?)
        used_by_type: type ID of the object
        result_types: list of type IDs to filter results (e.g., [4, 12] for metrics+attrs)
        domain: search domain (2 = project scope)
        recursive: if True, find indirect dependencies too

    Returns:
        list of dicts: [{id, name, type, subtype, dateModified, owner, path}, ...]
    """
    params = {"domain": domain}

    if used_by_id and used_by_type:
        params["usedByObject"] = f"{used_by_id};{used_by_type}"
        params["usedByRecursive"] = str(recursive).lower()

    if result_types:
        params["type"] = ",".join(str(t) for t in result_types)

    # Step 1: POST to create search
    try:
        resp = session.post("metadataSearches/results", params=params)
    except Exception as e:
        log.warning(f"Metadata search POST failed: {e}")
        return []

    try:
        data = resp.json()
    except Exception:
        log.warning("Metadata search returned non-JSON response")
        return []

    # The POST response already contains the first page of results
    results = _parse_search_results(data)

    # Step 2: Paginate with GET if there are more results
    total = data.get("totalItems", len(results))
    offset = len(results)

    while offset < total:
        try:
            page = session.get("metadataSearches/results",
                               params={"offset": offset, "limit": 1000})
            page_results = _parse_search_results(page)
            if not page_results:
                break
            results.extend(page_results)
            offset += len(page_results)
        except Exception as e:
            log.warning(f"Metadata search GET pagination failed at offset {offset}: {e}")
            break

    return results


def search_all_objects(session, types, domain=2):
    """Search for all objects of given types in the project (no usedByObject filter)."""
    params = {"domain": domain, "type": ",".join(str(t) for t in types)}

    try:
        resp = session.post("metadataSearches/results", params=params)
    except Exception as e:
        log.warning(f"Search all objects failed: {e}")
        return []

    try:
        data = resp.json()
    except Exception:
        return []

    results = _parse_search_results(data)

    total = data.get("totalItems", len(results))
    offset = len(results)

    while offset < total:
        try:
            page = session.get("metadataSearches/results",
                               params={"offset": offset, "limit": 1000})
            page_results = _parse_search_results(page)
            if not page_results:
                break
            results.extend(page_results)
            offset += len(page_results)
        except Exception as e:
            log.warning(f"Pagination failed at offset {offset}: {e}")
            break

    return results


def _parse_search_results(data):
    """Parse metadata search response into list of object dicts."""
    results = []
    items = data.get("result", data.get("results", []))
    if isinstance(items, list):
        for item in items:
            results.append({
                "id": item.get("id", ""),
                "name": item.get("name", ""),
                "type": item.get("type", 0),
                "subtype": item.get("subtype", 0),
                "dateModified": item.get("dateModified", ""),
                "owner": _get_owner_name(item),
                "path": item.get("location", ""),
            })
    return results


def _get_owner_name(item):
    """Extract owner name from search result item."""
    owner = item.get("owner", {})
    if isinstance(owner, dict):
        return owner.get("name", "")
    return str(owner) if owner else ""


# ===========================================================================
# EXPRESSION EXTRACTION — 3-tier fallback
# ===========================================================================

def get_metric_expression(session, metric_id):
    """
    Get metric formula text with 3-tier fallback:
      1. Model API: GET /api/model/metrics/{id}?showExpressionAs=tokens
      2. Object API: GET /api/objects/{id}?type=4
      3. Metadata search components list
    Returns: (expression_text, source)
    """
    # Tier 1: Model API
    try:
        data = session.get(f"model/metrics/{metric_id}",
                           params={"showExpressionAs": "tokens"})
        info = data.get("information", {})
        expr = data.get("expression", {})
        text = expr.get("text", "")
        if text:
            return text, "Model API"
    except Exception as e:
        log.debug(f"Model API failed for metric {metric_id}: {e}")

    # Tier 2: Object Definition API
    try:
        data = session.get(f"objects/{metric_id}", params={"type": TYPE_METRIC})
        # Try to extract expression from object definition
        expr = _extract_expression_from_object(data)
        if expr:
            return expr, "Object API"
    except Exception as e:
        log.debug(f"Object API failed for metric {metric_id}: {e}")

    # Tier 3: Metadata search — list components
    try:
        components = metadata_search(session, used_by_id=metric_id,
                                     used_by_type=TYPE_METRIC)
        if components:
            names = [f"{c['name']} ({type_name(c['type'])})" for c in components]
            return f"Uses: {', '.join(names)}", "Component List"
    except Exception as e:
        log.debug(f"Component search failed for metric {metric_id}: {e}")

    return "N/A", "Unavailable"


def get_attribute_expression(session, attribute_id):
    """
    Get attribute form expressions with 3-tier fallback.
    Returns: (expression_text, source)
    """
    # Tier 1: Model API
    try:
        data = session.get(f"model/attributes/{attribute_id}",
                           params={"showExpressionAs": "tokens"})
        forms = data.get("forms", [])
        form_exprs = []
        for form in forms:
            expr = form.get("expression", {})
            text = expr.get("text", "")
            form_name = form.get("name", "")
            if text:
                form_exprs.append(f"{form_name}: {text}" if form_name else text)
        if form_exprs:
            return " | ".join(form_exprs), "Model API"
    except Exception as e:
        log.debug(f"Model API failed for attribute {attribute_id}: {e}")

    # Tier 2: Object Definition API
    try:
        data = session.get(f"objects/{attribute_id}", params={"type": TYPE_ATTRIBUTE})
        expr = _extract_expression_from_object(data)
        if expr:
            return expr, "Object API"
    except Exception as e:
        log.debug(f"Object API failed for attribute {attribute_id}: {e}")

    # Tier 3: Metadata search components
    try:
        components = metadata_search(session, used_by_id=attribute_id,
                                     used_by_type=TYPE_ATTRIBUTE)
        if components:
            names = [f"{c['name']} ({type_name(c['type'])})" for c in components]
            return f"Mapped to: {', '.join(names)}", "Component List"
    except Exception as e:
        log.debug(f"Component search failed for attribute {attribute_id}: {e}")

    return "N/A", "Unavailable"


def get_fact_expression(session, fact_id):
    """
    Get fact expression with 3-tier fallback.
    Returns: (expression_text, source)
    """
    # Tier 1: Model API
    try:
        data = session.get(f"model/facts/{fact_id}",
                           params={"showExpressionAs": "tokens"})
        expressions = data.get("expressions", [])
        expr_texts = []
        for expr_entry in expressions:
            expr = expr_entry.get("expression", {})
            text = expr.get("text", "")
            if text:
                tables = expr_entry.get("tables", [])
                table_names = [t.get("name", "") for t in tables if t.get("name")]
                if table_names:
                    text += f" [Tables: {', '.join(table_names)}]"
                expr_texts.append(text)
        if expr_texts:
            return " | ".join(expr_texts), "Model API"
    except Exception as e:
        log.debug(f"Model API failed for fact {fact_id}: {e}")

    # Tier 2: Object Definition API
    try:
        data = session.get(f"objects/{fact_id}", params={"type": TYPE_FACT})
        expr = _extract_expression_from_object(data)
        if expr:
            return expr, "Object API"
    except Exception as e:
        log.debug(f"Object API failed for fact {fact_id}: {e}")

    # Tier 3: Metadata search components
    try:
        components = metadata_search(session, used_by_id=fact_id,
                                     used_by_type=TYPE_FACT)
        if components:
            names = [f"{c['name']} ({type_name(c['type'])})" for c in components]
            return f"Uses: {', '.join(names)}", "Component List"
    except Exception as e:
        log.debug(f"Component search failed for fact {fact_id}: {e}")

    return "N/A", "Unavailable"


def _extract_expression_from_object(data):
    """Try to extract expression text from object definition response."""
    # Object API may return expression in different formats
    if isinstance(data, dict):
        # Check for direct expression field
        for key in ("expression", "formula", "definition"):
            val = data.get(key)
            if isinstance(val, str) and val:
                return val
            if isinstance(val, dict):
                text = val.get("text", "")
                if text:
                    return text
        # Check nested
        defn = data.get("definition", {})
        if isinstance(defn, dict):
            expr = defn.get("expression", {})
            if isinstance(expr, dict):
                return expr.get("text", "")
    return ""


# ===========================================================================
# TABLE / COLUMN / DATASOURCE ENRICHMENT
# ===========================================================================

_datasource_cache = {}


def get_table_details(session, table_id):
    """
    Get table physical details: columns and data source.
    Returns: list of (column_name, db_source)
    """
    try:
        data = session.get(f"model/tables/{table_id}")
        phys = data.get("physicalTable", {})
        columns = phys.get("columns", [])
        ds = data.get("primaryDataSource", {})
        ds_name = ds.get("name", "")

        if not ds_name and ds.get("objectId"):
            ds_name = _get_datasource_name(session, ds["objectId"])

        col_list = []
        for col in columns:
            col_name = col.get("columnName", col.get("name", ""))
            col_list.append((col_name, ds_name))

        if not col_list:
            table_name = phys.get("tableName", data.get("information", {}).get("name", ""))
            col_list.append(("(no columns returned)", ds_name or "N/A"))

        return col_list
    except Exception as e:
        log.debug(f"Table details failed for {table_id}: {e}")
        return [("N/A", "N/A")]


def _get_datasource_name(session, ds_id):
    """Get datasource name by ID, with caching."""
    if ds_id in _datasource_cache:
        return _datasource_cache[ds_id]

    try:
        data = session.get("datasources")
        sources = data if isinstance(data, list) else data.get("datasources", [])
        for src in sources:
            _datasource_cache[src.get("id", "")] = src.get("name", "")
    except Exception:
        pass

    return _datasource_cache.get(ds_id, "Unknown")


# ===========================================================================
# CUBE PUBLISHING — Push DataFrame to DEV server as dataset
# ===========================================================================

def _df_to_base64(df):
    """Convert DataFrame rows to base64-encoded JSON string (Strategy push data format)."""
    rows = []
    for _, row in df.iterrows():
        row_data = {}
        for col in df.columns:
            val = row[col]
            row_data[col] = str(val) if pd.notna(val) else ""
        rows.append(row_data)
    json_str = json.dumps(rows)
    return base64.b64encode(json_str.encode("utf-8")).decode("utf-8")


def publish_cube(session, cube_name, df, folder_id, description=""):
    """
    Publish a pandas DataFrame as a new dataset (cube) on the DEV server.

    Uses Strategy Push Data API:
      POST /api/datasets — create dataset with definition + base64-encoded data

    Data must be base64-encoded JSON array of objects.
    Expressions use "TableName.ColumnName" format.
    """
    if df.empty:
        log.warning(f"Skipping cube '{cube_name}' — no data")
        return None

    table_name = "LINEAGE_DATA"

    # Build column headers
    column_headers = []
    for col in df.columns:
        column_headers.append({
            "name": col,
            "dataType": "STRING",
        })

    # All columns as string attributes for join capability
    attributes = []
    for col in df.columns:
        attributes.append({
            "name": col,
            "attributeForms": [
                {
                    "category": "ID",
                    "expressions": [{"formula": f"{table_name}.{col}"}],
                    "dataType": "STRING",
                }
            ],
        })

    # Encode data as base64 JSON
    data_b64 = _df_to_base64(df)

    create_body = {
        "name": cube_name,
        "description": description or f"Data Lineage - {cube_name}",
        "folderId": folder_id,
        "tables": [
            {
                "name": table_name,
                "columnHeaders": column_headers,
                "data": data_b64,
            }
        ],
        "attributes": attributes,
        "metrics": [],
    }

    try:
        resp = session.post("datasets", json=create_body)
        result = resp.json()
        dataset_id = result.get("datasetId", "")
        table_id = ""
        tables_resp = result.get("tables", [])
        if tables_resp:
            table_id = tables_resp[0].get("id", "")

        if not dataset_id:
            log.error(f"Failed to create dataset '{cube_name}': no datasetId in response")
            log.error(f"Response: {result}")
            return None

        log.info(f"Published cube '{cube_name}' (datasetId: {dataset_id}, tableId: {table_id})")
        log.info(f"  Rows: {len(df)}, Columns: {len(df.columns)}")
        return dataset_id

    except Exception as e:
        log.error(f"Failed to create dataset '{cube_name}': {e}")
        try:
            log.error(f"Response body: {e.response.text}")
        except Exception:
            pass
        return None


# ===========================================================================
# 5-LEVEL LINEAGE TRAVERSAL
# ===========================================================================

def get_project_name(session, project_id):
    """Get project name from project ID."""
    try:
        data = session.get("projects")
        projects = data if isinstance(data, list) else data.get("projects", [])
        for proj in projects:
            if proj.get("id") == project_id:
                return proj.get("name", project_id)
    except Exception:
        pass
    return project_id


def extract_lineage(prod_session, dev_session, project_id, folder_id, timestamp):
    """
    Extract full 5-level lineage from one project on PROD,
    publish 5 cubes to DEV.
    """
    prod_session.set_project(project_id)
    project_name = get_project_name(prod_session, project_id)
    log.info(f"{'='*60}")
    log.info(f"PROJECT: {project_name} ({project_id})")
    log.info(f"{'='*60}")

    cube_ids = {}

    # -----------------------------------------------------------------------
    # LEVEL 1: Dashboards / Documents / Dossiers
    # -----------------------------------------------------------------------
    log.info("L1: Searching for Dashboards and Documents...")
    l1_objects = search_all_objects(prod_session, types=[TYPE_DOSSIER, TYPE_DOCUMENT])
    log.info(f"L1: Found {len(l1_objects)} dashboards/documents")

    l1_rows = []
    for obj in l1_objects:
        l1_rows.append({
            "project_id": project_id,
            "project_name": project_name,
            "l1_id": obj["id"],
            "l1_name": obj["name"],
            "l1_type": type_name(obj["type"]),
            "l1_subtype": str(obj["subtype"]),
            "l1_owner": obj["owner"],
            "l1_date_modified": obj["dateModified"],
            "l1_path": obj["path"],
        })

    l1_df = pd.DataFrame(l1_rows) if l1_rows else pd.DataFrame()
    cube_name = f"Lineage_L1_{project_name}_{timestamp}"
    cube_ids["L1"] = publish_cube(dev_session, cube_name, l1_df, folder_id,
                                  f"L1 Dashboards/Documents for {project_name}")

    # -----------------------------------------------------------------------
    # LEVEL 2: Datasets / Cubes used by each L1 object
    # -----------------------------------------------------------------------
    log.info("L2: Finding datasets for each L1 object...")
    l2_rows = []
    seen_l2 = {}  # track unique L2 objects

    for i, l1 in enumerate(l1_objects):
        log.info(f"  L2: Processing L1 {i+1}/{len(l1_objects)}: {l1['name']}")
        components = metadata_search(
            prod_session,
            used_by_id=l1["id"],
            used_by_type=l1["type"],
            result_types=[TYPE_REPORT, TYPE_OLAP_CUBE, TYPE_SUPER_CUBE],
        )
        for comp in components:
            l2_rows.append({
                "project_id": project_id,
                "l1_id": l1["id"],
                "l1_name": l1["name"],
                "l2_id": comp["id"],
                "l2_name": comp["name"],
                "l2_type": type_name(comp["type"]),
                "l2_subtype": str(comp["subtype"]),
                "l2_date_modified": comp["dateModified"],
            })
            seen_l2[comp["id"]] = comp

    log.info(f"L2: Found {len(seen_l2)} unique datasets across {len(l2_rows)} relationships")

    l2_df = pd.DataFrame(l2_rows) if l2_rows else pd.DataFrame()
    cube_name = f"Lineage_L2_{project_name}_{timestamp}"
    cube_ids["L2"] = publish_cube(dev_session, cube_name, l2_df, folder_id,
                                  f"L2 Datasets/Cubes for {project_name}")

    # -----------------------------------------------------------------------
    # LEVEL 3: Metrics / Attributes in each dataset (with expressions)
    # -----------------------------------------------------------------------
    log.info("L3: Finding metrics/attributes for each dataset...")
    l3_rows = []
    seen_l3 = {}
    unique_l2_list = list(seen_l2.values())

    for i, l2 in enumerate(unique_l2_list):
        log.info(f"  L3: Processing L2 {i+1}/{len(unique_l2_list)}: {l2['name']}")
        components = metadata_search(
            prod_session,
            used_by_id=l2["id"],
            used_by_type=l2["type"],
            result_types=[TYPE_METRIC, TYPE_ATTRIBUTE],
        )
        for comp in components:
            # Get expression with fallback
            if comp["type"] == TYPE_METRIC:
                expr, source = get_metric_expression(prod_session, comp["id"])
            else:
                expr, source = get_attribute_expression(prod_session, comp["id"])

            l3_rows.append({
                "project_id": project_id,
                "l2_id": l2["id"],
                "l2_name": l2["name"],
                "l3_id": comp["id"],
                "l3_name": comp["name"],
                "l3_type": type_name(comp["type"]),
                "l3_expression": expr,
                "l3_expression_source": source,
            })
            seen_l3[comp["id"]] = comp

    log.info(f"L3: Found {len(seen_l3)} unique metrics/attributes across {len(l3_rows)} relationships")

    l3_df = pd.DataFrame(l3_rows) if l3_rows else pd.DataFrame()
    cube_name = f"Lineage_L3_{project_name}_{timestamp}"
    cube_ids["L3"] = publish_cube(dev_session, cube_name, l3_df, folder_id,
                                  f"L3 Metrics/Attributes for {project_name}")

    # -----------------------------------------------------------------------
    # LEVEL 4: Facts / Schema attribute definitions (with expressions)
    # -----------------------------------------------------------------------
    log.info("L4: Finding facts/schema objects for each L3 object...")
    l4_rows = []
    seen_l4 = {}
    unique_l3_list = list(seen_l3.values())

    for i, l3 in enumerate(unique_l3_list):
        log.info(f"  L4: Processing L3 {i+1}/{len(unique_l3_list)}: {l3['name']}")
        # For metrics, find facts; for attributes, find schema attribute defs
        result_types = [TYPE_FACT, TYPE_ATTRIBUTE] if l3["type"] == TYPE_METRIC else [TYPE_FACT]
        components = metadata_search(
            prod_session,
            used_by_id=l3["id"],
            used_by_type=l3["type"],
            result_types=result_types,
        )
        for comp in components:
            if comp["type"] == TYPE_FACT:
                expr, source = get_fact_expression(prod_session, comp["id"])
            else:
                expr, source = get_attribute_expression(prod_session, comp["id"])

            l4_rows.append({
                "project_id": project_id,
                "l3_id": l3["id"],
                "l3_name": l3["name"],
                "l4_id": comp["id"],
                "l4_name": comp["name"],
                "l4_type": type_name(comp["type"]),
                "l4_expression": expr,
                "l4_expression_source": source,
            })
            seen_l4[comp["id"]] = comp

    log.info(f"L4: Found {len(seen_l4)} unique facts/schema objects across {len(l4_rows)} relationships")

    l4_df = pd.DataFrame(l4_rows) if l4_rows else pd.DataFrame()
    cube_name = f"Lineage_L4_{project_name}_{timestamp}"
    cube_ids["L4"] = publish_cube(dev_session, cube_name, l4_df, folder_id,
                                  f"L4 Facts/Schema Objects for {project_name}")

    # -----------------------------------------------------------------------
    # LEVEL 5: Tables / Columns / DB Sources
    # -----------------------------------------------------------------------
    log.info("L5: Finding tables for each L4 object...")
    l5_rows = []
    unique_l4_list = list(seen_l4.values())

    for i, l4 in enumerate(unique_l4_list):
        log.info(f"  L5: Processing L4 {i+1}/{len(unique_l4_list)}: {l4['name']}")
        tables = metadata_search(
            prod_session,
            used_by_id=l4["id"],
            used_by_type=l4["type"],
            result_types=[TYPE_TABLE],
        )
        for tbl in tables:
            # Get table columns and data source
            col_details = get_table_details(prod_session, tbl["id"])
            for col_name, db_source in col_details:
                l5_rows.append({
                    "project_id": project_id,
                    "l4_id": l4["id"],
                    "l4_name": l4["name"],
                    "l5_table_id": tbl["id"],
                    "l5_table_name": tbl["name"],
                    "l5_column_name": col_name,
                    "l5_db_source": db_source,
                })

    log.info(f"L5: Found {len(l5_rows)} table/column entries")

    l5_df = pd.DataFrame(l5_rows) if l5_rows else pd.DataFrame()
    cube_name = f"Lineage_L5_{project_name}_{timestamp}"
    cube_ids["L5"] = publish_cube(dev_session, cube_name, l5_df, folder_id,
                                  f"L5 Tables/Columns for {project_name}")

    # Summary
    log.info(f"\n{'='*60}")
    log.info(f"SUMMARY for {project_name}")
    log.info(f"  L1 Dashboards/Documents: {len(l1_objects)}")
    log.info(f"  L2 Datasets/Cubes:       {len(seen_l2)}")
    log.info(f"  L3 Metrics/Attributes:   {len(seen_l3)}")
    log.info(f"  L4 Facts/Schema:         {len(seen_l4)}")
    log.info(f"  L5 Table/Column rows:    {len(l5_rows)}")
    log.info(f"  Published cube IDs:      {cube_ids}")
    log.info(f"{'='*60}\n")

    return cube_ids


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="5-Level Data Lineage Extraction & Publishing")
    parser.add_argument("--project-ids", type=str, default=None,
                        help="Comma-separated PROD project IDs (overrides .env)")
    args = parser.parse_args()

    # Load configs
    prod_config = get_prod_config()
    dev_config = get_dev_config()

    # Get project IDs
    if args.project_ids:
        project_ids = [pid.strip() for pid in args.project_ids.split(",") if pid.strip()]
    else:
        project_ids = get_prod_project_ids()

    if not project_ids:
        log.error("No project IDs specified. Use --project-ids or set MSTR_PROD_PROJECT_IDS in .env")
        sys.exit(1)

    folder_id = dev_config.get("folder_id", "")
    if not folder_id:
        log.error("MSTR_DEV_FOLDER_ID not set in .env")
        sys.exit(1)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")

    log.info(f"Starting 5-Level Data Lineage Extraction")
    log.info(f"PROD server: {prod_config['base_url']}")
    log.info(f"DEV server:  {dev_config['base_url']}")
    log.info(f"Projects:    {len(project_ids)}")
    log.info(f"Target folder: {folder_id}")
    log.info("")

    # Create sessions for both servers
    prod_session = StrategySession.from_config(prod_config)
    dev_session = StrategySession.from_config(dev_config, project_id=dev_config.get("project_id"))

    try:
        prod_session.login()
        dev_session.login()

        all_cube_ids = {}
        for idx, project_id in enumerate(project_ids):
            log.info(f"\n>>> Project {idx+1} of {len(project_ids)} <<<")
            try:
                cube_ids = extract_lineage(prod_session, dev_session, project_id, folder_id, timestamp)
                all_cube_ids[project_id] = cube_ids
            except Exception as e:
                log.error(f"Failed to process project {project_id}: {e}")
                import traceback
                traceback.print_exc()

        log.info(f"\n{'#'*60}")
        log.info("ALL PROJECTS COMPLETE")
        log.info(f"Processed {len(all_cube_ids)}/{len(project_ids)} projects successfully")
        log.info(f"{'#'*60}")

    finally:
        prod_session.logout()
        dev_session.logout()


if __name__ == "__main__":
    main()
