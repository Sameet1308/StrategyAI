"""
5-Level Data Lineage Extraction & Publishing

Extracts full data lineage from PROD Strategy server across multiple projects
and publishes each level as a separate cube on DEV server.

Produces exactly 5 cubes (regardless of how many projects):
  Lineage_L1 — Dashboards / Documents
  Lineage_L2 — Datasets / Cubes
  Lineage_L3 — Metrics / Attributes / Freeform SQL
  Lineage_L4 — Facts / Schema Objects
  Lineage_L5 — Tables / Columns / DB Sources

On re-run: deletes existing cubes and recreates with fresh data.

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

# Cube names — exactly 5, fixed, shared across all projects
CUBE_NAMES = {
    "L1": "Lineage_L1",
    "L2": "Lineage_L2",
    "L3": "Lineage_L3",
    "L4": "Lineage_L4",
    "L5": "Lineage_L5",
}

# ---------------------------------------------------------------------------
# Strategy object type IDs
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

TYPE_NAMES = {
    3: "Report", 4: "Metric", 12: "Attribute", 13: "Fact",
    15: "Table", 55: "Dossier", 14768: "Document",
    776: "OLAP Cube", 779: "Super Cube",
}


def type_name(type_id):
    return TYPE_NAMES.get(type_id, f"Type_{type_id}")


# ===========================================================================
# METADATA SEARCH
# ===========================================================================

def metadata_search(session, used_by_id=None, used_by_type=None,
                    result_types=None, domain=2, recursive=False):
    params = {"domain": domain}
    if used_by_id and used_by_type:
        params["usedByObject"] = f"{used_by_id};{used_by_type}"
        params["usedByRecursive"] = str(recursive).lower()
    if result_types:
        params["type"] = ",".join(str(t) for t in result_types)

    try:
        resp = session.post("metadataSearches/results", params=params)
    except Exception as e:
        log.warning(f"Metadata search POST failed: {e}")
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
        except Exception:
            break

    return results


def search_all_objects(session, types, domain=2):
    params = {"domain": domain, "type": ",".join(str(t) for t in types)}
    try:
        resp = session.post("metadataSearches/results", params=params)
    except Exception:
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
        except Exception:
            break

    return results


def _parse_search_results(data):
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
    owner = item.get("owner", {})
    if isinstance(owner, dict):
        return owner.get("name", "")
    return str(owner) if owner else ""


# ===========================================================================
# EXPRESSION EXTRACTION — 3-tier fallback
# ===========================================================================

def get_metric_expression(session, metric_id):
    try:
        data = session.get(f"model/metrics/{metric_id}",
                           params={"showExpressionAs": "tokens"})
        text = data.get("expression", {}).get("text", "")
        if text:
            return text, "Model API"
    except Exception:
        pass
    try:
        data = session.get(f"objects/{metric_id}", params={"type": TYPE_METRIC})
        expr = _extract_expression_from_object(data)
        if expr:
            return expr, "Object API"
    except Exception:
        pass
    try:
        components = metadata_search(session, used_by_id=metric_id, used_by_type=TYPE_METRIC)
        if components:
            names = [f"{c['name']} ({type_name(c['type'])})" for c in components]
            return f"Uses: {', '.join(names)}", "Component List"
    except Exception:
        pass
    return "N/A", "Unavailable"


def get_attribute_expression(session, attribute_id):
    try:
        data = session.get(f"model/attributes/{attribute_id}",
                           params={"showExpressionAs": "tokens"})
        forms = data.get("forms", [])
        form_exprs = []
        for form in forms:
            text = form.get("expression", {}).get("text", "")
            form_name = form.get("name", "")
            if text:
                form_exprs.append(f"{form_name}: {text}" if form_name else text)
        if form_exprs:
            return " | ".join(form_exprs), "Model API"
    except Exception:
        pass
    try:
        data = session.get(f"objects/{attribute_id}", params={"type": TYPE_ATTRIBUTE})
        expr = _extract_expression_from_object(data)
        if expr:
            return expr, "Object API"
    except Exception:
        pass
    try:
        components = metadata_search(session, used_by_id=attribute_id, used_by_type=TYPE_ATTRIBUTE)
        if components:
            names = [f"{c['name']} ({type_name(c['type'])})" for c in components]
            return f"Mapped to: {', '.join(names)}", "Component List"
    except Exception:
        pass
    return "N/A", "Unavailable"


def get_fact_expression(session, fact_id):
    try:
        data = session.get(f"model/facts/{fact_id}",
                           params={"showExpressionAs": "tokens"})
        expressions = data.get("expressions", [])
        expr_texts = []
        for expr_entry in expressions:
            text = expr_entry.get("expression", {}).get("text", "")
            if text:
                tables = expr_entry.get("tables", [])
                table_names = [t.get("name", "") for t in tables if t.get("name")]
                if table_names:
                    text += f" [Tables: {', '.join(table_names)}]"
                expr_texts.append(text)
        if expr_texts:
            return " | ".join(expr_texts), "Model API"
    except Exception:
        pass
    try:
        data = session.get(f"objects/{fact_id}", params={"type": TYPE_FACT})
        expr = _extract_expression_from_object(data)
        if expr:
            return expr, "Object API"
    except Exception:
        pass
    try:
        components = metadata_search(session, used_by_id=fact_id, used_by_type=TYPE_FACT)
        if components:
            names = [f"{c['name']} ({type_name(c['type'])})" for c in components]
            return f"Uses: {', '.join(names)}", "Component List"
    except Exception:
        pass
    return "N/A", "Unavailable"


def _extract_expression_from_object(data):
    if isinstance(data, dict):
        for key in ("expression", "formula", "definition"):
            val = data.get(key)
            if isinstance(val, str) and val:
                return val
            if isinstance(val, dict):
                text = val.get("text", "")
                if text:
                    return text
    return ""


# ===========================================================================
# FREEFORM SQL DETECTION
# ===========================================================================

def get_report_definition(session, report_id):
    result = {"source_type": "unknown", "sql": "", "db_source": "", "columns": []}
    try:
        data = session.get(f"model/reports/{report_id}")
        result["source_type"] = data.get("sourceType", "unknown")
        if result["source_type"] == "custom_sql_free_form":
            ds = data.get("dataSource", {})
            table = ds.get("table", {})
            phys = table.get("physicalTable", {})
            sql_expr = phys.get("sqlExpression", {})
            tree = sql_expr.get("tree", {})
            children = tree.get("children", [])
            if children:
                result["sql"] = children[0].get("variant", {}).get("value", "")
            if not result["sql"]:
                result["sql"] = sql_expr.get("text", "")
            result["columns"] = [c.get("name", "") for c in phys.get("columns", []) if c.get("name")]
            result["db_source"] = table.get("dataSource", {}).get("name", "")
    except Exception:
        pass
    return result


def is_freeform_sql(session, report_id):
    defn = get_report_definition(session, report_id)
    return defn["source_type"] == "custom_sql_free_form", defn


# ===========================================================================
# TABLE / COLUMN / DATASOURCE ENRICHMENT
# ===========================================================================

_datasource_cache = {}


def get_table_details(session, table_id):
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
            col_list.append(("(no columns returned)", ds_name or "N/A"))
        return col_list
    except Exception:
        return [("N/A", "N/A")]


def _get_datasource_name(session, ds_id):
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
# CUBE PUBLISHING — Delete old + Create new
# ===========================================================================

def _df_to_base64(df):
    rows = []
    for _, row in df.iterrows():
        row_data = {}
        for col in df.columns:
            val = row[col]
            row_data[col] = str(val) if pd.notna(val) else ""
        rows.append(row_data)
    return base64.b64encode(json.dumps(rows).encode("utf-8")).decode("utf-8")


def find_and_delete_existing(session, cube_name):
    try:
        results = session.get("searches/results",
                              params={"name": cube_name, "type": 3, "pattern": 4})
        items = results.get("result", results.get("results", []))
        if isinstance(items, list):
            for item in items:
                if item.get("name") == cube_name:
                    obj_id = item.get("id", "")
                    if obj_id:
                        session.delete(f"objects/{obj_id}", params={"type": 3})
                        log.info(f"Deleted existing cube '{cube_name}' (ID: {obj_id})")
                        return True
    except Exception as e:
        log.debug(f"Search/delete for '{cube_name}' failed: {e}")
    return False


def publish_cube(session, cube_name, df, folder_id, description=""):
    if df.empty:
        log.warning(f"Skipping cube '{cube_name}' — no data")
        return None

    table_name = "LINEAGE_DATA"
    find_and_delete_existing(session, cube_name)

    column_headers = [{"name": col, "dataType": "STRING"} for col in df.columns]
    attributes = []
    for col in df.columns:
        attributes.append({
            "name": col,
            "attributeForms": [{
                "category": "ID",
                "expressions": [{"formula": f"{table_name}.{col}"}],
                "dataType": "STRING",
            }],
        })

    data_b64 = _df_to_base64(df)
    create_body = {
        "name": cube_name,
        "description": description or f"Data Lineage - {cube_name}",
        "folderId": folder_id,
        "tables": [{"name": table_name, "columnHeaders": column_headers, "data": data_b64}],
        "attributes": attributes,
        "metrics": [],
    }

    try:
        resp = session.post("datasets", json=create_body)
        result = resp.json()
        dataset_id = result.get("datasetId", "")
        if not dataset_id:
            log.error(f"Failed to create '{cube_name}': {result}")
            return None
        log.info(f"Published '{cube_name}' (ID: {dataset_id}) — {len(df)} rows")
        return dataset_id
    except Exception as e:
        log.error(f"Failed to create '{cube_name}': {e}")
        try:
            log.error(f"Response: {e.response.text}")
        except Exception:
            pass
        return None


# ===========================================================================
# 5-LEVEL LINEAGE EXTRACTION (per project)
# ===========================================================================

def get_project_name(session, project_id):
    try:
        data = session.get("projects")
        projects = data if isinstance(data, list) else data.get("projects", [])
        for proj in projects:
            if proj.get("id") == project_id:
                return proj.get("name", project_id)
    except Exception:
        pass
    return project_id


def extract_lineage_for_project(prod_session, project_id,
                                 all_l1, all_l2, all_l3, all_l4, all_l5):
    prod_session.set_project(project_id)
    project_name = get_project_name(prod_session, project_id)
    log.info(f"{'='*60}")
    log.info(f"PROJECT: {project_name} ({project_id})")
    log.info(f"{'='*60}")

    # L1: Dashboards / Documents
    log.info("L1: Searching for Dashboards and Documents...")
    l1_objects = search_all_objects(prod_session, types=[TYPE_DOSSIER, TYPE_DOCUMENT])
    log.info(f"L1: Found {len(l1_objects)}")

    for obj in l1_objects:
        all_l1.append({
            "project_id": project_id, "project_name": project_name,
            "l1_id": obj["id"], "l1_name": obj["name"],
            "l1_type": type_name(obj["type"]), "l1_subtype": str(obj["subtype"]),
            "l1_owner": obj["owner"], "l1_date_modified": obj["dateModified"],
            "l1_path": obj["path"],
        })

    # L2: Datasets / Cubes
    log.info("L2: Finding datasets...")
    seen_l2 = {}
    for i, l1 in enumerate(l1_objects):
        log.info(f"  L2: {i+1}/{len(l1_objects)}: {l1['name']}")
        components = metadata_search(prod_session, used_by_id=l1["id"], used_by_type=l1["type"],
                                     result_types=[TYPE_REPORT, TYPE_OLAP_CUBE, TYPE_SUPER_CUBE])
        for comp in components:
            is_ffsql_flag, ffsql_defn = is_freeform_sql(prod_session, comp["id"])
            comp["is_freeform_sql"] = is_ffsql_flag
            comp["ffsql_defn"] = ffsql_defn if is_ffsql_flag else None
            all_l2.append({
                "project_id": project_id, "project_name": project_name,
                "l1_id": l1["id"], "l1_name": l1["name"],
                "l2_id": comp["id"], "l2_name": comp["name"],
                "l2_type": type_name(comp["type"]), "l2_subtype": str(comp["subtype"]),
                "l2_source_type": "Freeform SQL" if is_ffsql_flag else "Schema",
                "l2_date_modified": comp["dateModified"],
            })
            seen_l2[comp["id"]] = comp
    log.info(f"L2: {len(seen_l2)} unique datasets")

    # L3: Metrics / Attributes / Freeform SQL
    log.info("L3: Finding metrics/attributes...")
    seen_l3 = {}
    ffsql_l5_rows = []
    for i, l2 in enumerate(list(seen_l2.values())):
        log.info(f"  L3: {i+1}/{len(seen_l2)}: {l2['name']}")
        if l2.get("is_freeform_sql") and l2.get("ffsql_defn"):
            defn = l2["ffsql_defn"]
            all_l3.append({
                "project_id": project_id, "project_name": project_name,
                "l2_id": l2["id"], "l2_name": l2["name"],
                "l3_id": f"{l2['id']}_FFSQL", "l3_name": f"[Freeform SQL] {l2['name']}",
                "l3_type": "Freeform SQL", "l3_expression": defn["sql"],
                "l3_expression_source": "Report Definition",
            })
            for col_name in defn["columns"]:
                ffsql_l5_rows.append({
                    "project_id": project_id, "project_name": project_name,
                    "l4_id": f"{l2['id']}_FFSQL", "l4_name": f"[Freeform SQL] {l2['name']}",
                    "l5_table_id": f"{l2['id']}_FFSQL_TBL", "l5_table_name": "(from SQL query)",
                    "l5_column_name": col_name, "l5_db_source": defn["db_source"],
                })
            continue

        components = metadata_search(prod_session, used_by_id=l2["id"], used_by_type=l2["type"],
                                     result_types=[TYPE_METRIC, TYPE_ATTRIBUTE])
        for comp in components:
            if comp["type"] == TYPE_METRIC:
                expr, source = get_metric_expression(prod_session, comp["id"])
            else:
                expr, source = get_attribute_expression(prod_session, comp["id"])
            all_l3.append({
                "project_id": project_id, "project_name": project_name,
                "l2_id": l2["id"], "l2_name": l2["name"],
                "l3_id": comp["id"], "l3_name": comp["name"],
                "l3_type": type_name(comp["type"]), "l3_expression": expr,
                "l3_expression_source": source,
            })
            seen_l3[comp["id"]] = comp

    # L4: Facts / Schema definitions
    log.info("L4: Finding facts/schema objects...")
    seen_l4 = {}
    for i, l3 in enumerate(list(seen_l3.values())):
        log.info(f"  L4: {i+1}/{len(seen_l3)}: {l3['name']}")
        result_types = [TYPE_FACT, TYPE_ATTRIBUTE] if l3["type"] == TYPE_METRIC else [TYPE_FACT]
        components = metadata_search(prod_session, used_by_id=l3["id"], used_by_type=l3["type"],
                                     result_types=result_types)
        for comp in components:
            if comp["type"] == TYPE_FACT:
                expr, source = get_fact_expression(prod_session, comp["id"])
            else:
                expr, source = get_attribute_expression(prod_session, comp["id"])
            all_l4.append({
                "project_id": project_id, "project_name": project_name,
                "l3_id": l3["id"], "l3_name": l3["name"],
                "l4_id": comp["id"], "l4_name": comp["name"],
                "l4_type": type_name(comp["type"]), "l4_expression": expr,
                "l4_expression_source": source,
            })
            seen_l4[comp["id"]] = comp

    # L5: Tables / Columns / DB Sources
    log.info("L5: Finding tables...")
    l5_count = 0
    for i, l4 in enumerate(list(seen_l4.values())):
        log.info(f"  L5: {i+1}/{len(seen_l4)}: {l4['name']}")
        tables = metadata_search(prod_session, used_by_id=l4["id"], used_by_type=l4["type"],
                                 result_types=[TYPE_TABLE])
        for tbl in tables:
            for col_name, db_source in get_table_details(prod_session, tbl["id"]):
                all_l5.append({
                    "project_id": project_id, "project_name": project_name,
                    "l4_id": l4["id"], "l4_name": l4["name"],
                    "l5_table_id": tbl["id"], "l5_table_name": tbl["name"],
                    "l5_column_name": col_name, "l5_db_source": db_source,
                })
                l5_count += 1

    all_l5.extend(ffsql_l5_rows)
    l5_count += len(ffsql_l5_rows)
    log.info(f"  Project done: L1={len(l1_objects)} L2={len(seen_l2)} L3={len(seen_l3)} L4={len(seen_l4)} L5={l5_count}")


# ===========================================================================
# MAIN
# ===========================================================================

def main():
    parser = argparse.ArgumentParser(description="5-Level Data Lineage")
    parser.add_argument("--project-ids", type=str, default=None,
                        help="Comma-separated PROD project IDs (overrides .env)")
    args = parser.parse_args()

    prod_config = get_prod_config()
    dev_config = get_dev_config()

    project_ids = ([p.strip() for p in args.project_ids.split(",") if p.strip()]
                   if args.project_ids else get_prod_project_ids())

    if not project_ids:
        log.error("No project IDs. Use --project-ids or set MSTR_PROD_PROJECT_IDS in .env")
        sys.exit(1)

    folder_id = dev_config.get("folder_id", "")
    if not folder_id:
        log.error("MSTR_DEV_FOLDER_ID not set in .env")
        sys.exit(1)

    log.info("Starting 5-Level Data Lineage Extraction")
    log.info(f"PROD: {prod_config['base_url']}")
    log.info(f"DEV:  {dev_config['base_url']}")
    log.info(f"Projects: {len(project_ids)}")
    log.info(f"Output: 5 cubes in folder {folder_id}")

    all_l1, all_l2, all_l3, all_l4, all_l5 = [], [], [], [], []

    prod_session = StrategySession.from_config(prod_config)
    dev_session = StrategySession.from_config(dev_config, project_id=dev_config.get("project_id"))

    try:
        prod_session.login()
        dev_session.login()

        for idx, project_id in enumerate(project_ids):
            log.info(f"\n>>> Project {idx+1}/{len(project_ids)} <<<")
            try:
                extract_lineage_for_project(prod_session, project_id,
                                            all_l1, all_l2, all_l3, all_l4, all_l5)
            except Exception as e:
                log.error(f"Failed project {project_id}: {e}")
                import traceback
                traceback.print_exc()

        log.info(f"\n{'#'*60}")
        log.info("PUBLISHING 5 CUBES TO DEV SERVER")
        log.info(f"{'#'*60}")

        for level, rows in [("L1", all_l1), ("L2", all_l2), ("L3", all_l3),
                             ("L4", all_l4), ("L5", all_l5)]:
            df = pd.DataFrame(rows) if rows else pd.DataFrame()
            publish_cube(dev_session, CUBE_NAMES[level], df, folder_id,
                         f"Data Lineage {level} — {len(project_ids)} projects")

        log.info(f"\nDONE — {len(project_ids)} projects → 5 cubes")
        log.info(f"  L1:{len(all_l1)} L2:{len(all_l2)} L3:{len(all_l3)} L4:{len(all_l4)} L5:{len(all_l5)}")

    finally:
        prod_session.logout()
        dev_session.logout()


if __name__ == "__main__":
    main()
