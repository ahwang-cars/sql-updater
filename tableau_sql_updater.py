#!/usr/bin/env python3
"""
Tableau Data Source SQL Updater
-------------------------------
Programmatically updates Custom SQL and/or Initial SQL in a Tableau Online
data source without opening Tableau Desktop (which can take 30+ minutes).

Workflow:
  1. Authenticate to Tableau Online via Personal Access Token
  2. Download the .tdsx data source
  3. Extract and parse the inner .tds XML
  4. Replace Custom SQL (<relation type="text">) and/or Initial SQL (one-time-sql attr)
  5. Repackage the .tdsx and publish back with Overwrite mode

Requirements:
  pip install tableauserverclient

Usage (with config file):
  python tableau_sql_updater.py \
    --config config.json \
    --datasource-name "DI 13mo Daily DigAd Summary Dealer Performance" \
    --custom-sql-file updated_query.sql \
    --dry-run

Usage (with explicit credentials):
  python tableau_sql_updater.py \
    --token-name "MY_PAT_NAME" \
    --token-value "MY_PAT_VALUE" \
    --datasource-id "76595187-2cbc-4f88-ba72-ba162f734bf5" \
    --custom-sql-file updated_query.sql \
    --dry-run
"""

from __future__ import annotations

import argparse
import difflib
import io
import json
import os
import re
import shutil
import sys
import tempfile
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

try:
    import tableauserverclient as TSC
except ImportError:
    print("ERROR: tableauserverclient not installed. Run: pip install tableauserverclient")
    sys.exit(1)


# ---------------------------------------------------------------------------
# XML helpers
# ---------------------------------------------------------------------------

def find_xml_in_zip(zip_path: str, extension: str) -> str:
    """Return the name of the first file matching the given extension inside a ZIP."""
    with zipfile.ZipFile(zip_path, "r") as z:
        for name in z.namelist():
            if name.lower().endswith(extension.lower()):
                return name
    raise FileNotFoundError(f"No {extension} file found inside {zip_path}")


def find_tds_in_zip(zip_path: str) -> str:
    """Return the name of the .tds file inside a .tdsx archive."""
    return find_xml_in_zip(zip_path, ".tds")


def find_twb_in_zip(zip_path: str) -> str:
    """Return the name of the .twb file inside a .twbx archive."""
    return find_xml_in_zip(zip_path, ".twb")


def parse_xml_from_zip(zip_path: str, xml_name: str) -> tuple[ET.ElementTree, str]:
    """Extract and parse an XML file from a ZIP. Returns (tree, raw_xml)."""
    with zipfile.ZipFile(zip_path, "r") as z:
        raw = z.read(xml_name)
    tree = ET.ElementTree(ET.fromstring(raw))
    return tree, raw.decode("utf-8")


def parse_tds(zip_path: str, tds_name: str) -> tuple[ET.ElementTree, str]:
    """Extract and parse the .tds XML from a .tdsx ZIP. Returns (tree, raw_xml)."""
    return parse_xml_from_zip(zip_path, tds_name)


def update_custom_sql(root: ET.Element, new_sql: str, relation_name: str | None = None) -> int:
    """
    Replace the text content of <relation type='text'> elements.
    If relation_name is given, only replace matching relation(s).
    Returns the number of relations updated.
    """
    count = 0
    for rel in root.iter("relation"):
        if rel.get("type") == "text":
            if relation_name and rel.get("name") != relation_name:
                continue
            rel.text = new_sql
            count += 1
    return count


def embed_connection_credentials(root: ET.Element, username: str, password: str) -> int:
    """
    Embed database credentials directly in <connection> elements.
    Sets username, password, and switches workgroup-auth-mode from 'prompt'
    to 'username-password' so Bridge uses the embedded credentials.
    Returns the number of connections updated.
    """
    count = 0
    for conn in root.iter("connection"):
        cls = conn.get("class", "")
        if cls in ("federated", "hyper", ""):
            continue
        conn.set("username", username)
        conn.set("password", password)
        if conn.get("workgroup-auth-mode") == "prompt":
            conn.set("workgroup-auth-mode", "username-password")
        count += 1
    return count


def switch_to_table(root: ET.Element, table_ref: str, relation_name: str | None = None) -> int:
    """
    Convert <relation type='text'> (Custom SQL) elements to <relation type='table'>
    pointing at a Redshift view or table.

    table_ref should be 'schema.tablename' or 'tablename' (no schema).
    Tableau's table attribute format is '[schema].[tablename]'.

    If relation_name is given, only convert matching relation(s).
    Returns the number of relations converted.
    """
    # Parse schema and table from dot-separated input, strip any existing brackets
    table_ref_clean = table_ref.replace("[", "").replace("]", "")
    parts = table_ref_clean.split(".", 1)
    if len(parts) == 2:
        schema, table = parts
        tableau_table_attr = f"[{schema}].[{table}]"
        relation_display_name = table
    else:
        table = parts[0]
        tableau_table_attr = f"[{table}]"
        relation_display_name = table

    count = 0
    for rel in root.iter("relation"):
        if rel.get("type") == "text":
            if relation_name and rel.get("name") != relation_name:
                continue
            rel.set("type", "table")
            rel.set("table", tableau_table_attr)
            rel.set("name", relation_display_name)
            rel.text = None
            count += 1
    return count


def update_initial_sql(root: ET.Element, new_sql: str) -> int:
    """
    Replace the one-time-sql attribute on <connection> elements.
    Returns the number of connections updated.
    """
    count = 0
    for conn in root.iter("connection"):
        if conn.get("one-time-sql") is not None or count == 0:
            conn.set("one-time-sql", new_sql)
            count += 1
    return count


def remove_initial_sql(root: ET.Element) -> int:
    """Remove the one-time-sql attribute from all <connection> elements."""
    count = 0
    for conn in root.iter("connection"):
        if "one-time-sql" in conn.attrib:
            del conn.attrib["one-time-sql"]
            count += 1
    return count


def repackage_zip(
    original_zip_path: str,
    xml_name: str,
    tree: ET.ElementTree,
    output_path: str,
):
    """Create a new ZIP with the modified XML file, preserving all other files."""
    buf = io.StringIO()
    tree.write(buf, encoding="unicode", xml_declaration=True)
    modified_xml = buf.getvalue().encode("utf-8")

    with zipfile.ZipFile(original_zip_path, "r") as z_in, \
         zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as z_out:
        for item in z_in.infolist():
            if item.filename == xml_name:
                z_out.writestr(item, modified_xml)
            else:
                z_out.writestr(item, z_in.read(item.filename))


# Backward-compatible alias
repackage_tdsx = repackage_zip


def _print_sql_summary(root: ET.Element):
    """Print connections, custom SQL, and table relations from an XML root."""
    for conn in root.iter("connection"):
        otsql = conn.get("one-time-sql")
        if otsql:
            print(f"\n--- Initial SQL (one-time-sql) ---")
            print(otsql[:500] + ("..." if len(otsql) > 500 else ""))

    for rel in root.iter("relation"):
        if rel.get("type") == "text":
            name = rel.get("name", "(unnamed)")
            sql = (rel.text or "").strip()
            print(f"\n--- Custom SQL: '{name}' ---")
            print(sql[:500] + ("..." if len(sql) > 500 else ""))
        elif rel.get("type") == "table":
            name = rel.get("name", "(unnamed)")
            table = rel.get("table", "(unknown)")
            print(f"\n--- Table/View: '{name}' -> {table} ---")


def _normalize_sql(sql: str) -> str:
    """Collapse whitespace for comparison — ignores indentation/line-break differences."""
    return re.sub(r"\s+", " ", sql).strip()


def validate_custom_sql(root: ET.Element, expected_sql: str,
                        relation_name: str | None = None) -> tuple[bool, list[str]]:
    """
    Compare each <relation type='text'> against expected_sql (whitespace-normalized).
    Returns (all_match, messages). Messages include per-relation MATCH/MISMATCH lines
    and a unified diff on mismatch. If no Custom SQL relations are found, returns
    (False, [...]) since the datasource isn't using Custom SQL.
    """
    messages = []
    found = 0
    mismatches = 0
    expected_norm = _normalize_sql(expected_sql)

    for rel in root.iter("relation"):
        if rel.get("type") != "text":
            continue
        if relation_name and rel.get("name") != relation_name:
            continue
        found += 1
        name = rel.get("name", "(unnamed)")
        actual = rel.text or ""
        if _normalize_sql(actual) == expected_norm:
            messages.append(f"MATCH: relation '{name}'")
        else:
            mismatches += 1
            diff = difflib.unified_diff(
                expected_sql.splitlines(),
                actual.splitlines(),
                fromfile="expected",
                tofile=f"actual ({name})",
                lineterm="",
            )
            messages.append(f"MISMATCH: relation '{name}'")
            messages.extend(diff)

    if found == 0:
        messages.append("No Custom SQL relations found in datasource.")
        return False, messages

    return mismatches == 0, messages


def inspect_datasource(zip_path: str):
    """Print a summary of the data source's SQL configuration."""
    tds_name = find_tds_in_zip(zip_path)
    tree, _ = parse_tds(zip_path, tds_name)
    root = tree.getroot()

    print(f"\n{'='*70}")
    print(f"  Data Source Inspection: {os.path.basename(zip_path)}")
    print(f"  TDS file: {tds_name}")
    print(f"{'='*70}")
    _print_sql_summary(root)
    print(f"\n{'='*70}\n")


def inspect_workbook(zip_path: str):
    """Print a summary of the workbook's SQL configuration."""
    twb_name = find_twb_in_zip(zip_path)
    tree, _ = parse_xml_from_zip(zip_path, twb_name)
    root = tree.getroot()

    print(f"\n{'='*70}")
    print(f"  Workbook Inspection: {os.path.basename(zip_path)}")
    print(f"  TWB file: {twb_name}")
    print(f"{'='*70}")
    _print_sql_summary(root)
    print(f"\n{'='*70}\n")


# ---------------------------------------------------------------------------
# Tableau Server Client helpers
# ---------------------------------------------------------------------------

def load_config(config_path: str) -> dict:
    """Load credentials from a config.json file."""
    with open(config_path) as f:
        return json.load(f)


def connect(server_url: str, site_id: str, token_name: str, token_value: str) -> TSC.Server:
    """Authenticate and return a connected TSC.Server instance."""
    server = TSC.Server(server_url, use_server_version=True)
    auth = TSC.PersonalAccessTokenAuth(token_name, token_value, site_id=site_id)
    server.auth.sign_in(auth)
    print(f"Signed in to {server_url} (site: {site_id})")
    return server


def find_datasource_by_name(server: TSC.Server, name: str) -> str:
    """Look up a datasource UUID by name. Raises if not found or ambiguous."""
    matches = []
    for ds in TSC.Pager(server.datasources):
        if ds.name.lower() == name.lower():
            matches.append(ds)

    if not matches:
        raise ValueError(f"No datasource found with name: '{name}'")
    if len(matches) > 1:
        options = "\n".join(f"  {ds.id}  ({ds.project_name})" for ds in matches)
        raise ValueError(
            f"Multiple datasources named '{name}'. Use --datasource-id with one of:\n{options}"
        )
    print(f"Found datasource: '{matches[0].name}' (id={matches[0].id})")
    return matches[0].id


def find_workbook_by_name(server: TSC.Server, name: str) -> str:
    """Look up a workbook UUID by name. Raises if not found or ambiguous."""
    matches = []
    for wb in TSC.Pager(server.workbooks):
        if wb.name.lower() == name.lower():
            matches.append(wb)

    if not matches:
        raise ValueError(f"No workbook found with name: '{name}'")
    if len(matches) > 1:
        options = "\n".join(f"  {wb.id}  ({wb.project_name})" for wb in matches)
        raise ValueError(
            f"Multiple workbooks named '{name}'. Use --workbook-id with one of:\n{options}"
        )
    print(f"Found workbook: '{matches[0].name}' (id={matches[0].id})")
    return matches[0].id


def download_workbook(server: TSC.Server, workbook_id: str, dest_dir: str) -> str:
    """Download a workbook as .twbx and return the file path."""
    wb = server.workbooks.get_by_id(workbook_id)
    print(f"Downloading workbook: {wb.name} (id={workbook_id})")
    path = server.workbooks.download(wb.id, filepath=dest_dir, include_extract=True)
    print(f"Downloaded to: {path}")
    return path


def publish_workbook(
    server: TSC.Server,
    workbook_id: str,
    file_path: str,
    db_username: str | None = None,
    db_password: str | None = None,
):
    """Publish the modified .twbx back, overwriting the existing workbook.
    If db_username/db_password are provided, re-apply them to all connections after publish.
    """
    original = server.workbooks.get_by_id(workbook_id)
    wb_item = TSC.WorkbookItem(project_id=original.project_id, name=original.name)

    print(f"Publishing workbook '{original.name}' (overwrite)...")
    result = server.workbooks.publish(
        wb_item,
        file_path,
        mode=TSC.Server.PublishMode.Overwrite,
    )
    print(f"Published successfully. Workbook ID: {result.id}")

    if db_username and db_password:
        server.workbooks.populate_connections(result)
        updated = 0
        for conn in result.connections:
            conn.username = db_username
            conn.password = db_password
            conn.embed_password = True
            server.workbooks.update_connection(result, conn)
            updated += 1
        print(f"Re-applied database credentials to {updated} connection(s)")

    return result


def download_datasource(server: TSC.Server, datasource_id: str, dest_dir: str) -> str:
    """Download a data source as .tdsx and return the file path."""
    ds = server.datasources.get_by_id(datasource_id)
    print(f"Downloading data source: {ds.name} (id={datasource_id})")
    path = server.datasources.download(ds.id, filepath=dest_dir, include_extract=True)
    print(f"Downloaded to: {path}")
    return path


def publish_datasource(
    server: TSC.Server,
    datasource_id: str,
    file_path: str,
    db_username: str | None = None,
    db_password: str | None = None,
):
    """Publish the modified .tdsx back, overwriting the existing data source.
    If db_username/db_password are provided, re-apply them to all connections after publish.
    """
    original = server.datasources.get_by_id(datasource_id)
    ds_item = TSC.DatasourceItem(project_id=original.project_id, name=original.name)

    print(f"Publishing '{original.name}' (overwrite)...")
    result = server.datasources.publish(
        ds_item,
        file_path,
        mode=TSC.Server.PublishMode.Overwrite,
    )
    print(f"Published successfully. Datasource ID: {result.id}")

    if db_username and db_password:
        server.datasources.populate_connections(result)
        updated = 0
        for conn in result.connections:
            conn.username = db_username
            conn.password = db_password
            conn.embed_password = True
            server.datasources.update_connection(result, conn)
            updated += 1
        print(f"Re-applied database credentials to {updated} connection(s)")

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Update Custom SQL and/or Initial SQL in a Tableau Online data source."
    )

    # Credentials — either via config file or explicit flags
    creds = parser.add_argument_group("credentials (use --config or explicit flags)")
    creds.add_argument("--config", help="Path to config.json with credentials")
    creds.add_argument("--server", default="https://us-west-2b.online.tableau.com",
                       help="Tableau Server URL (default: cars server)")
    creds.add_argument("--site", default="cars", help="Tableau site content URL (default: cars)")
    creds.add_argument("--token-name", help="Personal Access Token name")
    creds.add_argument("--token-value", help="Personal Access Token secret")

    # Target — datasource or workbook, by name or ID
    ds = parser.add_argument_group("target (datasource or workbook, by name or ID)")
    ds.add_argument("--datasource-name", help="Datasource name (will look up ID automatically)")
    ds.add_argument("--datasource-id", help="Datasource UUID (faster, skips name lookup)")
    ds.add_argument("--workbook-name", help="Workbook name (will look up ID automatically)")
    ds.add_argument("--workbook-id", help="Workbook UUID (faster, skips name lookup)")

    # Database credentials (re-applied after publish to preserve connection)
    db = parser.add_argument_group("database credentials (re-applied after publish)")
    db.add_argument("--db-username", help="Database username for the datasource connection")
    db.add_argument("--db-password", help="Database password for the datasource connection")

    # SQL files
    parser.add_argument("--custom-sql-file", help="Path to .sql file with new Custom SQL")
    parser.add_argument("--initial-sql-file", help="Path to .sql file with new Initial SQL")
    parser.add_argument("--relation-name", help="Only update the Custom SQL relation with this name")
    parser.add_argument("--switch-to-table",
                        help="Convert Custom SQL to a direct table/view connection. "
                             "Provide as 'schema.viewname' or 'viewname'")
    parser.add_argument("--remove-initial-sql", action="store_true",
                        help="Remove Initial SQL entirely")

    # Run modes
    parser.add_argument("--inspect-only", action="store_true",
                        help="Download and show current SQL without modifying")
    parser.add_argument("--validate-sql",
                        help="Path to .sql file; download datasource and verify each "
                             "Custom SQL relation matches this file (whitespace-normalized). "
                             "Exits 0 on match, 1 on mismatch.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Modify locally but do NOT publish back")
    parser.add_argument("--output-dir", help="Save modified file to this directory")
    parser.add_argument("--local-tdsx", help="Use a local .tdsx file instead of downloading")
    parser.add_argument("--local-twbx", help="Use a local .twbx file instead of downloading")

    args = parser.parse_args()

    # --- Resolve credentials ---
    token_name = args.token_name
    token_value = args.token_value
    server_url = args.server
    site_id = args.site

    if args.config:
        cfg = load_config(args.config)
        # Look for a matching site section, fall back to tableau_server
        site_cfg = None
        for key, val in cfg.items():
            if isinstance(val, dict) and val.get("site_id") == site_id:
                site_cfg = val
                break
        if site_cfg is None:
            site_cfg = cfg.get("tableau_server", {})

        token_name = token_name or site_cfg.get("token_name")
        token_value = token_value or site_cfg.get("token_secret")
        server_url = server_url or site_cfg.get("server_url", "https://us-west-2b.online.tableau.com")

    if not token_name or not token_value:
        parser.error("Provide credentials via --config or --token-name/--token-value")

    # --- Resolve database credentials ---
    db_username = args.db_username
    db_password = args.db_password

    if args.config and (not db_username or not db_password):
        cfg = load_config(args.config)
        db_cfg = cfg.get("connection_credentials", {})
        db_username = db_username or db_cfg.get("username")
        db_password = db_password or db_cfg.get("password")

    # --- Validate action ---
    if not args.custom_sql_file and not args.initial_sql_file \
       and not args.inspect_only and not args.remove_initial_sql \
       and not args.switch_to_table and not args.validate_sql:
        parser.error("Provide at least one of: --custom-sql-file, --initial-sql-file, "
                     "--remove-initial-sql, --switch-to-table, --validate-sql, or --inspect-only")

    # Determine mode: workbook vs datasource
    workbook_mode = bool(args.workbook_name or args.workbook_id or args.local_twbx)
    datasource_mode = bool(args.datasource_name or args.datasource_id or args.local_tdsx)

    if workbook_mode and datasource_mode:
        parser.error("Specify either a workbook or a datasource target, not both")
    if not workbook_mode and not datasource_mode:
        parser.error("Provide a target: --datasource-name, --datasource-id, --local-tdsx, "
                     "--workbook-name, --workbook-id, or --local-twbx")

    # --- Step 1: Get the file ---
    tmpdir = tempfile.mkdtemp(prefix="tableau_sql_")
    server = None
    target_id = None

    if workbook_mode:
        if args.local_twbx:
            zip_path = args.local_twbx
            print(f"Using local file: {zip_path}")
            target_id = args.workbook_id
        else:
            server = connect(server_url, site_id, token_name, token_value)
            target_id = args.workbook_id
            if not target_id:
                target_id = find_workbook_by_name(server, args.workbook_name)
            zip_path = download_workbook(server, target_id, tmpdir)
    else:
        if args.local_tdsx:
            zip_path = args.local_tdsx
            print(f"Using local file: {zip_path}")
            target_id = args.datasource_id
        else:
            server = connect(server_url, site_id, token_name, token_value)
            target_id = args.datasource_id
            if not target_id:
                target_id = find_datasource_by_name(server, args.datasource_name)
            zip_path = download_datasource(server, target_id, tmpdir)

    # --- Step 2: Inspect only ---
    if args.inspect_only:
        if workbook_mode:
            inspect_workbook(zip_path)
        else:
            inspect_datasource(zip_path)
        return

    # --- Step 2b: Validate Custom SQL against an expected file ---
    if args.validate_sql:
        xml_name = find_twb_in_zip(zip_path) if workbook_mode else find_tds_in_zip(zip_path)
        tree, _ = parse_xml_from_zip(zip_path, xml_name)
        expected = Path(args.validate_sql).read_text(encoding="utf-8").strip()
        all_match, messages = validate_custom_sql(tree.getroot(), expected, args.relation_name)
        print(f"\n{'='*70}")
        print(f"  Validating Custom SQL against: {args.validate_sql}")
        print(f"{'='*70}")
        for line in messages:
            print(line)
        print(f"{'='*70}")
        if all_match:
            print("RESULT: all Custom SQL relations match expected.")
            return
        print("RESULT: mismatch detected.")
        sys.exit(1)

    # --- Step 3: Parse and modify ---
    if workbook_mode:
        xml_name = find_twb_in_zip(zip_path)
    else:
        xml_name = find_tds_in_zip(zip_path)

    tree, _ = parse_xml_from_zip(zip_path, xml_name)
    root = tree.getroot()

    changes_made = False

    if db_username and db_password:
        n = embed_connection_credentials(root, db_username, db_password)
        print(f"Embedded database credentials in {n} connection(s)")
        changes_made = n > 0

    if args.switch_to_table:
        n = switch_to_table(root, args.switch_to_table, args.relation_name)
        print(f"Converted {n} Custom SQL relation(s) to table: {args.switch_to_table}")
        changes_made = n > 0

    if args.custom_sql_file:
        new_sql = Path(args.custom_sql_file).read_text(encoding="utf-8").strip()
        n = update_custom_sql(root, new_sql, args.relation_name)
        print(f"Updated {n} Custom SQL relation(s)")
        changes_made = n > 0

    if args.initial_sql_file:
        new_initial = Path(args.initial_sql_file).read_text(encoding="utf-8").strip()
        n = update_initial_sql(root, new_initial)
        print(f"Updated Initial SQL on {n} connection(s)")
        changes_made = n > 0

    if args.remove_initial_sql:
        n = remove_initial_sql(root)
        print(f"Removed Initial SQL from {n} connection(s)")
        changes_made = n > 0

    if not changes_made:
        print("WARNING: No changes were applied. Check your arguments.")
        return

    # --- Step 4: Repackage ---
    output_dir = args.output_dir or tmpdir
    modified_path = os.path.join(output_dir, f"modified_{os.path.basename(zip_path)}")
    repackage_zip(zip_path, xml_name, tree, modified_path)
    print(f"Modified file saved to: {modified_path}")

    if workbook_mode:
        inspect_workbook(modified_path)
    else:
        inspect_datasource(modified_path)

    # --- Step 5: Publish (unless dry-run) ---
    if args.dry_run:
        print("DRY RUN: Skipping publish. Review the modified file above.")
        return

    if workbook_mode:
        if args.local_twbx:
            server = connect(server_url, site_id, token_name, token_value)
        publish_workbook(server, target_id, modified_path, db_username, db_password)
    else:
        if args.local_tdsx:
            server = connect(server_url, site_id, token_name, token_value)
        publish_datasource(server, target_id, modified_path, db_username, db_password)

    if not args.output_dir:
        shutil.rmtree(tmpdir, ignore_errors=True)

    if server:
        server.auth.sign_out()

    print("Done!")


if __name__ == "__main__":
    main()
