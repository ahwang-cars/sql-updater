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

def find_tds_in_zip(zip_path: str) -> str:
    """Return the name of the .tds file inside a .tdsx archive."""
    with zipfile.ZipFile(zip_path, "r") as z:
        for name in z.namelist():
            if name.lower().endswith(".tds"):
                return name
    raise FileNotFoundError("No .tds file found inside the .tdsx archive")


def parse_tds(zip_path: str, tds_name: str) -> tuple[ET.ElementTree, str]:
    """Extract and parse the .tds XML from a .tdsx ZIP. Returns (tree, raw_xml)."""
    with zipfile.ZipFile(zip_path, "r") as z:
        raw = z.read(tds_name)
    tree = ET.ElementTree(ET.fromstring(raw))
    return tree, raw.decode("utf-8")


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


def repackage_tdsx(
    original_zip_path: str,
    tds_name: str,
    tree: ET.ElementTree,
    output_path: str,
):
    """Create a new .tdsx ZIP with the modified .tds, preserving all other files."""
    buf = io.StringIO()
    tree.write(buf, encoding="unicode", xml_declaration=True)
    modified_tds = buf.getvalue().encode("utf-8")

    with zipfile.ZipFile(original_zip_path, "r") as z_in, \
         zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as z_out:
        for item in z_in.infolist():
            if item.filename == tds_name:
                z_out.writestr(item, modified_tds)
            else:
                z_out.writestr(item, z_in.read(item.filename))


def inspect_datasource(zip_path: str):
    """Print a summary of the data source's SQL configuration."""
    tds_name = find_tds_in_zip(zip_path)
    tree, _ = parse_tds(zip_path, tds_name)
    root = tree.getroot()

    print(f"\n{'='*70}")
    print(f"  Data Source Inspection: {os.path.basename(zip_path)}")
    print(f"  TDS file: {tds_name}")
    print(f"{'='*70}")

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


def download_datasource(server: TSC.Server, datasource_id: str, dest_dir: str) -> str:
    """Download a data source as .tdsx and return the file path."""
    ds = server.datasources.get_by_id(datasource_id)
    print(f"Downloading data source: {ds.name} (id={datasource_id})")
    path = server.datasources.download(ds.id, filepath=dest_dir, include_extract=True)
    print(f"Downloaded to: {path}")
    return path


def publish_datasource(server: TSC.Server, datasource_id: str, file_path: str):
    """Publish the modified .tdsx back, overwriting the existing data source."""
    original = server.datasources.get_by_id(datasource_id)
    ds_item = TSC.DatasourceItem(project_id=original.project_id, name=original.name)

    print(f"Publishing '{original.name}' (overwrite)...")
    result = server.datasources.publish(
        ds_item,
        file_path,
        mode=TSC.Server.PublishMode.Overwrite,
    )
    print(f"Published successfully. Datasource ID: {result.id}")
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

    # Datasource — by name or ID
    ds = parser.add_argument_group("datasource (use --datasource-name or --datasource-id)")
    ds.add_argument("--datasource-name", help="Datasource name (will look up ID automatically)")
    ds.add_argument("--datasource-id", help="Datasource UUID (faster, skips name lookup)")

    # SQL files
    parser.add_argument("--custom-sql-file", help="Path to .sql file with new Custom SQL")
    parser.add_argument("--initial-sql-file", help="Path to .sql file with new Initial SQL")
    parser.add_argument("--relation-name", help="Only update the Custom SQL relation with this name")
    parser.add_argument("--remove-initial-sql", action="store_true",
                        help="Remove Initial SQL entirely")

    # Run modes
    parser.add_argument("--inspect-only", action="store_true",
                        help="Download and show current SQL without modifying")
    parser.add_argument("--dry-run", action="store_true",
                        help="Modify locally but do NOT publish back")
    parser.add_argument("--output-dir", help="Save modified .tdsx to this directory")
    parser.add_argument("--local-tdsx", help="Use a local .tdsx file instead of downloading")

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

    # --- Validate action ---
    if not args.custom_sql_file and not args.initial_sql_file \
       and not args.inspect_only and not args.remove_initial_sql:
        parser.error("Provide at least one of: --custom-sql-file, --initial-sql-file, "
                     "--remove-initial-sql, or --inspect-only")

    if not args.datasource_name and not args.datasource_id and not args.local_tdsx:
        parser.error("Provide --datasource-name, --datasource-id, or --local-tdsx")

    # --- Step 1: Get the .tdsx ---
    tmpdir = tempfile.mkdtemp(prefix="tableau_sql_")

    if args.local_tdsx:
        tdsx_path = args.local_tdsx
        print(f"Using local file: {tdsx_path}")
        server = None
        datasource_id = args.datasource_id
    else:
        server = connect(server_url, site_id, token_name, token_value)

        # Resolve datasource ID from name if needed
        datasource_id = args.datasource_id
        if not datasource_id:
            datasource_id = find_datasource_by_name(server, args.datasource_name)

        tdsx_path = download_datasource(server, datasource_id, tmpdir)

    # --- Step 2: Inspect only ---
    if args.inspect_only:
        inspect_datasource(tdsx_path)
        return

    # --- Step 3: Parse and modify ---
    tds_name = find_tds_in_zip(tdsx_path)
    tree, _ = parse_tds(tdsx_path, tds_name)
    root = tree.getroot()

    changes_made = False

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

    # --- Step 4: Repackage as .tdsx ---
    output_dir = args.output_dir or tmpdir
    modified_path = os.path.join(output_dir, f"modified_{os.path.basename(tdsx_path)}")
    repackage_tdsx(tdsx_path, tds_name, tree, modified_path)
    print(f"Modified .tdsx saved to: {modified_path}")

    inspect_datasource(modified_path)

    # --- Step 5: Publish (unless dry-run) ---
    if args.dry_run:
        print("DRY RUN: Skipping publish. Review the modified file above.")
        return

    if args.local_tdsx:
        server = connect(server_url, site_id, token_name, token_value)

    publish_datasource(server, datasource_id, modified_path)

    if not args.output_dir:
        shutil.rmtree(tmpdir, ignore_errors=True)

    if server:
        server.auth.sign_out()

    print("Done!")


if __name__ == "__main__":
    main()
