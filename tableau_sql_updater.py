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

Usage:
  python tableau_sql_updater.py \
    --server https://us-west-2b.online.tableau.com \
    --site cars \
    --token-name "MY_PAT_NAME" \
    --token-value "MY_PAT_VALUE" \
    --datasource-id 101217312 \
    --custom-sql-file updated_query.sql \
    --initial-sql-file initial_sql.sql \
    --dry-run
"""

import argparse
import io
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
    # Preserve original encoding declaration
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
    # Write modified XML to bytes
    buf = io.BytesIO()
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

    # Initial SQL
    for conn in root.iter("connection"):
        otsql = conn.get("one-time-sql")
        if otsql:
            print(f"\n--- Initial SQL (one-time-sql) ---")
            print(otsql[:500] + ("..." if len(otsql) > 500 else ""))

    # Custom SQL relations
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

def connect(server_url: str, site_id: str, token_name: str, token_value: str) -> TSC.Server:
    """Authenticate and return a connected TSC.Server instance."""
    server = TSC.Server(server_url, use_server_version=True)
    auth = TSC.PersonalAccessTokenAuth(token_name, token_value, site_id=site_id)
    server.auth.sign_in(auth)
    print(f"Signed in to {server_url} (site: {site_id})")
    return server


def download_datasource(server: TSC.Server, datasource_id: str, dest_dir: str) -> str:
    """Download a data source as .tdsx and return the file path."""
    ds = server.datasources.get_by_id(datasource_id)
    print(f"Downloading data source: {ds.name} (id={datasource_id})")
    path = server.datasources.download(ds.id, filepath=dest_dir, include_extract=False)
    print(f"Downloaded to: {path}")
    return path


def publish_datasource(
    server: TSC.Server,
    datasource_id: str,
    file_path: str,
):
    """Publish the modified .tdsx back, overwriting the existing data source."""
    # Get original datasource metadata to preserve project, name, etc.
    original = server.datasources.get_by_id(datasource_id)
    ds_item = TSC.DatasourceItem(project_id=original.project_id, name=original.name)

    print(f"Publishing {file_path} as '{original.name}' (overwrite)...")
    result = server.datasources.publish(
        ds_item,
        file_path,
        mode=TSC.Server.PublishMode.Overwrite,
        connection_credentials=None,
    )
    print(f"Published successfully. New datasource ID: {result.id}")
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Update Custom SQL and/or Initial SQL in a Tableau Online data source."
    )
    parser.add_argument("--server", default="https://us-west-2b.online.tableau.com",
                        help="Tableau Server URL")
    parser.add_argument("--site", default="cars", help="Tableau site content URL")
    parser.add_argument("--token-name", required=True, help="Personal Access Token name")
    parser.add_argument("--token-value", required=True, help="Personal Access Token value")
    parser.add_argument("--datasource-id", required=True, help="Data source ID to update")

    parser.add_argument("--custom-sql-file", help="Path to .sql file with new Custom SQL")
    parser.add_argument("--initial-sql-file", help="Path to .sql file with new Initial SQL")
    parser.add_argument("--relation-name", help="Only update relation with this name (for Custom SQL)")
    parser.add_argument("--remove-initial-sql", action="store_true",
                        help="Remove Initial SQL instead of replacing it")

    parser.add_argument("--inspect-only", action="store_true",
                        help="Download and inspect without modifying")
    parser.add_argument("--dry-run", action="store_true",
                        help="Download, modify locally, and inspect — but do NOT publish back")
    parser.add_argument("--output-dir", help="Directory to save the modified .tdsx (optional)")
    parser.add_argument("--local-tdsx", help="Use a local .tdsx file instead of downloading")

    args = parser.parse_args()

    if not args.custom_sql_file and not args.initial_sql_file \
       and not args.inspect_only and not args.remove_initial_sql:
        parser.error("Provide at least one of: --custom-sql-file, --initial-sql-file, "
                      "--remove-initial-sql, or --inspect-only")

    # --- Step 1: Get the .tdsx ---
    tmpdir = tempfile.mkdtemp(prefix="tableau_sql_")

    if args.local_tdsx:
        tdsx_path = args.local_tdsx
        print(f"Using local file: {tdsx_path}")
    else:
        server = connect(args.server, args.site, args.token_name, args.token_value)
        tdsx_path = download_datasource(server, args.datasource_id, tmpdir)

    # --- Step 2: Inspect ---
    if args.inspect_only:
        inspect_datasource(tdsx_path)
        return

    # --- Step 3: Parse and modify ---
    tds_name = find_tds_in_zip(tdsx_path)
    tree, _ = parse_tds(tdsx_path, tds_name)
    root = tree.getroot()

    changes_made = False

    if args.custom_sql_file:
        new_sql = Path(args.custom_sql_file).read_text(encoding="utf-8")
        # Split out initial SQL if file starts with the marker
        custom_sql = new_sql
        initial_sql_from_file = None

        marker = "--## This file contains Initial SQL ##--"
        if marker in new_sql:
            # Everything before the first CTE/SELECT after the marker is initial SQL
            # For now, treat the entire file as custom SQL and let --initial-sql-file handle initial SQL
            pass

        n = update_custom_sql(root, custom_sql.strip(), args.relation_name)
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
    modified_path = os.path.join(output_dir, f"modified_{os.path.basename(tdsx_path)}")
    repackage_tdsx(tdsx_path, tds_name, tree, modified_path)
    print(f"Modified .tdsx saved to: {modified_path}")

    # Show what the updated file looks like
    inspect_datasource(modified_path)

    # --- Step 5: Publish (unless dry-run) ---
    if args.dry_run:
        print("DRY RUN: Skipping publish. Review the modified file above.")
        return

    if args.local_tdsx:
        server = connect(args.server, args.site, args.token_name, args.token_value)

    publish_datasource(server, args.datasource_id, modified_path)

    # Cleanup
    if not args.output_dir:
        shutil.rmtree(tmpdir, ignore_errors=True)

    if not args.local_tdsx:
        server.auth.sign_out()

    print("Done!")


if __name__ == "__main__":
    main()
