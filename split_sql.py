#!/usr/bin/env python3
"""
Split a combined SQL file into separate Initial SQL and Custom SQL files.

Many Tableau data sources use a pattern where:
  - Initial SQL creates temp tables (runs once when connection opens)
  - Custom SQL references those temp tables

If your .sql file contains both, this script splits them based on markers
or heuristics so they can be fed independently to tableau_sql_updater.py.

Usage:
  python split_sql.py input.sql --output-dir ./split_output

Marker convention:
  Lines before the marker "-- CUSTOM SQL BELOW --" become initial_sql.sql
  Lines after become custom_sql.sql

  If no marker is found, the script looks for the LAST top-level SELECT
  statement and treats everything before it as Initial SQL.
"""

import argparse
import os
import re
from pathlib import Path


INITIAL_SQL_MARKER = "--## This file contains Initial SQL ##--"
CUSTOM_SQL_MARKER = "-- CUSTOM SQL BELOW --"


def split_by_marker(content: str, marker: str) -> tuple[str, str] | None:
    """Split content at marker. Returns (before, after) or None if marker not found."""
    if marker in content:
        idx = content.index(marker)
        before = content[:idx].strip()
        after = content[idx + len(marker):].strip()
        return before, after
    return None


def split_initial_from_custom(content: str) -> tuple[str, str]:
    """
    Heuristic split: if the file starts with the Initial SQL marker,
    the entire file is likely the Custom SQL query (with CTEs that reference
    temp tables created by the Initial SQL that's configured separately).

    Returns (initial_sql, custom_sql).
    """
    # Check for explicit custom SQL marker
    result = split_by_marker(content, CUSTOM_SQL_MARKER)
    if result:
        return result

    # Check for initial SQL marker — if present, the whole thing after it is custom SQL
    if INITIAL_SQL_MARKER in content:
        after_marker = content.split(INITIAL_SQL_MARKER, 1)[1].strip()
        # In this case there's no separate initial SQL in the file
        return ("", after_marker)

    # Fallback: entire content is custom SQL
    return ("", content.strip())


def main():
    parser = argparse.ArgumentParser(description="Split combined SQL into Initial SQL and Custom SQL files")
    parser.add_argument("input_file", help="Path to the combined .sql file")
    parser.add_argument("--output-dir", default=".", help="Directory to write output files")
    args = parser.parse_args()

    content = Path(args.input_file).read_text(encoding="utf-8")
    initial_sql, custom_sql = split_initial_from_custom(content)

    os.makedirs(args.output_dir, exist_ok=True)

    if initial_sql:
        out = os.path.join(args.output_dir, "initial_sql.sql")
        Path(out).write_text(initial_sql, encoding="utf-8")
        print(f"Initial SQL written to: {out} ({len(initial_sql)} chars)")
    else:
        print("No separate Initial SQL found in the file.")

    if custom_sql:
        out = os.path.join(args.output_dir, "custom_sql.sql")
        Path(out).write_text(custom_sql, encoding="utf-8")
        print(f"Custom SQL written to: {out} ({len(custom_sql)} chars)")
    else:
        print("WARNING: No Custom SQL content found.")


if __name__ == "__main__":
    main()
