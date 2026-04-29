#!/usr/bin/env python3
"""
Interactive setup: prompts for Tableau PATs and Redshift creds, writes config.json.

Run once after cloning:
  python setup_config.py
"""

import getpass
import json
import sys
from pathlib import Path

CONFIG_PATH = Path(__file__).parent / "config.json"
DEFAULT_SERVER_URL = "https://us-west-2b.online.tableau.com"


def prompt(label, *, default=None, secret=False, allow_empty=False):
    suffix = f" [{default}]" if default else ""
    while True:
        try:
            raw = getpass.getpass(f"{label}{suffix}: ") if secret else input(f"{label}{suffix}: ")
        except (EOFError, KeyboardInterrupt):
            print("\nAborted.")
            sys.exit(1)
        value = raw.strip()
        if not value and default is not None:
            return default
        if value or allow_empty:
            return value
        print("  (required — please enter a value)")


def main():
    if CONFIG_PATH.exists():
        ans = input(f"{CONFIG_PATH.name} already exists. Overwrite? [y/N]: ").strip().lower()
        if ans != "y":
            print("Aborted.")
            sys.exit(0)

    print("\nWriting config.json (gitignored) for the Tableau SQL Updater.\n")

    print("--- cars site (required) ---")
    cars_token_name = prompt("PAT name")
    cars_token_secret = prompt("PAT secret", secret=True)

    print("\n--- dealertools site (optional — press enter to skip) ---")
    dt_token_name = prompt("PAT name", allow_empty=True)
    dt_token_secret = prompt("PAT secret", secret=True) if dt_token_name else ""

    print("\n--- Redshift connection credentials (required for publishes) ---")
    db_username = prompt("DB username")
    db_password = prompt("DB password", secret=True)

    print("\n--- Tableau server URL ---")
    server_url = prompt("Server URL", default=DEFAULT_SERVER_URL)

    config = {
        "cars_site": {
            "site_id": "cars",
            "server_url": server_url,
            "token_name": cars_token_name,
            "token_secret": cars_token_secret,
        },
        "connection_credentials": {
            "username": db_username,
            "password": db_password,
        },
    }

    if dt_token_name:
        config["dealertools_site"] = {
            "site_id": "dealertools",
            "server_url": server_url,
            "token_name": dt_token_name,
            "token_secret": dt_token_secret,
        }

    CONFIG_PATH.write_text(json.dumps(config, indent=2) + "\n")
    CONFIG_PATH.chmod(0o600)
    print(f"\nWrote {CONFIG_PATH} (mode 600)")
    print("Now run: python tableau_sql_updater.py --config config.json --site cars --inspect-only ...")


if __name__ == "__main__":
    main()
