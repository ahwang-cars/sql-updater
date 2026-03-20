# Tableau SQL Updater

## Purpose
Programmatically update the **Custom SQL** and/or **Initial SQL** of a Tableau Online data source without opening Tableau Desktop (which can take 30+ minutes for large data sources).

## How It Works
Tableau `.tdsx` files are ZIP archives containing a `.tds` XML file. The XML stores:
- **Custom SQL** in `<relation type="text">` elements
- **Initial SQL** in the `one-time-sql` attribute of `<connection>` elements

This script uses the Tableau REST API (via `tableauserverclient`) to download the data source, modify the XML, and republish.

## Prerequisites
```bash
pip install tableauserverclient
```

You also need a **Personal Access Token (PAT)** from Tableau Online:
1. Go to Tableau Online → My Account Settings → Personal Access Tokens
2. Create a token and save both the **name** and **secret value**

## Setup (one-time per teammate)

Create a `config.json` file in the same directory as the script:
```json
{
  "cars_site": {
    "site_id": "cars",
    "token_name": "your_token_name",
    "token_secret": "your_token_secret"
  }
}
```

> `config.json` is gitignored — never commit your credentials.

## Default Configuration
- **Server:** `https://us-west-2b.online.tableau.com`
- **Site:** `cars`

---

## Usage

### Inspect a data source (no changes)
```bash
python tableau_sql_updater.py \
  --config config.json \
  --datasource-name "My Datasource Name" \
  --inspect-only
```

### Update Custom SQL (dry run first — always recommended)
```bash
python tableau_sql_updater.py \
  --config config.json \
  --datasource-name "My Datasource Name" \
  --custom-sql-file updated_query.sql \
  --dry-run
```

### Update Initial SQL
```bash
python tableau_sql_updater.py \
  --config config.json \
  --datasource-name "My Datasource Name" \
  --initial-sql-file initial_setup.sql \
  --dry-run
```

### Update both Custom SQL and Initial SQL
```bash
python tableau_sql_updater.py \
  --config config.json \
  --datasource-name "My Datasource Name" \
  --custom-sql-file updated_query.sql \
  --initial-sql-file initial_setup.sql \
  --dry-run
```

### Publish for real (remove --dry-run)
```bash
python tableau_sql_updater.py \
  --config config.json \
  --datasource-name "My Datasource Name" \
  --custom-sql-file updated_query.sql
```

### Use explicit credentials instead of config file
```bash
python tableau_sql_updater.py \
  --token-name "your_token_name" \
  --token-value "your_token_secret" \
  --datasource-name "My Datasource Name" \
  --custom-sql-file updated_query.sql
```

### Use datasource ID instead of name (faster, skips name lookup)
```bash
python tableau_sql_updater.py \
  --config config.json \
  --datasource-id "76595187-2cbc-4f88-ba72-ba162f734bf5" \
  --custom-sql-file updated_query.sql
```

---

## Flags Reference

| Flag | Description |
|------|-------------|
| `--config` | Path to `config.json` with credentials |
| `--server` | Tableau Server URL (default: `https://us-west-2b.online.tableau.com`) |
| `--site` | Site content URL (default: `cars`) |
| `--token-name` | PAT name (if not using --config) |
| `--token-value` | PAT secret (if not using --config) |
| `--datasource-name` | Datasource name — looks up ID automatically |
| `--datasource-id` | Datasource UUID — faster, skips name lookup |
| `--custom-sql-file` | Path to `.sql` file with replacement Custom SQL |
| `--initial-sql-file` | Path to `.sql` file with replacement Initial SQL |
| `--relation-name` | Only update the Custom SQL relation with this specific name |
| `--remove-initial-sql` | Remove Initial SQL entirely |
| `--inspect-only` | Download and show current SQL without modifying |
| `--dry-run` | Modify locally but do NOT publish back |
| `--output-dir` | Save modified `.tdsx` to this directory |
| `--local-tdsx` | Use a local `.tdsx` file instead of downloading |

---

## Important Notes

- **Always use `--dry-run` first** to verify changes before publishing
- **`--inspect-only`** shows what SQL is currently live on the server
- If a datasource name matches multiple datasources in different projects, use `--datasource-id` instead
- Publishing overwrites the datasource in-place, preserving permissions and connected workbooks
- The script downloads the full extract (`include_extract=True`) to avoid publish errors on extract-based datasources
- After publishing, Tableau updates the **Publish Date** immediately; **Last Updated** may lag in the UI

## Known Datasource IDs

| Datasource | UUID |
|------------|------|
| DI 13mo Daily DigAd Summary Dealer Performance | `76595187-2cbc-4f88-ba72-ba162f734bf5` |
