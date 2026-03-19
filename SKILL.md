# Tableau SQL Updater Skill

## Purpose
Programmatically update the **Custom SQL** and/or **Initial SQL** of a Tableau Online data source without opening Tableau Desktop (which can take 30+ minutes for large data sources).

## How It Works
Tableau `.tdsx` files are ZIP archives containing a `.tds` XML file. The XML stores:
- **Custom SQL** in `<relation type="text">` elements (the query text is the element body)
- **Initial SQL** in the `one-time-sql` attribute of `<connection>` elements

This script uses the Tableau REST API (via `tableauserverclient`) to download the data source, modify the XML, and republish.

## Prerequisites
```bash
pip install tableauserverclient
```

You also need a **Personal Access Token (PAT)** from Tableau Online:
1. Go to Tableau Online > My Account Settings > Personal Access Tokens
2. Create a token and save both the **name** and **secret value**

## Default Configuration
- **Server:** `https://us-west-2b.online.tableau.com`
- **Site:** `cars`

## Usage

### Inspect a data source (no changes)
```bash
python tableau_sql_updater.py \
  --token-name "MY_TOKEN" \
  --token-value "MY_SECRET" \
  --datasource-id 101217312 \
  --inspect-only
```

### Update Custom SQL
```bash
python tableau_sql_updater.py \
  --token-name "MY_TOKEN" \
  --token-value "MY_SECRET" \
  --datasource-id 101217312 \
  --custom-sql-file updated_query.sql \
  --dry-run
```

### Update Initial SQL
```bash
python tableau_sql_updater.py \
  --token-name "MY_TOKEN" \
  --token-value "MY_SECRET" \
  --datasource-id 101217312 \
  --initial-sql-file initial_setup.sql \
  --dry-run
```

### Update both Custom SQL and Initial SQL
```bash
python tableau_sql_updater.py \
  --token-name "MY_TOKEN" \
  --token-value "MY_SECRET" \
  --datasource-id 101217312 \
  --custom-sql-file updated_query.sql \
  --initial-sql-file initial_setup.sql \
  --dry-run
```

### Publish for real (remove --dry-run)
```bash
python tableau_sql_updater.py \
  --token-name "MY_TOKEN" \
  --token-value "MY_SECRET" \
  --datasource-id 101217312 \
  --custom-sql-file updated_query.sql \
  --initial-sql-file initial_setup.sql
```

### Work with a local .tdsx file (no download)
```bash
python tableau_sql_updater.py \
  --token-name "MY_TOKEN" \
  --token-value "MY_SECRET" \
  --datasource-id 101217312 \
  --local-tdsx my_datasource.tdsx \
  --custom-sql-file updated_query.sql \
  --output-dir ./output
```

## Flags Reference

| Flag | Description |
|------|-------------|
| `--server` | Tableau Server URL (default: `https://us-west-2b.online.tableau.com`) |
| `--site` | Site content URL (default: `cars`) |
| `--token-name` | PAT name (required) |
| `--token-value` | PAT secret (required) |
| `--datasource-id` | Data source ID to update (required) |
| `--custom-sql-file` | Path to `.sql` file with replacement Custom SQL |
| `--initial-sql-file` | Path to `.sql` file with replacement Initial SQL |
| `--relation-name` | Only update the Custom SQL relation with this specific name |
| `--remove-initial-sql` | Remove Initial SQL entirely |
| `--inspect-only` | Download and show current SQL without modifying |
| `--dry-run` | Modify locally but do NOT publish back |
| `--output-dir` | Save modified `.tdsx` to this directory |
| `--local-tdsx` | Use a local `.tdsx` file instead of downloading |

## SQL File Format

### Custom SQL file
Should contain just the query — no markers or wrappers needed:
```sql
WITH my_cte AS (
    SELECT ...
)
SELECT * FROM my_cte
```

### Initial SQL file
Should contain the setup statements that run once when the connection opens:
```sql
CREATE TEMPORARY TABLE tmp_table AS (
    SELECT ...
);
```

## Important Notes

- **Always use `--dry-run` first** to verify the modifications before publishing
- **`--inspect-only`** is great for checking what SQL is currently in a data source
- The script downloads **without extracts** (`include_extract=False`) to keep file sizes small
- Publishing with Overwrite mode replaces the data source in-place, preserving permissions and connected workbooks
- If the data source has multiple Custom SQL relations, use `--relation-name` to target a specific one

## Example: Your DigAd Summary Data Source

```bash
# First, inspect what's currently in there
python tableau_sql_updater.py \
  --token-name "MY_TOKEN" \
  --token-value "MY_SECRET" \
  --datasource-id 101217312 \
  --inspect-only

# Dry-run with your optimized SQL
python tableau_sql_updater.py \
  --token-name "MY_TOKEN" \
  --token-value "MY_SECRET" \
  --datasource-id 101217312 \
  --custom-sql-file digad_summary_optimized.sql \
  --dry-run

# If it looks good, publish for real
python tableau_sql_updater.py \
  --token-name "MY_TOKEN" \
  --token-value "MY_SECRET" \
  --datasource-id 101217312 \
  --custom-sql-file digad_summary_optimized.sql
```
