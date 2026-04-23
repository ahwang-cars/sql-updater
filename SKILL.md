---
name: tableau-sql-updater
description: Update or validate the Custom SQL / Initial SQL of a Tableau Online data source (or workbook) without opening Tableau Desktop. Trigger when the user asks to update, replace, inspect, validate, or switch-to-table the SQL of a Tableau datasource or workbook.
---

# Tableau SQL Updater

Programmatically edit a Tableau Online data source's Custom SQL or Initial SQL via the REST API — no Tableau Desktop round-trip.

## Setup (one-time)

Requires Python 3.9+.

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Then create `config.json` in the repo root (gitignored) with Tableau PATs for each site you need:

```json
{
  "tableau_server": {
    "server_url": "https://us-west-2b.online.tableau.com",
    "site_id": "dealertools",
    "token_name": "<your_pat_name>",
    "token_secret": "<your_pat_secret>"
  },
  "cars_site": {
    "site_id": "cars",
    "token_name": "<your_pat_name>",
    "token_secret": "<your_pat_secret>"
  },
  "connection_credentials": {
    "username": "<redshift_user>",
    "password": "<redshift_password>"
  }
}
```

For each new terminal session, activate the venv: `source venv/bin/activate`.

## Where to save SQL files

Save ticket-scoped SQL in `sql/`, named after the ticket (e.g. `sql/EASD-2288.sql`). These are committed to git as the audit trail of what was deployed — `--validate-sql` can re-verify prod against the committed file at any time.

## When to use this skill

User says things like:
- "update the custom SQL for datasource X"
- "point datasource X at table `schema.table` instead of custom SQL"
- "validate the SQL on datasource X matches ticket-123.sql"
- "inspect the current SQL on datasource X"
- "update Initial SQL on datasource X"

Works against both sites in `config.json`: `cars` and `dealertools`. Default is `cars`.

## Standard workflow

Follow this sequence for any SQL update:

1. **Confirm target and site.** Ask the user which datasource and which site (`cars` or `dealertools`) if not clear.
2. **Save the new SQL to `sql/sql/<TICKET>.sql`** (e.g. `sql/EASD-2288.sql`).
3. **Dry-run** with `--dry-run` to confirm the script found the relations and the new SQL preview looks right.
4. **Confirm with the user** before publishing.
5. **Publish** (drop `--dry-run`).
6. **Validate** after publish with `--validate-sql` to prove the live datasource now matches the file.

## Commands

Assume `python` resolves to the project's venv (`source venv/bin/activate` if running as a subprocess).

### Inspect current SQL
```bash
python tableau_sql_updater.py --config config.json --site <cars|dealertools> \
  --datasource-name "<Datasource Name>" --inspect-only
```

### Update Custom SQL (dry-run, then publish)
```bash
# Dry run
python tableau_sql_updater.py --config config.json --site <cars|dealertools> \
  --datasource-name "<Datasource Name>" --custom-sql-file sql/<TICKET>.sql --dry-run

# Publish
python tableau_sql_updater.py --config config.json --site <cars|dealertools> \
  --datasource-name "<Datasource Name>" --custom-sql-file sql/<TICKET>.sql
```

### Validate live SQL matches a file
```bash
python tableau_sql_updater.py --config config.json --site <cars|dealertools> \
  --datasource-name "<Datasource Name>" --validate-sql sql/<TICKET>.sql
```
Exits 0 on match, 1 on mismatch (with a unified diff).

### Switch Custom SQL to a direct table/view
```bash
python tableau_sql_updater.py --config config.json --site <cars|dealertools> \
  --datasource-name "<Datasource Name>" --switch-to-table "schema.tablename" --dry-run
```

### Update Initial SQL
```bash
python tableau_sql_updater.py --config config.json --site <cars|dealertools> \
  --datasource-name "<Datasource Name>" --initial-sql-file <file>.sql --dry-run
```

### Workbook targets
Swap `--datasource-name` for `--workbook-name` for workbook targets (same flags apply).

## Flag reference

| Flag | Purpose |
|------|---------|
| `--config` | Path to `config.json` with credentials |
| `--site` | `cars` or `dealertools` (default: `cars`) |
| `--datasource-name` / `--datasource-id` | Target datasource (name looks up ID) |
| `--workbook-name` / `--workbook-id` | Target workbook instead of datasource |
| `--custom-sql-file` | Replace Custom SQL with the contents of this file |
| `--initial-sql-file` | Replace Initial SQL with the contents of this file |
| `--remove-initial-sql` | Remove Initial SQL entirely |
| `--switch-to-table` | Replace Custom SQL with a direct table ref (`schema.table`) |
| `--relation-name` | Only update the relation with this exact name |
| `--validate-sql` | Download and diff Custom SQL against a file (exit 1 on mismatch) |
| `--inspect-only` | Print current SQL; no changes |
| `--dry-run` | Modify locally but do NOT publish |
| `--output-dir` | Save the modified `.tdsx` locally |
| `--local-tdsx` / `--local-twbx` | Use a local file instead of downloading |

## Helper: split_sql.py

If someone hands you one combined `.sql` file with both Initial SQL and Custom SQL, split it first:
```bash
python split_sql.py combined.sql --output-dir ./split_output
```
Marker between sections must be `-- CUSTOM SQL BELOW --`. Output: `initial_sql.sql` and `custom_sql.sql`.

## Gotchas

- **Post-publish auth error is cosmetic.** The script embeds DB credentials into the `.tdsx` XML *before* publish, then tries to re-apply them via the REST API after publish. The API call fails on Bridge-connected datasources with `400033: Authentication update is not allowed`. The publish itself succeeded — ignore the traceback.
- **Two Custom SQL relations are normal.** Most datasources have the same query in two places (physical layer + logical/object-graph layer). Tableau's UI shows it as one. The updater edits both to keep them consistent.
- **`--switch-to-table` changes the physical layer only.** The logical-table caption (e.g. "Custom SQL Query") persists in the UI even after switching — this is cosmetic. Renaming the caption requires Tableau Desktop (it rewrites column bindings safely).
- **Always run `--validate-sql` after publish** when the change is driven by a ticket. It proves the deployed state matches the file you intended to ship.
- **Credentials live in `config.json`** (gitignored). Never commit.

## Troubleshooting

- **"command not found: --flag"** — zsh parsed a multi-line command as separate commands. Run as one line.
- **"No datasource found with name: ..."** — check the exact casing; the name lookup is case-insensitive but the string must otherwise match. If ambiguous across projects, use `--datasource-id`.
- **Extract refresh fails after publish** — check the Bridge connection settings in Tableau Online; embedded credentials may need to be re-saved manually.
