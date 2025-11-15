# Sakila Analytics Sync (MySQL → SQLite)

This project implements an ORM-based data sync pipeline from the MySQL Sakila sample database into a star-schema analytics database in SQLite.  
It was built as a course assignment to demonstrate:

- ORM models for both source (Sakila) and target (analytics)
- Initial full load of dimensions, bridges, and facts
- Incremental updates using last-update timestamps and a `sync_state` watermark table
- Data validation between MySQL and SQLite
- A small CLI interface for running the sync process end to end

---
## Project Structure

Core files:

- `models_sakila.py` – SQLAlchemy ORM models for the source MySQL Sakila tables.
- `models_analytics.py` – SQLAlchemy ORM models for the target SQLite analytics schema.
- `database.py` – Engine and session setup for MySQL (source) and SQLite (target).
- `cli.py` – Main CLI entrypoint exposing the four commands:
  - `init`
  - `full-load`
  - `incremental`
  - `validate`
- `testresult.docx` – five test results
---

## Prerequisites

- Python 3.11 
- MySQL server with the Sakila sample database loaded
  - Database name: `sakila`
  - Standard Sakila schema and data
- SQLite (built into Python)
- Python packages (install via `pip`):
  - `sqlalchemy`
  - `pymysql`
  - `click`

## CLI Usage
# Init — Set up the analytics database
Initializes the SQLite analytics database, creates all ORM tables, and prepares support structures like dim_date and sync_state.

Command:
python cli.py init

What it does:
Creates all analytics tables in analytics.db
Pre-populates dim_date with a calendar range (e.g., 2005–2006)
Initializes sync_state with a default watermark (2000-01-01) for each tracked source table
Creates useful indexes for query performance
Run this once before any load, or as part of a rebuild.

# Full-load — Load all source data
Performs a full import from MySQL Sakila into SQLite, using ORM-based transformations.
Command:
"Run once after init, or to rebuild from scratch with a clean database"
python cli.py full-load --force

What it does:
Optionally drops and re-creates analytics tables when --force is provided
Re-runs the init logic (dimension dates and sync_state)
Loads all dimensions:
Customers, stores, films, actors, categories
Loads bridge tables:
Film–actor (bridge_film_actor)
Film–category (bridge_film_category)
Loads fact tables:
Rentals (fact_rental)
Payments (fact_payment)
Updates sync_state watermarks to the current time for all tables
This is usually run once at the beginning or when rebuilding everything.

# Incremental — Load only new or changed data
Synchronizes only new or updated rows since the last successful sync.

Command:
python cli.py incremental

What it does:
Reads the last sync timestamp for each table from sync_state
For each source table:
Dimensions (e.g., customer, film, actor, category, store):
Queries rows with last_update > last_sync_timestamp
Updates existing rows in the corresponding dim table (Type 1)
Inserts rows that do not yet exist
Bridge tables (film_actor, film_category):
Same incremental logic based on last_update
Fact tables:
Rentals: uses rental_date as the watermark for new events
Payments: uses payment_date as the watermark for new events
Adds only new facts (insert-only)
After successful completion, updates all relevant sync_state.last_sync_timestamp to the current time
This command is meant to be re-run on a schedule to keep analytics.db up to date.

# Validate — Verify data consistency

Compares counts and aggregated totals between MySQL and SQLite over a selected period.

Command:
Default: last 30 days
python cli.py validate

What it does:
Validates dimension counts (total):
Customer Count: Sakila vs Analytics
Film Count: Sakila vs Analytics
Validates fact counts for the last N days:
Rental count: MySQL rental vs SQLite fact_rental
Validates fact totals for the last N days:
Total payment amount: MySQL payment vs SQLite fact_payment

Validates per-store aggregates:
Rentals per store
Payment totals per store
Uses a small numeric tolerance (e.g., difference < 0.01) for floating sums
Prints Validation SUCCESS or Validation FAILED messages; if a critical mismatch is found, it stops early with an error message
Use this after full-load or incremental to confirm that the sync is correct.