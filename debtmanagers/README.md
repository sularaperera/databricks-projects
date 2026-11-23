# Debt collection pipeline on Databricks (medallion, star schema, SCD2, idempotent)

This is an end-to-end Databricks project built on the same made-up debt collection data as the practice pack.
Client placement files, a bank payment file and dialler contact logs land daily. They go through **bronze → silver → gold**
into a star schema with KPI views, with data quality checks and a test that proves reruns are safe.

```
data/landing/ (CSV, daily)         BRONZE (raw text)          SILVER (clean, keyed)                 GOLD (reporting)
──────────────────────────         ─────────────────          ─────────────────────                 ────────────────
clients_<date>.csv          ──▶    debt_bronze.clients   ──▶  debt_silver.clients     (SCD1)  ──┐   dim_client
placements_<date>.csv       ──▶    debt_bronze.placements ─▶  debt_silver.accounts    (SCD1)  ──┤   dim_account
                                                         └─▶  debt_silver.debtors     (SCD2)  ──┤   dim_debtor (masked, SCD2)
bank_payments_<date>.csv    ──▶    debt_bronze.bank_payments▶ debt_silver.payments    (dedupe)──┤   dim_date
bank_control_<date>.csv     ──▶    debt_bronze.bank_control   (used for reconciliation)          ├─▶ fact_payments
contact_attempts_<date>.csv ──▶    debt_bronze.contact_attempts▶debt_silver.contact_attempts ────┤   fact_contact_attempts
                                                              *_quarantine (bad rows + reason)  └─▶ fact_account_snapshot (daily)
debt_meta: load_log, pipeline_runs, dq_results                                                       vw_* KPI views
```

## Notebooks (run in this order, or use `11_run_pipeline`)
| # | Notebook | What it does |
|---|---|---|
| 00 | `00_config` | Settings, metadata for each source, cleaning and MERGE helpers. Every notebook runs it first with `%run`. |
| 01 | `01_setup` | Creates the schemas, the landing volume and the meta tables, and copies the sample CSVs into the volume |
| 02 | `02_bronze_ingest` | Loads each file for `run_date` as raw text, skips files already loaded (checksum) |
| 03 | `03_silver_clients` | Client reference data, SCD1 MERGE |
| 04 | `04_silver_placements` | Clean, validate, quarantine, then accounts (SCD1) and debtors (**SCD2**) |
| 05 | `05_silver_payments` | Dedupe bank transactions, match them to accounts, quarantine unknown refs |
| 06 | `06_silver_contacts` | Calls, SMS, emails and letters |
| 07 | `07_gold_dimensions` | `dim_date`, `dim_client`, `dim_debtor` (masked PII), `dim_account` |
| 08 | `08_gold_facts` | `fact_payments`, `fact_contact_attempts`, `fact_account_snapshot` with point-in-time debtor keys |
| 09 | `09_gold_kpi_views` | Collections per month, collection rate, ageing, 90+ call queue, promise-to-pay kept rate |
| 10 | `10_data_quality` | Freshness, bank reconciliation, quarantine rate, unique keys, orphans. A FAIL fails the job. |
| 11 | `11_run_pipeline` | Runs 02–10 for each date from `start_date` to `end_date` (demo or backfill) |
| 12 | `12_idempotency_test` | Reruns, force-reloads and replays the days, then checks every table is unchanged |

## What each day of dummy data tests
| Day | What's in the files | What you should see |
|---|---|---|
| **13 Nov** | History: 4 clients, 88 placements with messy formats, 215 bank rows (one transaction sent twice), 292 contact attempts | 88 accounts, **60 debtors** (6 duplicate people merged), 214 payments (duplicate removed), reconciliation PASS |
| **14 Nov** | 12 new Kiwi Power placements (the Task 2 messy file), 4 payments, 4 contacts | **6 rows quarantined** with reasons, 5 accounts added, quarantine rate WARN (50%) |
| **15 Nov** | Liam's phone changed, Sione's balance changed, Noah's fixed row re-sent, Mia is new. Payments include a re-sent one, a **late October payment** and one for an unknown account. | SCD2 closes Liam's old version and adds a new one. 2 accounts updated, 2 inserted. The re-sent payment is ignored, the late payment lands in October, and the $40 goes to quarantine (WARN). |

## How to run it in Databricks (Free Edition works)
**Option A: Git folder (best, everything comes across in one go)**
1. Put this folder in a GitHub repo.
2. In Databricks: **Workspace → Create → Git folder** and paste the repo URL.
3. Open `notebooks/01_setup`, attach to **Serverless**, then **Run all**. It finds `data/landing` and copies it into the volume.
4. Open `notebooks/11_run_pipeline` → **Run all** (loads 13 to 15 Nov).
5. Open `notebooks/12_idempotency_test` → **Run all**. All three tests should say PASS.

**Option B: import the zip**
1. **Workspace → your folder → ⋮ → Import →** choose `databricks_debt_project.zip`.
2. Run `01_setup`. If it says it can't find the sample data, go to **Catalog → workspace → debt_meta → Volumes → landing → Upload to this volume** and upload each folder's CSVs into a matching sub-folder (`placements/`, `bank_payments/`, `bank_control/`, `contact_attempts/`, `clients/`).
3. Carry on with steps 4 and 5 above.

The catalog defaults to `workspace`. To use another one, change the `catalog` widget at the top of any notebook.

**As a scheduled job:** `databricks.yml` and `resources/debt_pipeline_job.yml` define a Databricks Asset Bundle. Each notebook
is its own task with dependencies (payments and contacts run in parallel), retries on bronze, `max_concurrent_runs: 1`,
and a 7am NZ schedule (paused). Run `databricks bundle deploy`, then start the job with `run_date` = 2026-11-13, 14, 15.

## Tested
Every notebook was run end to end on open-source Spark 4.0.1 + Delta Lake 4.0.0 with `tools/run_local.py`. That script
runs the notebooks exactly as written, with small stand-ins for `dbutils`. Results are in `tools/local_test_output.txt`:
all data quality checks PASS (with the expected WARNs), and **all three idempotency tests PASS (19/19 tables identical)**.
It has not been run on a real Databricks workspace. The only Databricks-specific parts are the volume path, the
three-part table names and `dbutils`.

## Interview talking points this project gives you
- **Idempotency in three layers:** a file checksum in `load_log`, dedupe inside the batch, then MERGE on the business key with a row hash. All three are proven by `12_idempotency_test`.
- **Backfill guard:** rerunning an old date never overwrites newer data (`only_if_newer`, and the `valid_from` check in SCD2).
- **Money must reconcile:** the bank control total is checked to the cent, and unmatched payments are quarantined, not dropped.
- **Point-in-time correctness:** facts use the debtor version valid on the event date, and snapshots count a payment only once it had arrived (`loaded_date`).
- **Privacy Act:** phone and email are masked in gold, and only silver (restricted) holds the raw values.
- **Known limitation, and what you'd do next:** `silver.accounts` is SCD1, so a restated old snapshot uses the latest balance. Next steps would be SCD2 for accounts, Auto Loader for many files, DLT/Lakeflow expectations for the rules, Unity Catalog row filters per client, and alerts on `dq_results`.

All data is fake.
