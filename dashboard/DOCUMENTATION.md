# NYC Taxi Lakehouse — Full Project Documentation

This is the complete technical writeup of the project: every phase, every
architectural decision and why it was made, the real bugs hit along the
way and how they were diagnosed, and how the whole thing is deployed.
The README is the short version; this is the long one.

**Live dashboard:** https://nyc-taxi-lakehouse-dashboard.streamlit.app/
**Repo:** https://github.com/AdwaitMinde/nyc-taxi-lakehouse

---

## Table of contents

1. [Project summary](#project-summary)
2. [Why Databricks Free Edition shaped every decision](#why-databricks-free-edition-shaped-every-decision)
3. [Phase 0 — Data acquisition](#phase-0--data-acquisition)
4. [Phase 1 — Bronze ingestion](#phase-1--bronze-ingestion)
5. [Phase 2 — Silver transforms](#phase-2--silver-transforms)
6. [Phase 3 — Gold aggregates](#phase-3--gold-aggregates)
7. [Phase 4 — Job orchestration](#phase-4--job-orchestration)
8. [Phase 5 — Streamlit dashboard](#phase-5--streamlit-dashboard)
9. [Deployment](#deployment)
10. [A real security incident, and how it was handled](#a-real-security-incident-and-how-it-was-handled)
11. [Repo layout](#repo-layout)
12. [What I'd do differently / next steps](#what-id-do-differently--next-steps)

---

## Project summary

A medallion-architecture (bronze → silver → gold) data pipeline built
entirely on Databricks Free Edition, processing NYC Taxi and Limousine
Commission (TLC) trip data — Yellow, Green, FHV, and FHVHV (Uber/Lyft) —
covering the most recent 12 published months at the time of building
(May 2025 through April 2026). The pipeline is orchestrated as a 12-task
DAG in Databricks Jobs and served through a publicly deployed Streamlit
dashboard.

This isn't a notebook that runs top to bottom once. It's structured the
way a real data engineering project is: staged phases, each independently
committed to git once verified working; explicit schemas checked against
real files rather than trusted documentation; data quality enforced with
a quarantine pattern instead of silent drops; and a deployment story that
accounts for real infrastructure behavior (cold starts, credential
hygiene) rather than glossing over it.

## Why Databricks Free Edition shaped every decision

Before writing any code, the project had to answer: what does this
platform actually allow, and what happens when you exceed it? Free
Edition's real constraints:

- **Serverless-only compute.** No clusters to size or forget to shut
  down — Databricks manages this automatically. This removed an entire
  category of beginner mistakes rather than being a limitation to work
  around.
- **A daily/monthly compute quota.** Exceeding it locks the workspace's
  compute for the rest of the day, or in extreme cases the rest of the
  month. This is an availability risk, not a billing risk — there's no
  amount of money that fixes it once you're locked out. It's why the
  pipeline is built as discrete, checkpointed stages (bronze session,
  silver session, gold session) rather than one giant run.
- **Restricted outbound internet from serverless compute**, with no
  published list of which domains are allowlisted. This single fact
  drove the entire Phase 0 architecture: download data locally first,
  upload it second, rather than gambling on whether a notebook could
  reach the TLC's CDN directly.

## Phase 0 — Data acquisition

**What it does:** downloads 47 of 48 possible parquet files (Yellow,
Green, FHVHV all 12 months; FHV 11 of 12 — TLC hadn't published April
2026 for that dataset yet) from the TLC's public CDN to local disk, then
uploads them into a Unity Catalog volume (`nyc_taxi.raw.trip_data`) via
the Databricks CLI.

**Key design decision — download locally, don't pull from inside
Databricks.** Since serverless's outbound network allowlist is
unverifiable from outside the platform, the safer design downloads with
a plain Python script (full unrestricted internet access) and uploads as
a separate step. This also means a Databricks outage or quota lockout
never threatens the raw data — it's sitting on local disk regardless.

**Script design details:**
- Probes the CDN with HEAD requests to find the actual latest published
  month, rather than hardcoding "today minus 2 months" — TLC's
  publication schedule drifts by days or weeks around month boundaries.
- Idempotent: skips files already downloaded, so a partial failure can
  be safely re-run without redoing completed work.
- Downloads to a `.part` file and renames only on success, avoiding a
  half-written file being mistaken for a complete one on a later run.

**Auth:** Databricks CLI configured with OAuth user-to-machine
authentication rather than a long-lived personal access token — the CLI
manages short-lived tokens via a browser login flow, which shrinks the
damage window if anything ever leaked.

**A real gotcha hit here:** after `winget install`, the CLI wasn't found
("not recognized as a cmdlet"). Cause: PowerShell caches `PATH` at
launch, so an already-open terminal doesn't see updates from an installer
that ran in a different process. Fix: close and reopen the terminal.

See [`docs/phase1_lesson_guide.md`](docs/phase1_lesson_guide.md) for the
full conceptual writeup of Unity Catalog's volume/catalog/schema
hierarchy referenced here.

## Phase 1 — Bronze ingestion

**What it does:** reads raw parquet from the Unity Catalog volume,
writes to Delta tables partitioned by `pickup_year`/`pickup_month`, one
table per dataset (`bronze.yellow`, `.green`, `.fhvhv`, `.fhv`).

**Key design decision — explicit schemas for 3 of 4 datasets, inference
for the 4th.** TLC parquet files are not type-consistent across months —
the same logical column (`passenger_count`, `RatecodeID`) can be `INT64`
in one month's file and `DOUBLE` in another's. Spark's `mergeSchema`
option does **not** fix type mismatches, only missing/extra columns, so
trusting inference across 12 months risks a conversion error. Yellow,
Green, and FHVHV use manually defined `StructType` schemas; FHV uses
inference because its column naming has shifted more across TLC's
history and a wrong hardcoded name seemed riskier than the type-mismatch
problem for this particular sparse dataset.

**Real bugs hit and fixed, found only by inspecting actual files —
not the TLC's own data dictionary PDFs:**

| Dataset | Field | Data dictionary said | Actual file has |
|---|---|---|---|
| Yellow | `passenger_count`, `RatecodeID` | double | long |
| Yellow | `airport_fee` | lowercase | `Airport_fee` (capital A) |
| Green | `RatecodeID`, `passenger_count`, `trip_type` | double | long |
| FHVHV | `PULocationID`, `DOLocationID` | long | integer |

**Takeaway documented at the time:** treat a data dictionary as a
starting point, not ground truth. Run
`spark.read.format("parquet").load(path).printSchema()` against a real
file *before* writing the explicit schema, not after the first failure.

**Unity Catalog gotcha:** `input_file_name()` is blocked outright under
UC (`UC_COMMAND_NOT_SUPPORTED`) because it can expose underlying cloud
storage paths that UC is designed to abstract away. Fixed by switching to
`col("_metadata.file_path")`, the UC-native equivalent.

Full conceptual treatment, including why partitioning by month matters
and the HEAD-vs-GET / idempotency reasoning from Phase 0, in
[`docs/phase1_lesson_guide.md`](docs/phase1_lesson_guide.md).

## Phase 2 — Silver transforms

**What it does:** deduplicates bronze data and applies bounds checks per
dataset, splitting each table into a clean `silver.*` table and a
`*_quarantine` table for rows that failed validation.

**Key design decision — quarantine, don't drop.** Every row either lands
in silver or lands in quarantine with the reason visible. Silently
dropping bad rows destroys the ability to later answer "how much data was
bad, and what kind." For a portfolio project, showing the bad data and
the reasoning is more valuable than making it disappear.

**Dedup via window function, not `dropDuplicates()`.**
`dropDuplicates()` picks an arbitrary surviving row with no way to say
which one or why. A `row_number()` window with explicit `orderBy` on
`_ingest_timestamp` makes the tiebreak deterministic and answerable in a
follow-up question.

**Bounds thresholds are sourced, not invented:** pickup/dropoff location
IDs 1–263 (the actual count of NYC TLC taxi zones), passenger count 1–6
(legal max for a yellow cab), fare $2.50–$250, trip duration 1–180
minutes. Each dataset's checks differ based on what fields it actually
has — FHVHV uses `trip_miles`/`base_passenger_fare`/`trip_time` instead
of `passenger_count`/`fare_amount`; FHV, being sparse, only checks
location IDs and duration.

**Data quality tooling — a deliberate departure from the original plan.**
The architecture was originally going to use Great Expectations. Checked
first and found an open, unresolved compatibility issue: GX throws
`PERSIST TABLE is not supported on serverless compute` on exactly the
compute tier this project runs on. Rather than build on a dependency with
a known structural gap, validation is implemented as plain PySpark
boolean checks instead — same effect, no extra dependency risk.

**A bug that silver initially missed, caught two phases later by the
gold layer:** see the [ANSI cast bug](#a-bug-that-spanned-three-phases)
section below — this is the single most instructive debugging story in
the whole project.

Full writeup in
[`docs/phase2_lesson_guide.md`](docs/phase2_lesson_guide.md).

## Phase 3 — Gold aggregates

**What it does:** builds four business-facing tables from silver:

- **`trips_unified`** — narrows all 4 silver tables to common fields
  (pickup/dropoff time, locations, a `trip_type` label) and stacks them
  with `unionByName`, making cross-dataset comparison possible at all.
- **`demand_patterns`** — pickups by zone, hour, day-of-week, built off
  `trips_unified` since it's dataset-agnostic.
- **`revenue_by_zone_hour`** — aggregates **per dataset first**, before
  combining, since fare fields genuinely differ (`fare_amount`/
  `total_amount` for Yellow/Green vs. `base_passenger_fare`/`tips` for
  FHVHV). FHV is excluded entirely — it reports no fare data, so padding
  it with nulls would be worse than leaving it out.
- **`trip_duration_stats`** — avg/median/p90 duration per trip type,
  using `approx_percentile` since exact percentiles over hundreds of
  millions of rows cost more than the marginal accuracy is worth.

**Key design decision — narrow to common fields before `unionByName`,
don't merge full schemas.** `unionByName(allowMissingColumns=True)` would
backfill every column not present in a given table with `NULL`, producing
a 40+ column table that's mostly empty depending on which dataset a row
came from. Explicitly selecting just the fields every dataset shares,
then unioning those narrow frames, is a cleaner and more intentional
result.

### A bug that spanned three phases

This is worth its own callout because it's a genuinely good debugging
story.

While building `gold_trip_duration_stats`, the output showed a row with
**a negative duration (-58 minutes)** in FHVHV, and **a 1,578,280-minute
(~3 year) "trip"** in FHV. Neither should have been possible — silver's
bounds checks were supposed to catch exactly this.

**Root cause, found by tracing backward:** every duration calculation
across the project used `.cast("long")` on a `timestamp_ntz` column to
get epoch seconds for subtraction. Under Spark's ANSI mode (on by
default), casting `TIMESTAMP_NTZ` directly to `BIGINT` isn't a supported
cast — only `TIMESTAMP_LTZ` (timestamp with timezone) supports that
direct numeric cast. The actual error surfaced as:

```
[FAILED_READ_FILE.PARQUET_COLUMN_DATA_TYPE_MISMATCH]
Cannot resolve "CAST(dropOff_datetime AS BIGINT)" due to data type mismatch
```

**The fix:** swap every `F.col(x).cast("long")` for `F.unix_timestamp(x)`
— a dedicated datetime function, not a generic cast, so it isn't subject
to the same ANSI restriction. Same result (epoch seconds), different
code path that Spark actually permits.

**This single root cause had been silently present in five files** —
`silver_yellow.py`, `silver_green.py`, `silver_fhvhv.py`, `silver_fhv.py`,
and `gold_trip_duration_stats.py` — but only `silver_fhvhv.py`'s *new*
duration check (added specifically to catch this) and `silver_fhv.py`'s
*missing* duration check (it never had one) let bad rows slip all the
way to gold before anyone noticed. Yellow and Green had the identical
latent bug but it never surfaced as a visible problem for them, which is
itself a lesson: the absence of a visible error doesn't mean the code
is correct, only that it hasn't been tested by data that exposes the bug
yet.

**After the fix and a full re-run:** every dataset's min/max duration
landed cleanly within the intended 1–180 minute bound. FHV's average
duration dropped from 102 minutes to 34 minutes once the 3-year outlier
was correctly quarantined — a clean illustration of why mean is a poor
outlier detector compared to median/percentiles.

## Phase 4 — Job orchestration

**What it is:** a 12-task DAG in Databricks Jobs (Lakeflow Jobs), not a
notebook — there's no code artifact for this phase, just a job
configuration built in the workspace UI.

**DAG structure:**

```
bronze_yellow  bronze_green  bronze_fhvhv  bronze_fhv
      |             |             |             |
silver_yellow  silver_green  silver_fhvhv  silver_fhv
      \             |             |            /
       \            |             |           /
        gold_trips_unified <-----+------------+
           |                |
   gold_demand_patterns  gold_trip_duration_stats

gold_revenue_by_zone_hour  <-- depends on silver_yellow, silver_green,
                                silver_fhvhv directly (skips fhv and
                                trips_unified)
```

The 4 bronze→silver chains are independent and run in parallel.
`gold_trips_unified` is the one real bottleneck, since it can't start
until all 4 silver tasks finish. `gold_revenue_by_zone_hour` is the one
gold task that *doesn't* wait on `trips_unified` — it reads the 3
relevant silver tables directly, so it can start as soon as
silver_yellow/green/fhvhv finish, without waiting on the slowest part of
that layer (silver_fhv) or the union step.

**Deliberately excluded from the job:** the three `setup_*_schema.sql`
notebooks. They're one-time and idempotent
(`CREATE SCHEMA IF NOT EXISTS`) — running them on every scheduled
execution would be unnecessary overhead for something that almost never
needs to change.

**No scheduled trigger.** TLC data updates monthly at most, and an
unattended schedule on a quota-limited free tier risks burning the daily
allowance on a day nobody planned to use the workspace. Manual "Run now"
fits the actual cadence of new data better than a cron schedule would.

**Verified end to end:** a full run completed in 13m50s, all 12 tasks
succeeded. Worth noting — the first run sat in "Queued" for several
minutes with no error message before any task started executing. This
was cold-start latency for serverless compute provisioning on a brand
new job, not a configuration problem; subsequent runs start faster.

Full task-dependency spec and the UI click-path in
[`docs/phase4_job_orchestration.md`](docs/phase4_job_orchestration.md).

## Phase 5 — Streamlit dashboard

**What it does:** a 4-tab dashboard (Overview, Demand patterns, Revenue,
Trip duration) querying the four gold tables directly via the Databricks
SQL Connector for Python.

**Key constraint:** the SQL connector does not support connecting to
jobs/notebook serverless compute at all — it needs an actual SQL
warehouse. Free Edition auto-creates a small one ("Starter Warehouse")
the first time the SQL editor is opened, so there was nothing to
provision manually.

**Auth — a deliberate, acknowledged tradeoff.** The CLI setup in Phase 0
used OAuth specifically because short-lived tokens are safer than a
standing credential. The dashboard uses a personal access token instead,
because OAuth's interactive browser login doesn't fit a long-running
local (or deployed) script. This is a real step down in security
posture, mitigated by scoping the token narrowly (BI Tools / `sql` scope
only, not full workspace API access) and giving it a short 90-day
lifetime.

**No PyArrow dependency.** The connector's `fetchall_arrow()` path is
faster for large result sets, but the gold tables are small,
pre-aggregated rows — plain `cursor.fetchall()` into a pandas DataFrame
is simpler and sufficient, and it avoids an extra dependency for no real
benefit at this data size.

**Caching:** `st.cache_resource` for the connection object (don't reopen
a connection per query), `st.cache_data(ttl=600)` for query results (10
minutes), to avoid re-querying Databricks on every tab switch or page
rerun given the daily compute quota.

## Deployment

Deployed publicly on Streamlit Community Cloud at
**nyc-taxi-lakehouse-dashboard.streamlit.app**, deploying directly from
the `dashboard/` subfolder of this repo (`dashboard/app.py` as the main
file path).

**Secrets handling:** Community Cloud exposes whatever's pasted into its
"Secrets" field as environment variables automatically. Since the app
already reads `os.environ[...]`, zero code changes were needed between
local and deployed — the same `app.py` runs both places.

**Two independent cold-start layers, both real, both explained in-app:**

1. **The Databricks SQL warehouse** auto-stops after ~10 minutes idle.
   First query after that triggers a restart that can take a few
   minutes. The app now shows an explicit spinner message
   ("Connecting... this can take a few minutes if nobody's viewed this
   recently") rather than leaving a visitor staring at a blank screen.
2. **Streamlit Community Cloud itself** puts apps with no traffic for 12
   hours to sleep, showing a generic platform-level "wake this app up"
   screen before the dashboard's own code even runs. This is a platform
   default, not something configurable per app.

Worst case, both stack — maybe 3-5 minutes total for the first visitor
after a quiet period. The practical mitigation isn't more engineering,
it's visiting the link yourself a few minutes before sharing it (e.g.
before sending it in a job application), which warms both layers ahead
of time.

Full deploy steps in
[`dashboard/DEPLOYMENT.md`](dashboard/DEPLOYMENT.md).

## A real security incident, and how it was handled

Worth documenting honestly rather than omitting, because the response is
arguably more instructive than the mistake.

While editing `dashboard/app.py` locally, a real Databricks personal
access token ended up hardcoded into the file (likely pasted in during
local testing instead of being set as an environment variable). On
`git push`, **GitHub's secret scanning push protection caught it and
blocked the push outright**, before it ever reached the remote
repository.

**Response, in order:**
1. The token was revoked immediately in the Databricks workspace —
   treated as compromised regardless of whether the push had actually
   succeeded, since it had already touched disk in a hardcoded form.
2. A fresh token was generated with the same narrow scope.
3. Git history was inspected directly (`git show <commit>:<path>`) to
   confirm exactly which commit held the literal secret and exactly
   which line, rather than guessing.
4. Confirmed the *current* HEAD and the live GitHub copy of the file
   were both clean before considering the incident closed — not just
   trusting that the file looked right locally.
5. The new token was verified working end-to-end (local dashboard run,
   then the live deployed dashboard) before moving on.

**Why this is worth including in the docs rather than scrubbing it from
the story:** push protection is a real safety net, and using it
correctly — verifying the actual git history and not just the working
file, revoking on suspicion rather than waiting for proof of misuse — is
exactly the kind of judgment call that matters more in practice than
never making the mistake in the first place.

## Repo layout

```
scripts/      local download + upload scripts (Phase 0)
notebooks/    bronze, silver, gold notebooks + schema setup SQL
dashboard/    Streamlit app, its own requirements.txt, README, deploy guide
docs/         phase-by-phase lesson guides + the job orchestration spec
data/         local raw parquet landing zone (gitignored, not in repo)
```

## What I'd do differently / next steps

- **Inspect real file schemas before writing explicit `StructType`
  definitions**, rather than starting from the TLC's data dictionary
  PDFs and patching after a failure. This would have skipped three
  separate rounds of trial-and-error in Phase 1 alone.
- **Add a proper data quality framework once one exists that's verified
  compatible with serverless compute** — the plain PySpark checks work,
  but a framework would give richer reporting (e.g. percentage-based
  thresholds, automatic alerting) than hand-rolled boolean columns.
- **A scheduled trigger with quota-aware logic** — right now
  orchestration is manual specifically to avoid burning the daily quota
  unexpectedly, but a smarter setup could check remaining quota before
  triggering a scheduled run.
- **Join `PULocationID`/`DOLocationID` against the TLC's taxi zone
  lookup table** in the dashboard, so zone IDs show as actual
  neighborhood names instead of bare integers.
