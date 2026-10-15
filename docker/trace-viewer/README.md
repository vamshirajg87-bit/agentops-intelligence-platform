# Trace viewer — local Docker demonstration

The trace viewer serves the trace list, the **Anomalies** tab and the **RCA panel**
(`trace-viewer/`) on `http://127.0.0.1:8000/`. It is a local demonstration, not a
deployment: the viewer has no authentication and is reachable from this machine only.

Status:

- The image was built from `docker/trace-viewer/Dockerfile` (B3J-140).
- The viewer was run from that image against the existing legacy database in isolated
  temporary containers; the API smoke test and an interactive browser check both passed
  (B3J-141, B3J-142). See [Verified legacy-database demo](#verified-legacy-database-demo).
- The Compose overlay `docker/compose.trace-viewer.yml` itself has only been validated with
  `docker compose config`. It has not been started, and it cannot be used unchanged with
  a database that has no bootstrap ledger (see below).

## Files

| File | Purpose |
|---|---|
| `docker/trace-viewer/Dockerfile` | The image: application modules, static files, pinned packages, unprivileged user, no credentials. |
| `docker/compose.trace-viewer.yml` | Overlay with one service, `trace-viewer`, published on `127.0.0.1:8000` only. |
| `storage-consumer/migrations/015_grant_trace_viewer_anomaly_rca_read.sql` | Gives `trace_viewer_reader` SELECT on `anomaly_events`, `rca_investigations` and `rca_evidence`. |

## Prerequisites

1. Docker with Compose v2, run from the repository root.
2. A `.env` (copy `.env.example`, never commit it) with a value for at least
   `POSTGRES_PASSWORD`, `GF_DATASOURCE_AGENTOPS_PASSWORD` and
   **`TRACE_VIEWER_DB_PASSWORD`**. The bootstrap creates `trace_viewer_reader` with that
   password, so set it **before** the database is first bootstrapped. Also set
   `ANOMALY_DETECTOR_DB_PASSWORD` and `RCA_ANALYZER_DB_PASSWORD` if you will run the
   detector and the analyzer (step 4).
3. **Migration 015 applied.** How depends on how the database was built; see below.

### Migration 015 and the bootstrap

`db-bootstrap` is the supported way to build a database. B3J-130C registered migration 015
in `db-bootstrap/bootstrap.py` as a **post-marker upgrade** (`_UPGRADES`): the plan is the
17 base steps (001, dbt, 002 to 014, the post-dbt grants and the `complete` marker) followed
by 015 as step 18. The base order and checksums are unchanged, and the bootstrap still
refuses any migration file it does not know.

| Database | What `db-bootstrap` does |
|---|---|
| **Fresh** (empty) | Runs the base plan, records `complete`, then applies 015 automatically. |
| **Already bootstrapped** (valid 17-step ledger, built before 015 existed) | Classifies it as partly complete and **resumes at 015 only**: nothing earlier is re-run. A second run changes nothing. |
| **Legacy** (platform objects but **no ledger**, for example built by hand) | **Incompatible**: it never adopts such a database, exits with code 5 and changes nothing. |

The `complete` marker means "the base bootstrap is complete". The database counts as
complete (and `analytics-refresh` accepts it) only once 015 is also recorded. The
bootstrap image copies `bootstrap.py` and the migrations at build time, so `db-bootstrap`
must be rebuilt to pick up 015. These bootstrap paths are covered by unit tests; they have
not been run against a live database.

For a legacy database, 015 cannot go through the bootstrap. The demo below applied its four
grants by hand, under explicit approval, and recorded that outside the database.

## Run

```
docker compose -f docker-compose.yml -f docker/compose.pipeline.yml \
               -f docker/compose.trace-viewer.yml up -d --build
```

Open `http://127.0.0.1:8000/`. The Traces tab works once telemetry has been stored.
`docker compose ... config` shows the resolved configuration without starting anything.

**This overlay depends on `db-bootstrap`** (`depends_on: db-bootstrap:
service_completed_successfully`). On a legacy database with no ledger, `db-bootstrap`
exits with code 5, so the viewer never starts. The overlay **cannot be used unchanged with
a ledger-less legacy database**. It is meant for a fresh or bootstrap-managed database.

## Verified legacy-database demo

The viewer was verified against the existing, ledger-less legacy database as follows. This
is a record of a **local demonstration**, not a deployment procedure, a production
configuration or an authentication model.

| Step | Result |
|---|---|
| B3J-134 | A custom-format dump and a globals-only dump (no role password hashes) were taken from a temporary container on the existing data volume, verified with `pg_restore --list` and SHA256, and stored outside the repository and OneDrive. |
| B3J-136 | The four grants of migration 015 were applied **by hand** in one transaction, with a check that rolls back on failure. No bootstrap ledger was created. An evidence note was written beside the backup. |
| B3J-139 | The two `SELECT` grants on `analytics.int_trace_spans` and `analytics.stg_telemetry_spans` were restored for `trace_viewer_reader`. The dbt rebuild of the views most likely removed them (as `post_dbt_grants.sql` describes); a later dbt run removes them again. |
| B3J-141 | The API smoke test passed: health, traces, anomalies, RCA, 404 and 422 handling, and the static dashboard files. |
| B3J-142 | Interactive browser verification passed: traces, anomaly details, RCA confidence, limitations and ranked evidence, related-trace navigation, execution tree and span details. |
| B3J-143 | The temporary containers and network were removed. The original container, the data volume, the images, the backups and the evidence notes were kept. |

How it was isolated: temporary containers, not Compose. A temporary PostgreSQL container
used the existing image and data volume on a private network and published **only**
`127.0.0.1:8000`; PostgreSQL's own port was never published. The viewer container shared that
container's network namespace and connected over loopback as `trace_viewer_reader`. The
viewer ran non-root with a read-only root file system, a `tmpfs` at `/tmp`, all capabilities
dropped and `no-new-privileges`.

Authentication in that arrangement was PostgreSQL's **local trust** rule for loopback
connections, with a non-secret placeholder in place of a password. That is acceptable only
inside a throwaway, single-machine, loopback-only demonstration. It does not exercise a real
password, and no role or password was changed. A shared or persistent setup needs a real
password and an authentication layer in front of the API, neither of which exists yet.

## Demonstration

1. **Telemetry.** Send traces through the collector so the pipeline stores them
   (the `workload-generator/` package produces them; its exact invocation is not covered
   here and has not been verified for this demo). The Traces tab then lists them.
2. **Analytics models.** `docker compose run --rm analytics-refresh` rebuilds the dbt
   views the viewer and the detector read.
3. **Anomaly detection (host).** The detector is a one-shot command. With
   `ANOMALY_DETECTOR_DB_PASSWORD` exported in your shell and `ANOMALY_DETECTOR_DB_HOST`
   left at its default (`localhost`; the main stack publishes PostgreSQL on port 5432):
   `python anomaly-detector/main.py --config anomaly-detector/runner_config.json`
4. **RCA (host).** Pick an anomaly id from the Anomalies tab or from the API, with
   `RCA_ANALYZER_DB_PASSWORD` exported:
   `python rca-analyzer/main.py --anomaly-id <64-character id>`
5. **View.** Reload the Anomalies tab, open an anomaly, read the RCA panel, and use
   *View related trace* to go to the trace.

Useful checks: `GET /api/health` (no database), `GET /api/anomalies?limit=5`,
`GET /api/anomalies/{id}/rca` (404 when no investigation exists).

## What `observed_value` means

The anomaly schema has no unit column, and no comment documents one. The unit depends on
`signal_name`, as set by the detectors in `anomaly-detector/detector/`:

| `signal_name` | `anomaly_type` | `observed_value` | Basis |
|---|---|---|---|
| `duration_ms` | `latency` | milliseconds | The signal is named `duration_ms`; its source columns are `trace_duration_ms` and `duration_ms`, and the trace one is computed as epoch seconds × 1000. |
| `retrieval_top_relevance_score` | `retrieval_quality` | unitless relevance score | The scale (for example 0 to 1) was not verified. |
| `retrieval_result_count` | `retrieval_quality` | a count of results | Count semantics follow from the signal name. |
| `error_rate` | `error_rate` | **not a rate: always `1.0`** | The detector sets `observed_value=1.0` for each error span. |
| `tool_error_rate` | `tool_failure` | **not a rate: always `1.0`** | Same, in the tool-failure detector. |

The Anomalies tab shows the bare number. It should show the unit from `signal_name`
(and not present `1.0` as a rate); that is a small follow-up in `trace-viewer/static/app.js`,
not done in this task. The latency unit is an inference from names and code, not from a
schema contract.

## Security notes

- `127.0.0.1:8000:8000` is the only published port. Do not widen it: there is no
  authentication, and anomaly and RCA text (`error_message`, `explanation`, `summary`)
  is served as stored.
- The viewer connects as `trace_viewer_reader`, which has SELECT only. Migration 015 adds
  SELECT on exactly three tables and `USAGE` on schema `public`.
- No password is written in any file here. `compose.trace-viewer.yml` refuses to start
  without `TRACE_VIEWER_DB_PASSWORD` in your environment or `.env`.
- The main stack's own ports (PostgreSQL 5432, Kafka, OTLP, Grafana) are published on all
  interfaces by `docker-compose.yml`. This task does not change that.
