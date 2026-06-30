# Scaled-down dbt_marts fan-out test

A smaller, fully-local version of your `dbt_marts_{label}` architecture:
same task structure (guards → clone → seed → Cosmos `DbtTaskGroup` → source
freshness → cleanup), same `executor_config` / Slack-callback / dominance-
selector patterns — but collapsed to one cadence, `LocalExecutor`, Postgres
instead of Redshift, and a local git repo + local file instead of GitHub + S3.

What this is for: measuring Cosmos's per-task fan-out (task count, idle/run
CPU) and confirming whether `executor_config={"KubernetesExecutor": ...}`
actually does anything outside a real Kubernetes-backed executor.

## 0. Prereqs

Docker, Docker Compose v2, ~4GB free RAM, no internet dependency beyond the
base image pulls.

## 1. Build everything

```bash
docker compose build
docker compose up -d airflow-meta-db warehouse-db
```

## 2. Seed the local "GitHub repo"

```bash
docker compose run --rm git-seed
```

This turns `dbt_source_repo/` into a bare git repo on the `git_store`
volume — the DAG's `clone_repo` task clones from this via a `file://` URL,
exactly like it clones from GitHub in prod, just local.

## 3. Build the manifest (simulates CI pushing `manifest.json` on commit)

```bash
docker compose run --rm scheduler \
  bash /opt/airflow/scripts/build_manifest.sh
```

(`scheduler`'s entrypoint isn't running yet — `run --rm` here just uses the
image as a one-off worker. This writes `manifest.json` to the
`manifest_store` volume, which `ProjectConfig(manifest_path=...)` in the DAG
reads.)

## 4. Init and start Airflow

```bash
docker compose run --rm airflow-init
docker compose up -d scheduler webserver
```

UI: http://localhost:8080 (admin/admin). You should see `dbt_marts_hourly`.

## 5. Sanity-check one run

```bash
docker compose exec scheduler airflow dags unpause dbt_marts_hourly
docker compose exec scheduler airflow dags trigger dbt_marts_hourly
```

Watch it in the UI Grid view. If `dbt_seed`/`source_freshness` or `dbt_models`
fail, check logs first — common local-only issues are the warehouse Postgres
not being ready yet, or the manifest step (3) not having been run.

## 6. Count the task fan-out

```bash
cd scripts
chmod +x *.sh
./count_tasks.sh dbt_marts_hourly
```

With the 6 mart models + their tests in `dbt_source_repo`, `AFTER_EACH`
(the default here, matching prod) should show noticeably more tasks than
`AFTER_ALL`. To compare:

```bash
# edit docker-compose.yaml: COSMOS_TEST_BEHAVIOR: "after_all"
docker compose up -d --force-recreate scheduler webserver
./count_tasks.sh dbt_marts_hourly
```

Put both counts side by side — this is your concrete "AFTER_EACH vs
AFTER_ALL" task-count number for the real project, scaled by however many
more models/tests the real `dwh-transforms` repo has relative to this
6-model stand-in.

## 7. Idle vs run compute

```bash
# idle baseline, DAG unpaused but not running
./monitor.sh ../results/idle.csv 180 5 cosmos_fanout_scheduler

# run, monitoring in the background
./monitor.sh ../results/run.csv 120 2 cosmos_fanout_scheduler &
docker compose exec scheduler airflow dags trigger dbt_marts_hourly
wait

python3 analyze.py ../results/idle.csv
python3 analyze.py ../results/run.csv
```

Repeat with `COSMOS_TEST_BEHAVIOR=after_all` to see how much of the run-time
CPU/duration tracks task count.

## 8. The executor_config question

Open a task instance for any task in the Airflow UI (e.g. `clone_repo`) →
"Details". Under `LocalExecutor` you'll see no pod/queue info tied to the
`KubernetesExecutor` key in `executor_config` — it's a no-op here, by
design of this test. If your real deployment shows pods being created per
task (check with `kubectl get pods` during a run, or your executor's task
instance "Queue"/"Pool" details), the `executor_config` is live in prod and
the per-task pod fan-out from step 6's task count is your real cost driver.
If real prod also shows no pod-per-task behavior, the `executor_config`
blocks are dead weight and the cost is coming from elsewhere (subprocess
spin-up per task, dbt parse, etc.) — worth removing them either way to
avoid confusion.

## Notes on what's deliberately different from prod

- `schedule=None` here (manual trigger only) instead of `@hourly`, so you
  control exactly when runs happen for clean measurement windows.
- `source freshness` will report "no sources found" since this stand-in
  project has no `sources.yml` — harmless, it's `check=False` in `_run_dbt`
  just like prod.
- Slack alerts will fail to send (no real Slack connection configured) and
  just print to logs — this exercises the same try/except path your
  production code already has as a safety net.