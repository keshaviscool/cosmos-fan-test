"""
Scaled-down version of the dbt_marts_{label} DAG factory, collapsed to a
single "hourly" cadence so we can isolate and measure:
  1. Cosmos task fan-out (one Airflow task per dbt model/test)
  2. TestBehavior.AFTER_EACH vs TestBehavior.AFTER_ALL task count difference
  3. Whether the KubernetesExecutor `executor_config` blocks actually do
     anything when the real executor is LocalExecutor (they should be inert
     here -- that's the diagnostic point)

CHANGED FOR LOCAL TESTING:
  - S3 manifest -> local file (manifest_store volume)
  - Redshift -> Postgres warehouse, RedshiftIAMProfileMapping ->
    PostgresUserPasswordProfileMapping
  - GitHub remote -> local bare git repo (file:// URL)
  - 4 cadences -> 1 (hourly only)

UNCHANGED (this is the point of the test):
  - Guard / clone / seed / Cosmos / source_freshness / cleanup task structure
  - executor_config={"KubernetesExecutor": ...} left on every task
  - Slack failure callback wiring
  - Dominance-style selector building
"""
import os
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from git import Repo
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import TestBehavior, LoadMode
from cosmos.profiles import PostgresUserPasswordProfileMapping

DBT_BINARY = "/opt/airflow/dbt_env/bin/dbt"
DBT_REPO = os.environ.get("DBT_REPO", "file:///opt/airflow/local_git/dwh-transforms.git")
DBT_PROJECT_PATH = "/tmp/dwh-transforms"
MANIFEST_PATH = os.environ.get("MANIFEST_LOCAL_PATH", "/opt/airflow/manifest_store/manifest.json")
SLACK_CONN_ID = "slack"
SLACK_CHANNEL = "C0000000000"

TEST_BEHAVIOR = (
    TestBehavior.AFTER_ALL
    if os.environ.get("COSMOS_TEST_BEHAVIOR", "after_each") == "after_all"
    else TestBehavior.AFTER_EACH
)

KUBERNETES_EXECUTOR_CONFIG = {
    "image": "example/airflow-dwh-transforms:test",
    "request_memory": "512Mi",
    "limit_memory": "1Gi",
    "request_cpu": "250m",
    "limit_cpu": "500m",
}

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 0,
}

REFRESH_HIERARCHY = ["refresh:hourly"]


class DbtAlertException(Exception):
    priority = "Medium"
    emoji = ":red_circle:"
    title = "dbt deployment failed"
    action = "Check the task logs for details."

    def __init__(self, detail: str):
        self.detail = detail
        super().__init__(detail)


class GitCloneError(DbtAlertException):
    priority = "High"
    emoji = ":rotating_light:"
    title = "Git clone failed"
    action = "Verify the local bare repo / branch name exists."


class WrongBranchError(DbtAlertException):
    priority = "High"
    emoji = ":no_entry:"
    title = "Wrong branch on prod"
    action = "Merge your changes to `main` before triggering prod."


class FullRefreshOnScheduleError(DbtAlertException):
    priority = "High"
    emoji = ":no_entry:"
    title = "Full-refresh on scheduled run"
    action = "Only use `--full-refresh` on manual triggers."


class MissingConfigFileError(DbtAlertException):
    priority = "High"
    emoji = ":file_folder:"
    title = "Required config file missing"
    action = "Ensure `dbt_project.yml` is committed to `dwh-transforms`."


_PRIORITY_LABELS = {
    "High": ":red_circle: *High*",
    "Medium": ":large_yellow_circle: *Medium*",
    "Low": ":large_green_circle: *Low*",
}


def slack_failure_alert(context):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    log_url = context["task_instance"].log_url
    exception = context.get("exception", None)
    env = Variable.get("APP_ENV", default_var="unknown").upper()

    if isinstance(exception, DbtAlertException):
        emoji, title, priority = exception.emoji, exception.title, exception.priority
        detail, action = exception.detail[:300], exception.action
    else:
        emoji, title, priority = ":red_circle:", "dbt deployment failed", "Medium"
        detail = str(exception)[:300] if exception else "Unknown error"
        action = "Check the task logs for details."

    message = (
        f"{emoji} *{title}*\n"
        f">*Priority:* {_PRIORITY_LABELS.get(priority, priority)}\n"
        f">*DAG:* `{dag_id}`\n"
        f">*Task:* `{task_id}`\n"
        f">*Env:* `{env}`\n"
        f">*What happened:* {detail}\n"
        f">*Action:* {action}\n"
        f">*Logs:* <{log_url}|View logs>"
    )

    try:
        SlackAPIPostOperator(
            task_id="slack_alert",
            slack_conn_id=SLACK_CONN_ID,
            channel=SLACK_CHANNEL,
            text=message,
            username="airflow",
        ).execute(context=context)
    except Exception as e:
        print(f"Slack alert failed to send (expected in local test env): {e}")


def _build_selector(refresh_tag: str):
    if refresh_tag not in REFRESH_HIERARCHY:
        raise ValueError(f"Unknown refresh_tag {refresh_tag!r}. Valid values: {REFRESH_HIERARCHY}")
    idx = REFRESH_HIERARCHY.index(refresh_tag)
    dominant_tags = REFRESH_HIERARCHY[:idx]
    return {
        "select": [f"tag:{refresh_tag}", "tag:marts"],
        "exclude": [f"tag:{t}" for t in dominant_tags],
    }


def _run_dbt(command_args, project_dir, profiles_dir, description, check=True, timeout=600):
    cmd = [DBT_BINARY] + command_args + ["--profiles-dir", profiles_dir, "--project-dir", project_dir]
    print(f"\n{'='*60}\n{description}\n{' '.join(cmd)}\n{'='*60}")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=False)
    if result.stdout:
        print("STDOUT:\n", result.stdout[-8000:])
    if result.stderr:
        print("STDERR:\n", result.stderr[-2000:])
    if check and result.returncode != 0:
        raise subprocess.CalledProcessError(result.returncode, cmd, output=result.stdout, stderr=result.stderr)
    return result


REFRESH_TAG, CRON, LABEL = "refresh:hourly", "@hourly", "hourly"
selector = _build_selector(REFRESH_TAG)


@dag(
    dag_id="dbt_marts_hourly",
    default_args={**default_args, "on_failure_callback": slack_failure_alert},
    description=f"Scaled-down local test of the dbt marts fan-out pipeline ({LABEL})",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "data-warehouse", "marts", LABEL, "fanout-test"],
    params={
        "branch": Param("main", type="string", description="Git branch to deploy"),
        "isFullRefresh": Param(False, type="boolean", description="Full refresh — manual triggers only"),
        "selector": Param(" ", type="string", description="Leave blank for automatic selector"),
    },
)
def pipeline():
    env = Variable.get("APP_ENV", default_var="stage").lower()

    @task(on_failure_callback=slack_failure_alert)
    def run_guards():
        ctx = get_current_context()
        run_type = ctx["dag_run"].run_type
        branch = ctx["params"]["branch"]
        is_full_refresh = ctx["params"]["isFullRefresh"]
        param_selector = ctx["params"].get("selector", "").strip()

        if is_full_refresh and run_type == "scheduled":
            raise FullRefreshOnScheduleError("--full-refresh was triggered on a scheduled run.")
        if env == "prod" and branch != "main":
            raise WrongBranchError(f"Attempted to deploy branch `{branch}` to production.")

        if param_selector and run_type == "manual":
            resolved_select, resolved_exclude = [param_selector], []
        else:
            resolved_select, resolved_exclude = selector["select"], selector["exclude"]

        print(f"Guards passed | env={env} branch={branch} full_refresh={is_full_refresh} select={resolved_select}")
        return {"branch": branch, "is_full_refresh": is_full_refresh}

    @task(
        on_failure_callback=slack_failure_alert,
        executor_config={"KubernetesExecutor": KUBERNETES_EXECUTOR_CONFIG},
    )
    def clone_repo(guard_result: dict):
        branch = guard_result["branch"]
        _ = Variable.get("git-read-secret", default_var="unused-for-local-clone")

        if Path(DBT_PROJECT_PATH).exists():
            shutil.rmtree(DBT_PROJECT_PATH)

        print(f"Cloning {DBT_REPO} branch={branch} ...")
        try:
            Repo.clone_from(DBT_REPO, DBT_PROJECT_PATH, branch=branch, depth=1)
        except Exception as e:
            raise GitCloneError(f"Failed to clone branch `{branch}`. Error: {e}") from None

        if not (Path(DBT_PROJECT_PATH) / "dbt_project.yml").exists():
            raise MissingConfigFileError("`dbt_project.yml` not found in cloned project root.")

    @task(executor_config={"KubernetesExecutor": KUBERNETES_EXECUTOR_CONFIG})
    def dbt_seed(guard_result: dict):
        is_full_refresh = guard_result["is_full_refresh"]
        seed_args = ["seed", "--target", env]
        if is_full_refresh:
            seed_args.append("--full-refresh")
        _run_dbt(seed_args, DBT_PROJECT_PATH, DBT_PROJECT_PATH, f"Load seeds ({env})", check=False, timeout=180)

    dbt_models = DbtTaskGroup(
        group_id="dbt_models",
        project_config=ProjectConfig(
            manifest_path=MANIFEST_PATH,
            project_name="dwh_transforms",
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            select=selector["select"],
            exclude=selector["exclude"],
            test_behavior=TEST_BEHAVIOR,
        ),
        profile_config=ProfileConfig(
            profile_name="dwh_transforms",
            target_name=env,
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id="postgres_warehouse"
            ),
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_BINARY,
            dbt_project_path=DBT_PROJECT_PATH,
        ),
        default_args={"on_failure_callback": slack_failure_alert},
        operator_args={"executor_config": {"KubernetesExecutor": KUBERNETES_EXECUTOR_CONFIG}},
    )

    @task(executor_config={"KubernetesExecutor": KUBERNETES_EXECUTOR_CONFIG})
    def source_freshness():
        _run_dbt(
            ["source", "freshness", "--target", env],
            DBT_PROJECT_PATH, DBT_PROJECT_PATH,
            f"Check source freshness ({env})", check=False, timeout=120,
        )

    @task(trigger_rule="all_done")
    def cleanup():
        if Path(DBT_PROJECT_PATH).exists():
            shutil.rmtree(DBT_PROJECT_PATH)
            print(f"Cleaned up {DBT_PROJECT_PATH}")

    guard_result = run_guards()
    clone_result = clone_repo(guard_result)
    seed_result = dbt_seed(guard_result)

    clone_result >> seed_result >> dbt_models >> source_freshness() >> cleanup()


pipeline()