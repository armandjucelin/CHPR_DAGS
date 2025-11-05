# dags/CHPR_DAGS.py
from __future__ import annotations

import os
import sys
import logging
import shlex
import subprocess
from collections import deque
from datetime import datetime, timedelta
from typing import Iterable, Mapping, Any
from pathlib import Path

import pendulum
from airflow.sdk import dag, task
from airflow.exceptions import AirflowException
try:
    # Airflow 3 style
    from airflow.sdk import Variable  # type: ignore
except Exception:  # Airflow < 3 fallback
    from airflow.models import Variable  # type: ignore

from airflow.models.taskinstance import TaskInstance
from airflow.models.dagrun import DagRun
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.sdk.bases.sensor import BaseSensorOperator

# ================================ CONFIG ====================================
EMAILS = ["nghan@chprhealth.org", "ngha.mbuh@gmail.com"]
LOCAL_TZ = pendulum.timezone("Africa/Douala")
DEFAULT_ARTIFACTS_DIR = "/tmp/airflow_artifacts"
DEFAULT_SCRIPTS_DIR = "/mnt/d/SCRIPTS_PATH/inspiretb"   # override with env/Variable 'script_path'

# ================================ UTILS =====================================
def _get_cfg_runtime(name: str, default: str | None = None) -> str:
    """
    Resolve config at RUNTIME with this priority:
      1) env[name] or env[name.upper()]
      2) Airflow Variable[name] or Variable[name.upper()]
      
      3) default (if given) else raise
    """
    env_val = os.environ.get(name) or os.environ.get(name.upper())
    if env_val:
        return env_val
    try:
        var_val = Variable.get(name, default=None)
        if var_val:
            return var_val
        var_val_uc = Variable.get(name.upper(), default=None)
        if var_val_uc:
            return var_val_uc
    except Exception:
        pass
    if default is not None:
        return default
    raise RuntimeError(f"Missing config: {name} (set env/Variable '{name}' or '{name.upper()}')")

def _normalize_path_for_env(path_str: str) -> str:
    """Best-effort normalization so a Windows drive path works under WSL.
    - If path like 'D:\\folder\\file' and running on POSIX, convert to '/mnt/d/folder/file'.
    - Otherwise return as-is.
    """
    try:
        if os.name != "nt" and len(path_str) >= 3 and path_str[1:3] == ":\\":
            drive = path_str[0].lower()
            rest = path_str[3:].replace("\\", "/")
            return f"/mnt/{drive}/{rest}"
    except Exception:
        pass
    return path_str


class FileMtimeSensor(BaseSensorOperator):
    """Rescheduling sensor that fires when a file/dir mtime increases.
    Stores baseline in Variable on first run to avoid a false positive.
    """

    def __init__(self, path: str, var_key: str, watch_dir: bool = False, **kwargs):
        super().__init__(**kwargs)
        self.path = path
        self.var_key = var_key
        self.watch_dir = watch_dir

    def _latest_mtime(self) -> float:
        p = Path(self.path)
        if not p.exists():
            return -1.0
        if self.watch_dir:
            latest = -1.0
            try:
                for sub in p.rglob("*"):
                    try:
                        if sub.is_file():
                            latest = max(latest, sub.stat().st_mtime)
                    except Exception:
                        continue
            except Exception:
                latest = max(latest, p.stat().st_mtime)
            return latest
        else:
            return p.stat().st_mtime

    def poke(self, context) -> bool:
        try:
            mtime = self._latest_mtime()
            if mtime < 0:
                self.log.info("Watch path not found yet (will keep checking): %s", self.path)
                return False

            # Get previous mtime from Airflow Variable
            try:
                # Try the Airflow 3.1 API with 'default' parameter
                try:
                    prev_val = Variable.get(self.var_key, default=None)
                except TypeError:
                    # Fallback for older Airflow versions that don't support default parameter
                    try:
                        prev_val = Variable.get(self.var_key)
                    except KeyError:
                        prev_val = None

                if prev_val is None:
                    # First time - establish baseline
                    Variable.set(self.var_key, str(mtime))
                    self.log.info("Baseline established for %s (var_key: %s) => mtime=%.6f",
                                self.path, self.var_key, mtime)
                    return False
                prev = float(prev_val)
            except Exception as e:
                self.log.warning("Failed to get previous mtime from variable '%s': %s. Using default 0.0",
                               self.var_key, e)
                prev = 0.0

            # If this is the first run (prev=0.0), establish baseline and don't trigger
            if prev == 0.0:
                try:
                    Variable.set(self.var_key, str(mtime))
                    self.log.info("Baseline established for %s (var_key: %s) => mtime=%.6f (first run)",
                                self.path, self.var_key, mtime)
                    return False
                except Exception as e:
                    self.log.error("Failed to set baseline variable '%s': %s", self.var_key, e)

            changed = mtime > prev
            self.log.info(
                "File watch check: path=%s var_key=%s current_mtime=%.6f prev_mtime=%.6f changed=%s watch_dir=%s",
                self.path, self.var_key, mtime, prev, changed, self.watch_dir
            )

            if changed:
                self.log.info("🔥 TRIGGER DETECTED! Path changed: %s (%.6f > %.6f)",
                            self.path, mtime, prev)

            return bool(changed)

        except Exception as e:
            self.log.exception("Error during file watch poke operation for path %s: %s", self.path, e)
            # Return False to keep trying rather than failing the sensor
            return False

# =============================== CALLBACKS ==================================
def on_failure_notify(context):
    log = logging.getLogger(__name__)
    ti = context.get("ti")
    dag_id = (context.get("dag_run").dag_id if context.get("dag_run")
              else context.get("dag").dag_id)
    run_id = context.get("run_id")
    task_id = context.get("task_instance").task_id if context.get("task_instance") else "unknown-task"
    try_number = ti.try_number if ti else "n/a"
    owner = context.get("task").owner if context.get("task") else "unknown-owner"
    log.error("❌ Failure in DAG '%s' / task '%s' (run_id=%s, try=%s, owner=%s).",
              dag_id, task_id, run_id, try_number, owner)

    message = (
        f":rotating_light: *Airflow Failure*\n"
        f"*DAG*: `{dag_id}`\n"
        f"*Task*: `{task_id}`\n"
        f"*Run*: `{run_id}` (try {try_number})\n"
        f"*Log*: {ti.log_url if ti else 'n/a'}"
    )

    slack_conn_id = os.environ.get("SLACK_WEBHOOK_CONN_ID") or Variable.get("SLACK_WEBHOOK_CONN_ID", default=None)
    if slack_conn_id:
        try:
            from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
            SlackWebhookHook(slack_webhook_conn_id=slack_conn_id).send(text=message)
            log.info("Sent Slack alert via connection %s", slack_conn_id)
            return
        except Exception as e:
            log.warning("Slack via connection failed: %s", e)

    slack_webhook = os.environ.get("SLACK_WEBHOOK") or Variable.get("SLACK_WEBHOOK", default=None)
    if slack_webhook:
        try:
            import requests
            resp = requests.post(slack_webhook, json={"text": message}, timeout=10)
            if resp.ok:
                log.info("Sent Slack alert via raw webhook.")
            else:
                log.warning("Slack webhook POST failed: HTTP %s", resp.status_code)
        except Exception as e:
            log.warning("Slack via raw webhook failed: %s", e)

# =============================== DAG FACTORY =================================
def build_script_dag(
    *,
    dag_id: str,
    schedule: str | None,
    start_date: datetime,
    jobs: Iterable[Mapping],      # [{"task_id":"...","script":"rel/path.py","args":[...]}]
    edges: Iterable[tuple] = (),
    tags: list[str] | None = None,
    retries: int = 2,
    retry_delay_minutes: int = 5,
    # CONCURRENCY KNOBS (per DAG)
    max_active_runs: int | None = None,     # None => use Airflow config default
    max_active_tasks: int | None = None,    # None => use Airflow config default
    catchup: bool = False,
    retry_exponential_backoff: bool = True,
    execution_timeout_minutes: int = 60,
    dagrun_timeout_minutes: int = 180,
    sla_minutes: int | None = None,
    pool: str | None = None,
    queue: str | None = None,
    priority_weight: int | None = None,
    doc_md: str | None = None,
    # Optional: only run tasks if a file/dir changed since last success
    file_change_watch_path: str | None = None,           # direct absolute path (file or dir)
    file_change_watch_path_key: str | None = None,       # env/Variable key that yields a base directory
    file_change_watch_file: str | None = None,           # join with base directory
    file_change_variable_key: str | None = None,
    trigger_on_change_immediately: bool = False,         # use rescheduling sensor + @continuous
    file_change_poke_interval_seconds: int = 5,
):
    if tags is None:
        tags = ["external-script"]

    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "retries": retries,
        "retry_delay": timedelta(minutes=retry_delay_minutes),
        "retry_exponential_backoff": retry_exponential_backoff,
        "email": EMAILS,
        "email_on_failure": True,
        "email_on_retry": False,
        "on_failure_callback": on_failure_notify,
    }
    if priority_weight is not None:
        default_args["priority_weight"] = priority_weight

    if not doc_md:
        job_lines = "\n".join(f"- `{j['task_id']}` → `{j['script']}`" for j in jobs)
        doc_md = f"### Pipeline `{dag_id}`\n\nJobs:\n{job_lines}\n\nSchedule: `{schedule}` (Africa/Douala)\n"

    # Build DAG decorator kwargs dynamically so older Airflow versions won’t choke
    dag_kwargs: dict[str, Any] = dict(
        dag_id=dag_id,
        start_date=start_date,
        schedule=schedule,
        catchup=catchup,
        tags=tags,
        default_args=default_args,
        dagrun_timeout=timedelta(minutes=dagrun_timeout_minutes),
        doc_md=doc_md,
    )
    if max_active_runs is not None:
        dag_kwargs["max_active_runs"] = max_active_runs
    if max_active_tasks is not None:
        dag_kwargs["max_active_tasks"] = max_active_tasks

    @dag(**dag_kwargs)
    def _factory_dag():
        logger = logging.getLogger(f"airflow.task.{dag_id}")

        # --------------------- Resolve watch path (optional) -----------------
        resolved_watch_path: str | None = None
        watch_is_dir = False
        if file_change_watch_path or file_change_watch_path_key:
            try:
                if file_change_watch_path:
                    resolved_watch_path = _normalize_path_for_env(file_change_watch_path)
                    logger.info("DAG %s: Using direct watch path: %s", dag_id, resolved_watch_path)
                else:
                    base_dir = _get_cfg_runtime(file_change_watch_path_key)  # dynamic, cross-platform
                    candidate = Path(base_dir)
                    if file_change_watch_file:
                        candidate = candidate / file_change_watch_file
                    resolved_watch_path = _normalize_path_for_env(str(candidate))
                    logger.info("DAG %s: Resolved watch path from base_dir '%s' + file '%s' = %s",
                              dag_id, base_dir, file_change_watch_file or "(none)", resolved_watch_path)

                # Check if path exists and determine type
                watch_path_obj = Path(resolved_watch_path)
                if watch_path_obj.exists():
                    watch_is_dir = watch_path_obj.is_dir()
                    logger.info("DAG %s: Watch path exists, is_dir=%s: %s", dag_id, watch_is_dir, resolved_watch_path)
                else:
                    # Path doesn't exist yet - try to infer from extension or assume directory
                    watch_is_dir = not bool(watch_path_obj.suffix)  # No extension = likely directory
                    logger.warning("DAG %s: Watch path does not exist yet, assuming is_dir=%s: %s",
                                 dag_id, watch_is_dir, resolved_watch_path)
            except Exception as e:
                logger.exception("DAG %s: Failed to resolve watch path configuration: %s", dag_id, e)
                resolved_watch_path = None
                watch_is_dir = False

        # ------------------------- File-change wait --------------------------
        sensor_task = None
        guard_task = None
        if resolved_watch_path:
            # Create a unique variable key for each DAG's file watching
            if file_change_variable_key:
                var_key = file_change_variable_key
            else:
                # Include path hash to make it unique across different watched paths
                import hashlib
                path_hash = hashlib.md5(resolved_watch_path.encode()).hexdigest()[:8]
                var_key = f"{dag_id}__mtime__{path_hash}"

            if trigger_on_change_immediately:
                # Wait for change (reschedules, no worker slot consumed)
                sensor_task = FileMtimeSensor(
                    task_id=f"wait_for_change__{dag_id}",
                    path=resolved_watch_path,
                    var_key=var_key,
                    watch_dir=watch_is_dir,
                    mode="reschedule",
                    poke_interval=file_change_poke_interval_seconds,
                )
            else:
                # Fallback to simple guard that only runs jobs if changed
                def _should_run_if_changed() -> bool:
                    try:
                        p = Path(resolved_watch_path)
                        if not p.exists():
                            logger.warning("Guard: path not found, skipping run. Path=%s", p)
                            return False
                        def latest(path: Path) -> float:
                            if path.is_file():
                                return path.stat().st_mtime
                            latest_ = -1.0
                            for sub in path.rglob("*"):
                                try:
                                    if sub.is_file():
                                        latest_ = max(latest_, sub.stat().st_mtime)
                                except Exception:
                                    continue
                            return max(latest_, path.stat().st_mtime)
                        mtime = latest(p)
                        try:
                            # Try the Airflow 3.1 API with 'default' parameter
                            try:
                                prev_val = Variable.get(var_key, default="0")
                            except TypeError:
                                # Fallback for older Airflow versions
                                try:
                                    prev_val = Variable.get(var_key)
                                except KeyError:
                                    prev_val = "0"
                            prev = float(prev_val)
                        except Exception:
                            prev = 0.0

                        # If this is the first run (prev=0.0), establish baseline and don't trigger
                        if prev == 0.0:
                            try:
                                Variable.set(var_key, str(mtime))
                                logger.info("Guard: Baseline established for %s (var_key: %s) => mtime=%.6f (first run)",
                                          p, var_key, mtime)
                                return False
                            except Exception as e:
                                logger.error("Guard: Failed to set baseline variable '%s': %s", var_key, e)

                        changed = mtime > prev
                        logger.info(
                            "Guard check: path=%s current_mtime=%s prev_mtime=%s changed=%s",
                            p, mtime, prev, changed,
                        )
                        return bool(changed)
                    except Exception as e:
                        logger.exception("Guard check failed; defaulting to skip. %s", e)
                        return False

                guard_task = ShortCircuitOperator(
                    task_id="proceed_if_changed",
                    python_callable=_should_run_if_changed,
                    dag=None,
                )

        @task(
            execution_timeout=timedelta(minutes=execution_timeout_minutes),
            sla=(timedelta(minutes=sla_minutes) if sla_minutes else None),
        )
        def run_external(
            script_rel_path: str,
            args: list[str] | None = None,
            task_instance: "TaskInstance" | None = None,   # injected by TaskFlow
            dag_run: "DagRun" | None = None,               # injected by TaskFlow
            data_interval_start=None,                      # injected by TaskFlow
        ):
            # ---- derive context (no get_current_context) ----
            task_id_local = task_instance.task_id if task_instance else "unknown"
            dag_id_local = (task_instance.dag_id if task_instance else (dag_run.dag_id if dag_run else "unknown"))
            run_id = dag_run.run_id if dag_run else "manual__unknown"
            if data_interval_start is None:
                data_interval_start = pendulum.now("UTC")

            # ---- resolve config at runtime ----
            script_base = _get_cfg_runtime("script_path", default=DEFAULT_SCRIPTS_DIR)
            artifacts_base = _get_cfg_runtime("artifacts_path", default=DEFAULT_ARTIFACTS_DIR)

            # ---- compute paths / guards ----
            script_file = Path(script_base) / script_rel_path
            logger.info("Resolved script base: %s", script_base)
            logger.info("Resolved script file: %s", script_file)

            if not script_file.exists():
                parent = script_file.parent
                sample = []
                if parent.exists():
                    try:
                        sample = [p.name for p in list(parent.glob("*"))[:40]]
                    except Exception:
                        sample = ["<error listing directory>"]
                raise AirflowException(
                    "Script not found.\n"
                    f"  Expected: {script_file}\n"
                    f"  Base dir: {script_base}\n"
                    f"  Parent exists: {parent.exists()}  |  Parent: {parent}\n"
                    f"  Parent sample: {sample}\n"
                    "Tip: Set env/Variable 'script_path' to the directory that contains your project folders."
                )

            run_dir = Path(artifacts_base) / dag_id_local / task_id_local / data_interval_start.strftime("%Y-%m-%dT%H%M%S")
            run_dir.mkdir(parents=True, exist_ok=True)

            # ---- build command ----
            safe_args = [str(a) for a in (args or [])]
            cmd = [sys.executable, str(script_file), *safe_args]
            logger.info("Launching external script: %s", " ".join(shlex.quote(c) for c in cmd))
            logger.info("Working directory for this run: %s", run_dir)

            # ---- stream logs; keep rolling tail ----
            tail = deque(maxlen=200)
            rc = None
            try:
                # Pass environment variables to subprocess
                # This ensures systemd-set vars (BASE_PATH, script_path, PYTHONPATH) are inherited
                with subprocess.Popen(
                    cmd,
                    cwd=run_dir,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    bufsize=1,
                    env=os.environ.copy(),  # CRITICAL: Pass environment variables to subprocess
                ) as proc:
                    assert proc.stdout is not None
                    for line in proc.stdout:
                        line = line.rstrip("\n")
                        logger.info(line)
                        tail.append(line[-1000:])
                    rc = proc.wait()
            except Exception as e:
                logger.exception("Subprocess failed to start or crashed: %s", e)
                raise

            if rc != 0:
                tail_text = "\n".join(tail)
                raise AirflowException(
                    f"Script failed (exit={rc}) for {script_file.name}\n"
                    f"Command: {' '.join(shlex.quote(c) for c in cmd)}\n"
                    f"Last output:\n{tail_text}"
                )

            return {
                "script": str(script_file),
                "args": safe_args,
                "run_dir": str(run_dir),
                "exit_code": rc,
                "tail": "\n".join(tail)[-2000:],
                "run_id": run_id,
            }

        # ---- build tasks (NO .partial) ----
        task_map: dict[str, Any] = {}
        for job in jobs:
            t_id = job["task_id"]
            script_rel = job["script"]
            job_args = job.get("args") or []

            op = run_external
            # operator-level params go in override()
            if pool or queue or (priority_weight is not None):
                kw: dict[str, Any] = {}
                if pool:
                    kw["pool"] = pool
                if queue:
                    kw["queue"] = queue
                if priority_weight is not None:
                    kw["priority_weight"] = priority_weight
                op = op.override(**kw)

            task_node = op.override(task_id=t_id)(script_rel, job_args)
            # If guard/sensor enabled, gate each job behind it
            if sensor_task is not None:
                sensor_task >> task_node
            elif guard_task is not None:
                guard_task >> task_node
            task_map[t_id] = task_node

        # ---- validate / apply edges ----
        for u, v in edges:
            if u not in task_map or v not in task_map:
                known = ", ".join(sorted(task_map.keys()))
                raise ValueError(f"Invalid edge {u} -> {v}. Known tasks: [{known}]")
            task_map[u] >> task_map[v]

        # If guard enabled, add final recorder to persist latest mtime only on success
        if (sensor_task is not None or guard_task is not None) and resolved_watch_path:
            # Use the same variable key as defined earlier
            var_key_local = var_key

            @task(task_id=f"record_mtime__{dag_id}", trigger_rule="all_success")
            def record_latest_mtime():
                p = Path(resolved_watch_path)
                if p.exists():
                    if p.is_file():
                        mtime = p.stat().st_mtime
                    else:
                        latest = -1.0
                        for sub in p.rglob("*"):
                            try:
                                if sub.is_file():
                                    latest = max(latest, sub.stat().st_mtime)
                            except Exception:
                                continue
                        mtime = max(latest, p.stat().st_mtime)
                    Variable.set(var_key_local, str(mtime))
                    logger.info("Recorded latest mtime %.6f to Variable '%s' for %s", mtime, var_key_local, p)
                else:
                    logger.warning("Recorder: file no longer exists; not updating mtime. %s", p)

            rec = record_latest_mtime()
            for _t in task_map.values():
                _t >> rec

    return _factory_dag()

# ============================== DEFINE DAGS ==================================
# NOTE: increase per-DAG concurrency here. These values allow overlapping runs
# and multiple tasks per DAG to execute at once (subject to executor & pools).
DAG_SPECS = [

# WAVE11 USER ACTIVITY

    {
        "dag_id": "wave11_user_activity_runner",
        "schedule": "*/30 6-18 * * *",
        "start_date": datetime(2025, 8, 24, 6, 0, tzinfo=LOCAL_TZ),
        "jobs": [{"task_id": "user_activity", "script": "WAVE11_PROJECT/WAVE11_USER_ACTIVITY.py"}],
        "edges": [],
        "tags": ["wave11", "external-script"],
        "retries": 2,
        "retry_delay_minutes": 5,
        "max_active_runs": 2,
        "max_active_tasks": 4,
        "pool": "data_import_pool",
    },

# WAVE11 PIPELINE

    {
        "dag_id": "WAVE11_PIPELINE",
        "schedule": "0 6-18 * * *",  # hourly at HH:00 between 06:00–18:00
        "start_date": datetime(2025, 8, 24, 6, 5, tzinfo=LOCAL_TZ),
        "jobs": [
            {"task_id": "wave11_importation",  "script": "WAVE11_PROJECT/WAVE11_DATA_IMPORTATION.py"},
            {"task_id": "wave11_data_processor", "script": "WAVE11_PROJECT/WAVE11_DATA_PROCESSING.py"},
        ],
        "edges": [("wave11_importation", "wave11_data_processor")],
        "tags": ["wave11", "pipeline"],
        "retries": 2,
        "retry_delay_minutes": 5,
        "max_active_runs": 2,
        "max_active_tasks": 4,
        "pool": "data_import_pool",
    },


# CONFIGURE FOR WAVE11 REPORT IMPORTATION


    {
        "dag_id": "WAVE11_REPORTS",
        "schedule": "5 6-18/3 * * *",                     # 06:05, 09:05, 12:05, 15:05, 18:05
        "start_date": datetime(2025, 8, 24, 6, 10, tzinfo=LOCAL_TZ),
        "catchup": False,
        "jobs": [
            {
                "task_id": "WAVE11_REPORTS_IMPORTATION",
                "script": "WAVE11_PROJECT/WAVE11_DATA_IMPORTATION_REPORTS.py",
            },

            {
                "task_id": "WAVE11_REPORTS_PROCESSING",
                "script": "WAVE11_PROJECT/WAVE11_DATA_PROCESSING.py"
            }
        ],
        "edges": [('WAVE11_REPORTS_IMPORTATION', 'WAVE11_REPORTS_PROCESSING')],
        "tags": ["wave11", "reports", "external-script"],
        "retries": 2,
        "retry_delay_minutes": 5,
        "max_active_runs": 2,
        "max_active_tasks": 4,
        "pool": "data_import_pool",
    },


# CONFIGURE THE GLOBAL OUTCOME USER ACTIVITY MONITORING

    {
        "dag_id": "GLOBAL_OUTCOME_USER_ACTIVITY_MONITORING",
        "schedule": "*/30 6-18 * * *",
        "start_date": datetime(2025, 8, 24, 6, 15, tzinfo=LOCAL_TZ),
        "jobs": [{"task_id": "global_outcome_user_activity",
                  "script": "GLOBAL_OUTCOME_PROJECT/GLOBAL_OUTCOME_USER_ACTIVITY_MONITORING.py"}],
        "edges": [],
        "tags": ["global_outcome", "user_activity", "external-script"],
        "retries": 2,
        "retry_delay_minutes": 5,
        "max_active_runs": 2,
        "max_active_tasks": 4,
        "pool": "data_import_pool",
    },

# CONFIGURE THE GLOBAL OUTCOME PIPELINE

    {
        "dag_id": "GLOBAL_OUTCOME_PIPELINE",
        "schedule": "0 6-18 * * *",  # hourly
        "start_date": datetime(2025, 8, 24, 6, 20, tzinfo=LOCAL_TZ),
        "jobs": [
            {"task_id": "global_outcome_import",
             "script": "GLOBAL_OUTCOME_PROJECT/GLOBAL_OUTCOME_IMPORTAION.py"},
            {"task_id": "global_outcome_process",
             "script": "GLOBAL_OUTCOME_PROJECT/GLOBAL_OUTCOME_PROCESSING.py"},
        ],
        "edges": [("global_outcome_import", "global_outcome_process")],
        "tags": ["global_outcome", "pipeline", "external-script"],
        "retries": 2,
        "retry_delay_minutes": 5,
        "max_active_runs": 2,
        "max_active_tasks": 4,
        "pool": "data_import_pool",
    },


    {
        "dag_id": "GLOBAL_OUTCOME_REPORTS",
        "schedule": "5 6-18/3 * * *",                     # 06:05, 09:05, 12:05, 15:05, 18:05
        "start_date": datetime(2025, 8, 24, 6, 25, tzinfo=LOCAL_TZ),
        "catchup": False,
        "jobs": [
            {
                "task_id": "GLOBAL_OUTCOME_REPORTS_IMPORTATION",
                "script": "GLOBAL_OUTCOME_PROJECT/GLOBAL_OUTCOME_IMPORTATION_REPORTS.py",
            },
            {
                "task_id": "global_outcome_process_reports",
                "script": "GLOBAL_OUTCOME_PROJECT/GLOBAL_OUTCOME_PROCESSING.py"
            },
        ],
        "edges": [("GLOBAL_OUTCOME_REPORTS_IMPORTATION", "global_outcome_process_reports")],
        "tags": ["global_outcome", "reports", "external-script"],
        "retries": 2,
        "retry_delay_minutes": 5,
        "max_active_runs": 2,
        "max_active_tasks": 4,
        "pool": "data_import_pool",
    },


# CONFIGURE THE VIRAL LOAD USER ACTIVITY

    {
        "dag_id": "VIRAL_LOAD_USER_ACTIVITY",
        "schedule": "*/30 6-18 * * *",
        "start_date": datetime(2025, 8, 24, 6, 30, tzinfo=LOCAL_TZ),
        "jobs": [{"task_id": "viral_load_user_activity",
                  "script": "VIRAL_LOAD_PROJECT/VIRAL_LOAD_USER_ACTIVITY.py"}],
        "edges": [],
        "tags": ["viral_load", "user_activity", "external-script"],
        "retries": 2,
        "retry_delay_minutes": 5,
        "max_active_runs": 2,
        "max_active_tasks": 4,
        "pool": "data_import_pool",
    },

# CONFIGURE THE VIRAL LOAD PIPELINE

    {
        "dag_id": "VIRAL_LOAD_PIPELINE",
        "schedule": "*/30 6-18 * * *",
        "start_date": datetime(2025, 8, 24, 6, 35, tzinfo=LOCAL_TZ),
        "jobs": [
            {"task_id": "viral_load_import",
             "script": "VIRAL_LOAD_PROJECT/VIRAL_LOAD_IMPORTATION_SCRIPTS.py"},
            {"task_id": "viral_load_clean",
             "script": "VIRAL_LOAD_PROJECT/VIRAL LOAD DATA CLEANING.py"},
        ],
        "edges": [("viral_load_import", "viral_load_clean")],
        "tags": ["viral_load", "pipeline", "external-script"],
        "retries": 2,
        "retry_delay_minutes": 5,
        "max_active_runs": 2,
        "max_active_tasks": 4,
        "pool": "data_import_pool",
    },

# CONFIGURE VIRAL LOAD REPORT IMPORTATION

    {
        "dag_id": "VIRAL_LOAD_REPORTS",
        "schedule": "5 6-18/3 * * *",                     # 06:05, 09:05, 12:05, 15:05, 18:05
        "start_date": datetime(2025, 8, 24, 6, 40, tzinfo=LOCAL_TZ),
        "catchup": False,
        "jobs": [
            {
                "task_id": "VIRAL_LOAD_REPORT_IMPORTATION",
                "script": "VIRAL_LOAD_PROJECT/VIRAL_LOAD_IMPORTATION_SCRIPTS_REPORTS.py",
            },

            {
                "task_id": "VIRAL_LOAD_REPORT_PROCESSING",
                "script": "VIRAL_LOAD_PROJECT/VIRAL LOAD DATA CLEANING.py"
            }
        ],
        "edges": [('VIRAL_LOAD_REPORT_IMPORTATION', 'VIRAL_LOAD_REPORT_PROCESSING')],
        "tags": ["viral_load", "reports", "external-script"],
        "retries": 2,
        "retry_delay_minutes": 5,
        "max_active_runs": 2,
        "max_active_tasks": 4,
        "pool": "data_import_pool",
    },


# CONFIGURE THE XPERT PROCESSING

    {
        "dag_id": "XPERT_PROCESSING",
        "schedule": "0 6-18 * * *",  # hourly
        "start_date": datetime(2025, 8, 24, 6, 45, tzinfo=LOCAL_TZ),
        "jobs": [{"task_id": "XPERT_DATA_PROCESSING",
                  "script": "XPERT_PROJECT/XPERT_PROCESSING.py"}],
        "edges": [],
        "tags": ["xpert", "pipeline", "external-script"],
        "retries": 2,
        "retry_delay_minutes": 5,
        "max_active_runs": 2,
        "max_active_tasks": 4,
        "pool": "data_import_pool",
    },

# CONFIGURE THE LAB PDF PIPELINE

    {
        "dag_id": "LAB_PDF_PIPELINE",
        "schedule": "*/30 6-18 * * *",
        "start_date": datetime(2025, 8, 24, 6, 50, tzinfo=LOCAL_TZ),
        "jobs": [
            {"task_id": "LAB_PDF_IMPORTATION",
             "script": "PDF_PROJECT/PDF GENERATION DATA IMPORTATION.py"},
            {"task_id": "LAB_PDF_CLEANING",
             "script": "PDF_PROJECT/PDF GENERATION PROCESSING.py"},
        ],
        "edges": [("LAB_PDF_IMPORTATION", "LAB_PDF_CLEANING")],
        "tags": ["Lab PDF generation", "pipeline", "external-script", "Importation", "Cleaning"],
        "retries": 2,
        "retry_delay_minutes": 5,
        "max_active_runs": 2,
        "max_active_tasks": 4,
        "pool": "data_import_pool",
    },

# CONFIGURE A DAG TO CLEAN UP THE WORKING SPACE
    

        {
        "dag_id": "WORKING_DIRECTORY_CLEANUP",
        "schedule": "*/15 6-18 * * *",  # every 15 minutes from 6 to 18

        "start_date": datetime(2025, 8, 24, 6, 55, tzinfo=LOCAL_TZ),
        "jobs": [{"task_id": "WORKING_DIRECTORY_CLEANUP",
                  "script": "delete_redcap_log_files.py"}],
        "edges": [],
        "tags": ["Cleanup Task",  "external-script"],
        "retries": 2,
        "retry_delay_minutes": 5,
        "max_active_runs": 2,
        "max_active_tasks": 4,
        "pool": "data_import_pool",
    },


# CONFIGURE THE IMAGE QUALITY STUDY PIPELINE
    {
        "dag_id": "IMAGE_QUALITY_STUDY_PIPELINE",
        "schedule":  "0 6-18 * * *",  # hourly "@continuous", #
        "start_date": datetime(2025, 8, 24, 7, 0, tzinfo=LOCAL_TZ),
        "jobs": [{"task_id": "IMAGE_QUALITY_STUDY_IMPORTATION",
                  "script": "IMAGE_QUALITY_STUDY/IMAGE_QUALITY_STUDY_IMPORTATION.py"},
                  {"task_id": "IMAGE_QUALITY_STUDY_PROCESSING",
                   "script": "IMAGE_QUALITY_STUDY/IMAGE_QUALITY_STUDY_PROCESSING.py"}
                  ],
        "edges": [ ("IMAGE_QUALITY_STUDY_IMPORTATION", "IMAGE_QUALITY_STUDY_PROCESSING")],
        "tags": ["Image Quality Study", "pipeline", "external-script"],
        "retries": 2,
        "retry_delay_minutes": 5,
        "max_active_runs": 2,
        "max_active_tasks": 4,
        "pool": "data_import_pool",
        # Wait for changes in the dynamic cross-platform directory + file
        # "file_change_watch_path_key": "base_path",
        # "file_change_watch_file": "Bamenda Center for Health Promotion and Research/Data Management - IMAGE_QUALITY_STUDY/TRIGGER_PATH/dag_trigger_IQS.csv",
        # "trigger_on_change_immediately": True,
        # "file_change_poke_interval_seconds": 5,
    },

# CONFIGURE FOR THE GHIT FUJILAM II STUDY PIPELINE
    {
        "dag_id": "FUJILAM_II_STUDY_PIPELINE",
        "schedule": "0 6-18 * * *",  # hourly
        "start_date": datetime(2025, 8, 24, 7, 3, tzinfo=LOCAL_TZ),
        "jobs": [{"task_id": "GHIT_IMPORTATION",
                  "script": "GHIT_PROJECT/GHIT_DATA_IMPORTATION.py"},
                  {"task_id": "GHIT_PROCESSING",
                   "script": "GHIT_PROJECT/GHIT_DATA_PROCESSING.py"}
                  ],
        "edges": [ ("GHIT_IMPORTATION", "GHIT_PROCESSING")],
        "tags": ["GHIT", "FUJILAM II", "pipeline", "external-script"],
        "retries": 2,
        "retry_delay_minutes": 5,
        "max_active_runs": 2,
        "max_active_tasks": 4,
        "pool": "data_import_pool",
        # Wait for changes in the dynamic cross-platform directory + file
        # "file_change_watch_path_key": "base_path",
        # "file_change_watch_file": "Bamenda Center for Health Promotion and Research/Data Management - GHIT_DATA/GHIT_TRIGGER_PATH/dag_trigger_FL2.csv",
        # "trigger_on_change_immediately": True,
        # "file_change_poke_interval_seconds": 5,
    },

# CONFIGURE FOR THE GHIT USER ACTIVITY
    {
        "dag_id": "FUJILAM_USER_ACTIVITY_MONITORING",
        "schedule": "*/45 6-18 * * *",  # every 45 minutes from 6 to 18
        "start_date": datetime(2025, 8, 24, 7, 7, tzinfo=LOCAL_TZ),
        "jobs": [{"task_id": "GHIT_USER_ACTIVITY",
                  "script": "GHIT_PROJECT/GHIT_USER_ACTIVITY.py"}

                  ],
        "edges": [],
        "tags": ["GHIT", "USER ACTIVITY", "FUJILAM II", "pipeline", "external-script"],
        "retries": 2,
        "retry_delay_minutes": 5,
        "max_active_runs": 2,
        "max_active_tasks": 4,
        "pool": "data_import_pool",
        # Wait for changes in the dynamic cross-platform directory + file
        # "file_change_watch_path_key": "base_path",
        # "file_change_watch_file": "Bamenda Center for Health Promotion and Research/Data Management - GHIT_DATA/GHIT_TRIGGER_PATH/dag_trigger_FL2.csv",
        # "trigger_on_change_immediately": True,
        # "file_change_poke_interval_seconds": 5,
    },



# CONFIGURE FOR THE SPECIMEN TRANSPORT STUDY PIPELINE
# to run every 30 minutes between 6am and 6pm
    {
        "dag_id": "SPECIMEN_TRANSPORT_PIPELINE",
        "schedule":  "*/15 6-18 * * *",  # every 15 minutes from 6 to 18"@continuous",  # event-driven: wait for change
        "start_date": datetime(2025, 8, 24, 7, 13, tzinfo=LOCAL_TZ),
        "jobs": [{"task_id": "SPECIMEN_TRANSPORT_IMPORTATION",
                  "script": "SPECIMEN_TRANSPORT/SPECIMEN_TRANSPORT_IMPORTATION.py"},
                  {"task_id": "SPECIMEN_TRANSPORT_PROCESSING",
                   "script": "SPECIMEN_TRANSPORT/SPECIMEN_TRANSPORT_PROCESSING.py"}
                  ],
        "edges": [ ("SPECIMEN_TRANSPORT_IMPORTATION", "SPECIMEN_TRANSPORT_PROCESSING")], # ,
        "tags": ["Specimen Transport", "pipeline", "external-script"],
        "retries": 2,
        "retry_delay_minutes": 5,
        "max_active_runs": 2,
        "max_active_tasks": 4,
        "pool": "data_import_pool",
        # Wait for changes in the dynamic cross-platform directory + file
        # "file_change_watch_path_key": "base_path",
        # "file_change_watch_file": "Bamenda Center for Health Promotion and Research/Data Management - CROSS_PROJECT_DATA/SPECIMEN_TRANSPORTATION/FLOW_TRIGGER_SPECIMEN_TRANSPORT/dag_trigger_stp.csv",
        # "trigger_on_change_immediately": True,
        # "file_change_poke_interval_seconds": 5,
    },

# COLLABORATORS PROJECT

    {
        "dag_id": "COLLABORATORS_PROJECT_PIPELINE",
        "schedule": "0 12 * * *",  # daily at 12:00
        "start_date": datetime(2025, 8, 24, 6, 5, tzinfo=LOCAL_TZ),
        "jobs": [
            {"task_id": "collaborators_importation",  "script": "COLLABORATORS_PROJECT/COLLABORATORS_DATA_IMPORTATION.py"},
            {"task_id": "collaborators_data_processor", "script": "COLLABORATORS_PROJECT/COLLABORATORS_DATA_PROCESSING.py"},
        ],
        "edges": [("collaborators_importation", "collaborators_data_processor")],
        "tags": ["collaborators_information", "pipeline"],
        "retries": 2,
        "retry_delay_minutes": 5,
        "max_active_runs": 2,
        "max_active_tasks": 4,
        "pool": "data_import_pool",
    },




]

# ============================= REGISTER DAGS ================================
for spec in DAG_SPECS:
    dag_obj = build_script_dag(**spec)
    globals()[spec["dag_id"]] = dag_obj




