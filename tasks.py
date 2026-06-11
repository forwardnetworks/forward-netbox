import json
import os
import re
import shlex
import sys
import time
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from types import SimpleNamespace

from dotenv import load_dotenv
from invoke.collection import Collection
from invoke.exceptions import CommandTimedOut
from invoke.exceptions import Exit
from invoke.tasks import task as invoke_task


INIT_FILE = "forward_netbox/__init__.py"
ALLOW_SHARED_RUNTIME_TESTS_ENV = "FORWARD_NETBOX_ALLOW_SHARED_RUNTIME_TESTS"
ACTIVE_EXECUTION_RUN_STATUSES = ("queued", "running", "waiting")
ISOLATED_TEST_PROJECT_NAME = "forward-netbox-test"
ISOLATED_PLAYWRIGHT_PROJECT_NAME = "forward-netbox-ui-test"
ISOLATED_PLAYWRIGHT_HOST_PORT = "18080"

load_dotenv(os.path.dirname(os.path.abspath(__file__)) + "/development/.env")

namespace = Collection("forward_netbox")
namespace.configure(
    {
        "forward_netbox": {
            "netbox_ver": os.environ.get("NETBOX_VER", ""),
            "project_name": "forward-netbox",
            "compose_dir": os.path.join(os.path.dirname(__file__), "development"),
        }
    }
)


def task(function=None, *args, **kwargs):
    def task_wrapper(function=None):
        if args or kwargs:
            task_func = invoke_task(*args, **kwargs)(function)
        else:
            task_func = invoke_task(function)
        namespace.add_task(task_func)
        return task_func

    if function:
        return task_wrapper(function)
    return task_wrapper


def docker_compose(context, command, **kwargs):
    build_env = {
        "NETBOX_VER": context.forward_netbox.netbox_ver,
        "ACI_PLUGIN_PACKAGE": os.environ.get("ACI_PLUGIN_PACKAGE", ""),
        "FORWARD_NETBOX_ENABLE_ACI_PLUGIN": os.environ.get(
            "FORWARD_NETBOX_ENABLE_ACI_PLUGIN",
            "",
        ),
        **kwargs.pop("env", {}),
    }
    compose_command_tokens = [
        "docker compose",
        f"--project-name {context.forward_netbox.project_name}",
        f'--project-directory "{context.forward_netbox.compose_dir}"',
        command,
    ]
    compose_command = " ".join(compose_command_tokens)
    return context.run(compose_command, env=build_env, **kwargs)


def _compose_project_context(context, project_name):
    return SimpleNamespace(
        run=context.run,
        forward_netbox=SimpleNamespace(
            netbox_ver=context.forward_netbox.netbox_ver,
            project_name=str(project_name or context.forward_netbox.project_name),
            compose_dir=context.forward_netbox.compose_dir,
        ),
    )


def manage_py(context, command, **kwargs):
    return docker_compose(
        context,
        f'exec -T netbox bash -lc "cd /opt/netbox/netbox && python manage.py {command}"',
        **kwargs,
    )


def _truthy_env(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def manage_py_one_off(context, command, **kwargs):
    bash_command = f"cd /opt/netbox/netbox && python manage.py {command}"
    return docker_compose(
        context,
        f"run --rm --no-deps netbox bash -lc {shlex.quote(bash_command)}",
        **kwargs,
    )


def _field_scale_manage_py(context, command, **kwargs):
    if _truthy_env(os.getenv("FORWARD_FIELD_SCALE_USE_SERVICE", "")):
        return manage_py(context, command, **kwargs)
    return manage_py_one_off(context, command, **kwargs)


def _truthy_arg(value):
    if isinstance(value, bool):
        return value
    return _truthy_env(value)


def _shared_runtime_test_guard_bypassed():
    return _truthy_env(os.environ.get(ALLOW_SHARED_RUNTIME_TESTS_ENV))


def _shared_runtime_active_execution_runs(context):
    statuses = json.dumps(list(ACTIVE_EXECUTION_RUN_STATUSES))
    python_code = (
        "import json; "
        "from forward_netbox.models import ForwardExecutionRun; "
        f"statuses={statuses}; "
        "qs=ForwardExecutionRun.objects.filter(status__in=statuses).order_by('-id'); "
        "print(json.dumps({"
        '"active_count": qs.count(), '
        '"runs": list(qs.values("id", "sync__name", "status")[:5])'
        "}, sort_keys=True))"
    )
    try:
        result = docker_compose(
            context,
            (
                "exec -T netbox python /opt/netbox/netbox/manage.py "
                f"shell -c {shlex.quote(python_code)}"
            ),
            hide=True,
            warn=True,
        )
    except Exception as exc:
        return {
            "active_count": 0,
            "runs": [],
            "guard_available": False,
            "reason": f"shared_runtime_probe_failed: {exc}",
        }
    stdout = getattr(result, "stdout", "")
    stderr = getattr(result, "stderr", "")
    exited = getattr(result, "exited", 0)
    if exited not in (0, None):
        detail = str(stderr or stdout or "").strip().splitlines()
        reason = detail[-1] if detail else f"exit {exited}"
        return {
            "active_count": 0,
            "runs": [],
            "guard_available": False,
            "reason": f"shared_runtime_probe_failed: {reason}",
        }
    for line in reversed(str(stdout or "").splitlines()):
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
            payload.setdefault("guard_available", True)
            return payload
        except json.JSONDecodeError:
            continue
    return {
        "active_count": 0,
        "runs": [],
        "guard_available": False,
        "reason": "shared_runtime_probe_missing_json",
    }


def _parse_json_from_manage_output(output):
    text = str(output or "")
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end < start:
        raise ValueError("No JSON payload found in manage.py output.")
    return json.loads(text[start : end + 1])


def _manage_py_json(context, command):
    result = manage_py(context, command, hide=True, warn=True)
    stdout = getattr(result, "stdout", "")
    try:
        return _parse_json_from_manage_output(stdout)
    except (ValueError, json.JSONDecodeError) as exc:
        raise Exit(
            f"Could not parse JSON output from manage.py command `{command}`: {exc}",
            code=2,
        ) from exc


def _manage_py_json_retry(context, command, *, attempts=3, delay_seconds=1):
    last_exc = None
    total_attempts = max(1, int(attempts))
    for attempt in range(1, total_attempts + 1):
        try:
            return _manage_py_json(context, command)
        except Exit as exc:
            if exc.code != 2:
                raise
            last_exc = exc
            if attempt >= total_attempts:
                raise
            time.sleep(max(0, int(delay_seconds)))
    if last_exc is not None:
        raise last_exc
    raise Exit(
        f"Could not parse JSON output from manage.py command `{command}`.",
        code=2,
    )


def _guard_shared_runtime_tests(context):
    if _shared_runtime_test_guard_bypassed():
        return
    active = _shared_runtime_active_execution_runs(context)
    if active.get("guard_available") is False:
        raise Exit(
            (
                "Could not inspect the shared local NetBox runtime for active "
                "Forward execution runs. Run tests with `invoke test-isolated`, "
                "fix the shared runtime, or set "
                f"{ALLOW_SHARED_RUNTIME_TESTS_ENV}=1 to bypass intentionally. "
                f"Reason: {active.get('reason') or 'unknown'}."
            ),
            code=2,
        )
    active_count = int(active.get("active_count") or 0)
    if active_count <= 0:
        return
    runs = active.get("runs") or []
    examples = ", ".join(
        f"run {item.get('id')} {item.get('sync__name') or ''} {item.get('status')}"
        for item in runs
    )
    raise Exit(
        (
            "Active Forward execution run(s) detected in the shared local NetBox "
            "runtime. Running Django tests against this runtime can move live RQ "
            "jobs to failed/abandoned state. Stop or finish the ingestion, run "
            "tests in an isolated stack, or set "
            f"{ALLOW_SHARED_RUNTIME_TESTS_ENV}=1 to bypass intentionally. "
            f"Detected {active_count} active run(s)"
            + (f": {examples}" if examples else ".")
        ),
        code=2,
    )


def _run_tests_in_isolated_runtime(
    context,
    *,
    test_label,
    project_name=f"{ISOLATED_TEST_PROJECT_NAME}-ci",
    keep_runtime=False,
):
    isolated = _compose_project_context(context, project_name)
    docker_compose(isolated, "down --remove-orphans -v")
    docker_compose(isolated, "build netbox netbox-worker")
    docker_compose(isolated, "up -d postgres redis")
    try:
        _wait_for_isolated_postgres(isolated)
        docker_compose(
            isolated,
            (
                "run --rm -T netbox bash -lc "
                f"{shlex.quote('cd /opt/netbox/netbox && python manage.py test --keepdb --noinput ' + str(test_label))}"
            ),
        )
    finally:
        if not _truthy_arg(keep_runtime):
            docker_compose(isolated, "down --remove-orphans -v")


def _wait_for_isolated_postgres(context, *, timeout_seconds=120):
    timeout_seconds = max(1, int(timeout_seconds))
    docker_compose(
        context,
        (
            "exec -T postgres sh -lc "
            + shlex.quote(
                (
                    f"for i in $(seq 1 {timeout_seconds}); do "
                    'pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" && exit 0; '
                    "sleep 1; "
                    "done; "
                    f'echo "Timed out waiting {timeout_seconds}s for isolated PostgreSQL readiness." >&2; '
                    "exit 1"
                )
            )
        ),
    )


def _run_tests_with_shared_runtime_fallback(context, *, test_label):
    if _shared_runtime_test_guard_bypassed():
        manage_py(context, f"test --keepdb --noinput {test_label}")
        return

    active = _shared_runtime_active_execution_runs(context)
    if active.get("guard_available") is False:
        print(
            "Shared runtime active-run guard unavailable; "
            "running Django tests in isolated runtime for CI safety. "
            f"Reason: {active.get('reason') or 'unknown'}."
        )
        _run_tests_in_isolated_runtime(
            context,
            test_label=test_label,
            project_name=f"{ISOLATED_TEST_PROJECT_NAME}-ci",
            keep_runtime=False,
        )
        return
    active_count = int(active.get("active_count") or 0)
    if active_count <= 0:
        manage_py(context, f"test --keepdb --noinput {test_label}")
        return

    print(
        "Active execution runs detected in shared runtime; "
        "running Django tests in isolated runtime for CI safety."
    )
    _run_tests_in_isolated_runtime(
        context,
        test_label=test_label,
        project_name=f"{ISOLATED_TEST_PROJECT_NAME}-ci",
        keep_runtime=False,
    )


def _run_playwright_ui(context, *, env=None):
    context.run("npm run test:ui", env={**(env or {})})


def _run_playwright_in_isolated_runtime(context, *, project_name=None, host_port=None):
    project_name = str(project_name or ISOLATED_PLAYWRIGHT_PROJECT_NAME)
    host_port = str(host_port or os.environ.get("FORWARD_NETBOX_PLAYWRIGHT_HOST_PORT"))
    if not host_port or host_port.lower() == "none":
        host_port = ISOLATED_PLAYWRIGHT_HOST_PORT
    isolated = _compose_project_context(context, project_name)
    compose_env = {"FORWARD_NETBOX_HOST_PORT": host_port}
    docker_compose(isolated, "down --remove-orphans -v", env=compose_env)
    try:
        docker_compose(
            isolated,
            "up -d --wait --wait-timeout 300 netbox",
            env=compose_env,
        )
        _run_playwright_ui(
            context,
            env={
                "NETBOX_URL": f"http://127.0.0.1:{host_port}",
                "PLAYWRIGHT_DOCKER_PROJECT_NAME": project_name,
                "PLAYWRIGHT_DOCKER_PROJECT_DIRECTORY": context.forward_netbox.compose_dir,
                "PLAYWRIGHT_ARTIFACT_DIR": f".playwright-artifacts/{project_name}",
            },
        )
    finally:
        docker_compose(isolated, "down --remove-orphans -v", env=compose_env)


def _run_playwright_with_shared_runtime_fallback(context):
    if _shared_runtime_test_guard_bypassed():
        _run_playwright_ui(context)
        return

    active = _shared_runtime_active_execution_runs(context)
    if active.get("guard_available") is False:
        print(
            "Shared runtime active-run guard unavailable; "
            "running Playwright UI tests in isolated runtime for CI safety. "
            f"Reason: {active.get('reason') or 'unknown'}."
        )
        _run_playwright_in_isolated_runtime(context)
        return

    active_count = int(active.get("active_count") or 0)
    if active_count <= 0:
        _run_playwright_ui(context)
        return

    print(
        "Active execution runs detected in shared runtime; "
        "running Playwright UI tests in isolated runtime for CI safety."
    )
    _run_playwright_in_isolated_runtime(context)


def _host_memory_gib():
    try:
        pages = int(os.sysconf("SC_PHYS_PAGES"))
        page_size = int(os.sysconf("SC_PAGE_SIZE"))
        total_bytes = pages * page_size
        gib = int(total_bytes / (1024**3))
        return max(gib, 4)
    except (AttributeError, OSError, ValueError):
        return 8


def _recommended_worker_replicas():
    cpu_count = int(os.cpu_count() or 4)
    return max(2, min(cpu_count, 32))


def _current_worker_replicas(context):
    result = context.run(
        (
            "docker compose "
            f"--project-name {context.forward_netbox.project_name} "
            f'--project-directory "{context.forward_netbox.compose_dir}" '
            "ps -q netbox-worker | wc -l"
        ),
        hide=True,
        warn=True,
    )
    raw_stdout = getattr(result, "stdout", "")
    output = (
        raw_stdout.strip() if isinstance(raw_stdout, str) else str(raw_stdout or "")
    )
    matches = re.findall(r"\d+", output)
    return int(matches[-1]) if matches else 0


def _ensure_worker_replicas(context, replicas):
    replicas = int(replicas or 0)
    if replicas <= 0:
        docker_compose(context, "up -d netbox-worker")
        return _current_worker_replicas(context)
    docker_compose(
        context,
        f"up -d --scale netbox-worker={replicas} netbox netbox-worker",
    )
    return replicas


def _recommended_postgres_settings():
    memory_gib = _host_memory_gib()
    shared_buffers_gib = max(2, min(memory_gib // 4, 16))
    effective_cache_size_gib = max(4, min((memory_gib * 3) // 4, 96))
    maintenance_work_mem_mb = max(512, min(memory_gib * 16, 4096))
    return {
        "shared_buffers": f"{shared_buffers_gib}GB",
        "effective_cache_size": f"{effective_cache_size_gib}GB",
        "work_mem": "32MB",
        "maintenance_work_mem": f"{maintenance_work_mem_mb}MB",
        "checkpoint_timeout": "15min",
        "max_wal_size": "16GB",
        "random_page_cost": "1.1",
        "max_worker_processes": "16",
        "max_parallel_workers": "16",
        "max_parallel_workers_per_gather": "4",
    }


@task
def build(context):
    docker_compose(context, "build")


@task
def start(context):
    docker_compose(context, "up -d")


@task
def stop(context):
    docker_compose(context, "down --remove-orphans")


@task
def makemigrations(context):
    docker_compose(
        context,
        "exec netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py makemigrations forward_netbox",
    )


@task
def lint(context):
    context.run(f"{shlex.quote(sys.executable)} -m pre_commit run --all-files")


@task(name="sensitive-check")
def sensitive_check(context):
    context.run(f"{shlex.quote(sys.executable)} scripts/check_sensitive_content.py")
    context.run(
        f"{shlex.quote(sys.executable)} scripts/check_sensitive_content.py --all-history"
    )


@task(name="harness-check")
def harness_check(context):
    context.run(f"{shlex.quote(sys.executable)} scripts/check_harness.py")


@task(name="harness-test")
def harness_test(context):
    context.run(
        f"{shlex.quote(sys.executable)} -m unittest discover -s scripts/tests -p 'test_*.py'"
    )


@task
def check(context):
    manage_py(context, "check")


@task
def test(context):
    _guard_shared_runtime_tests(context)
    manage_py(context, "test --keepdb --noinput forward_netbox.tests")


@task(name="test-isolated")
def test_isolated(
    context,
    test_label="forward_netbox.tests",
    project_name=ISOLATED_TEST_PROJECT_NAME,
    keep_runtime=True,
):
    """Run Django tests in a separate Docker compose project."""
    _run_tests_in_isolated_runtime(
        context,
        test_label=test_label,
        project_name=project_name,
        keep_runtime=keep_runtime,
    )


@task(name="scenario-test")
def scenario_test(context):
    _guard_shared_runtime_tests(context)
    manage_py(
        context,
        "test --keepdb --noinput forward_netbox.tests.test_synthetic_scenarios",
    )


@task(name="ingestion-delete-regression")
def ingestion_delete_regression(context):
    _guard_shared_runtime_tests(context)
    manage_py(
        context,
        (
            "test --keepdb --noinput "
            "forward_netbox.tests.test_synthetic_scenarios."
            "SyntheticSyncScenarioHarnessTest."
            "test_full_site_ingestion_then_diff_delete "
            "forward_netbox.tests.test_sync."
            "ForwardBranchBudgetPlanTest."
            "test_branch_plan_runs_prune_deletes_in_dependency_order "
            "forward_netbox.tests.test_sync."
            "ForwardBranchBudgetPlanTest."
            "test_branch_plan_splits_mixed_workloads_into_apply_then_delete_phases"
        ),
    )


@task(name="test-ci")
def test_ci(context):
    _run_tests_with_shared_runtime_fallback(
        context,
        test_label="forward_netbox.tests",
    )


@task(name="scenario-test-ci")
def scenario_test_ci(context):
    _run_tests_with_shared_runtime_fallback(
        context,
        test_label="forward_netbox.tests.test_synthetic_scenarios",
    )


@task(name="ingestion-delete-regression-ci")
def ingestion_delete_regression_ci(context):
    _run_tests_with_shared_runtime_fallback(
        context,
        test_label=(
            "forward_netbox.tests.test_synthetic_scenarios."
            "SyntheticSyncScenarioHarnessTest."
            "test_full_site_ingestion_then_diff_delete "
            "forward_netbox.tests.test_sync."
            "ForwardBranchBudgetPlanTest."
            "test_branch_plan_runs_prune_deletes_in_dependency_order "
            "forward_netbox.tests.test_sync."
            "ForwardBranchBudgetPlanTest."
            "test_branch_plan_splits_mixed_workloads_into_apply_then_delete_phases"
        ),
    )


@task(name="optimize-runtime")
def optimize_runtime(
    context,
    worker_replicas=0,
    query_fetch_concurrency=16,
    nqe_page_size=10000,
    source_name="",
    apply_postgres=True,
):
    replicas = (
        int(worker_replicas)
        if int(worker_replicas) > 0
        else _recommended_worker_replicas()
    )
    qfc = max(1, min(int(query_fetch_concurrency), 16))
    page_size = max(1, min(int(nqe_page_size), 10000))

    docker_compose(context, "up -d postgres redis netbox")
    if apply_postgres:
        postgres_settings = _recommended_postgres_settings()
        for setting, value in postgres_settings.items():
            docker_compose(
                context,
                (
                    "exec -T postgres psql -U netbox -d netbox -v ON_ERROR_STOP=1 "
                    f"-c \"ALTER SYSTEM SET {setting} = '{value}';\""
                ),
            )
        docker_compose(context, "restart postgres")

    current_workers = _current_worker_replicas(context)
    if current_workers != replicas:
        _ensure_worker_replicas(context, replicas)

    if source_name:
        _apply_source_fetch_tuning(
            context,
            source_name=source_name,
            query_fetch_concurrency=qfc,
            nqe_page_size=page_size,
        )

    print(
        "Optimized local runtime: "
        f"workers={replicas}, query_fetch_concurrency={qfc}, nqe_page_size={page_size}, "
        f"postgres_tuned={'yes' if apply_postgres else 'no'}."
    )


def _apply_source_fetch_tuning(
    context,
    *,
    source_name,
    query_fetch_concurrency,
    nqe_page_size,
):
    source_name = str(source_name or "").strip()
    if not source_name:
        return False
    qfc = max(1, min(int(query_fetch_concurrency), 16))
    page_size = max(1, min(int(nqe_page_size), 10000))
    source_name_literal = json.dumps(source_name)
    python_code = (
        "from forward_netbox.models import ForwardSource; "
        f"s=ForwardSource.objects.get(name={source_name_literal}); "
        "p=dict(s.parameters or {}); "
        f"p['query_fetch_concurrency']={qfc}; "
        f"p['nqe_page_size']={page_size}; "
        "p['timeout']=int(p.get('timeout') or 1200); "
        "s.parameters=p; s.save(update_fields=['parameters']); "
        "print('updated')"
    )
    docker_compose(
        context,
        (
            "exec -T netbox python /opt/netbox/netbox/manage.py "
            f"shell -c {shlex.quote(python_code)}"
        ),
    )
    return True


@task(name="runtime-capacity-review")
def runtime_capacity_review(
    context,
    output_json="docs/03_Plans/evidence/runtime-capacity-review.json",
    source_name="",
):
    """Write a read-only local runtime capacity review artifact."""
    repo_root = Path(__file__).resolve().parent
    report = _runtime_capacity_review(context, source_name=source_name)
    if output_json:
        output_path = repo_root / output_json
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        output_path.chmod(0o666)
        print(f"Wrote runtime capacity review: {output_path}")
    else:
        print(json.dumps(report, indent=2, sort_keys=True))


def _runtime_capacity_review(context, *, source_name=""):
    recommended_workers = _recommended_worker_replicas()
    current_workers = _current_worker_replicas(context)
    source_parameters = _runtime_capacity_source_parameters(context, source_name)
    worker_status = "pass" if current_workers >= min(recommended_workers, 4) else "warn"
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "host": {
            "cpu_count": int(os.cpu_count() or 0),
            "memory_gib": _host_memory_gib(),
        },
        "workers": {
            "current": current_workers,
            "recommended": recommended_workers,
            "status": worker_status,
            "message": (
                "Worker count is sufficient for capacity review."
                if worker_status == "pass"
                else "Worker count is below the recommended floor for large-run capacity review."
            ),
        },
        "postgres": {
            "recommended_settings": _recommended_postgres_settings(),
        },
        "storage": _runtime_capacity_storage(context),
        "source": source_parameters,
        "scheduler_overlap_capacity_review": {
            "status": worker_status,
            "message": (
                "Capacity review is present; scheduler overlap still requires a completed large-run benchmark."
                if worker_status == "pass"
                else "Review worker/database capacity before enabling scheduler overlap."
            ),
            "required_before_scheduler_overlap": [
                "Run a completed field-scale benchmark.",
                "Keep branch budget and dependency order enforced by the execution ledger.",
                "Confirm worker and database headroom under load.",
            ],
        },
    }


def _runtime_capacity_storage(context):
    docker_root = _docker_root_dir(context)
    postgres_mount = _postgres_data_mount(context)
    fetch_artifact_dir = os.environ.get("FORWARD_NETBOX_FETCH_ARTIFACT_DIR", "")
    storage = {
        "docker_root_dir": docker_root,
        "postgres_data_source": postgres_mount.get("source", ""),
        "postgres_data_type": postgres_mount.get("type", ""),
        "postgres_data_destination": postgres_mount.get("destination", ""),
        "fetch_artifact_dir": fetch_artifact_dir,
    }
    fast_local_paths = tuple(
        path
        for path in (
            "/var/lib/container-storage",
            "/mnt/fwd-vmstore",
        )
        if path
    )
    postgres_source = str(storage["postgres_data_source"] or "")
    if postgres_source and postgres_source.startswith(fast_local_paths):
        storage["status"] = "pass"
        storage["message"] = "Postgres data is on configured local container storage."
    elif postgres_source:
        storage["status"] = "warn"
        storage["message"] = (
            "Postgres data is not on a known high-throughput local container "
            "storage path; consider using FORWARD_NETBOX_POSTGRES_DATA_PATH for "
            "large local ingestion tests."
        )
    else:
        storage["status"] = "unknown"
        storage["message"] = "Postgres data mount could not be inspected."
    return storage


def _docker_root_dir(context):
    result = context.run(
        "docker info --format '{{json .DockerRootDir}}'",
        hide=True,
        warn=True,
    )
    stdout = str(getattr(result, "stdout", "") or "").strip()
    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        return stdout.strip('"')


def _postgres_data_mount(context):
    container_result = docker_compose(
        context,
        "ps -q postgres",
        hide=True,
        warn=True,
    )
    container_id = str(getattr(container_result, "stdout", "") or "").strip()
    if not container_id:
        return {}
    result = context.run(
        f"docker inspect {shlex.quote(container_id)} --format '{{{{json .Mounts}}}}'",
        hide=True,
        warn=True,
    )
    stdout = str(getattr(result, "stdout", "") or "").strip()
    try:
        mounts = json.loads(stdout)
    except json.JSONDecodeError:
        return {}
    if not isinstance(mounts, list):
        return {}
    for mount in mounts or []:
        if mount.get("Destination") != "/var/lib/postgresql/data":
            continue
        return {
            "type": mount.get("Type", ""),
            "source": mount.get("Source", ""),
            "destination": mount.get("Destination", ""),
        }
    return {}


def _runtime_capacity_source_parameters(context, source_name):
    source_name = str(source_name or "").strip()
    if not source_name:
        return {"available": False, "reason": "source_name_not_provided"}
    source_name_literal = json.dumps(source_name)
    python_code = (
        "import json; "
        "from forward_netbox.models import ForwardSource; "
        f"s=ForwardSource.objects.filter(name={source_name_literal}).first(); "
        'p=dict(getattr(s, "parameters", {}) or {}) if s else {}; '
        "print(json.dumps({"
        '"available": bool(s), '
        f'"source_name": {source_name_literal}, '
        '"query_fetch_concurrency": p.get("query_fetch_concurrency"), '
        '"nqe_page_size": p.get("nqe_page_size"), '
        '"timeout": p.get("timeout")'
        "}, sort_keys=True))"
    )
    result = docker_compose(
        context,
        (
            "exec -T netbox python /opt/netbox/netbox/manage.py "
            f"shell -c {shlex.quote(python_code)}"
        ),
        hide=True,
        warn=True,
    )
    stdout = getattr(result, "stdout", "")
    for line in reversed(str(stdout or "").splitlines()):
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    return {
        "available": False,
        "source_name": source_name,
        "reason": "source_parameters_unavailable",
    }


@task(name="scale-chaos-test")
def scale_chaos_test(context):
    _run_tests_with_shared_runtime_fallback(
        context,
        test_label=(
            "forward_netbox.tests.test_jobs "
            "forward_netbox.tests.test_api_views.ForwardExecutionRunAPIViewTest "
            "forward_netbox.tests.test_log_export "
            "forward_netbox.tests.test_synthetic_scenarios "
            "forward_netbox.tests.test_sync_state "
            "forward_netbox.tests.test_sync.ForwardMultiBranchExecutorAdaptiveSplitTest"
        ),
    )


@task(name="docker-chaos-kill")
def docker_chaos_kill(context, scenario="stage-after-branch", confirm=False):
    """
    Run an opt-in destructive worker kill scenario against the local Docker stack.

    This task is intentionally excluded from `invoke ci`.
    """
    if not confirm:
        raise Exit(
            "Refusing to run destructive Docker chaos task without --confirm=True",
            code=2,
        )

    allowed_scenarios = {
        "stage-before-branch",
        "stage-after-branch",
        "stage-during-apply",
        "merge-during-exec",
    }
    if scenario not in allowed_scenarios:
        raise Exit(f"Unsupported scenario `{scenario}`.", code=2)

    desired_workers = int(os.environ.get("FORWARD_CHAOS_WORKER_REPLICAS") or 0)
    if desired_workers <= 0:
        desired_workers = _current_worker_replicas(context)

    # Ensure worker containers are up before starting a destructive action.
    restored_workers = _ensure_worker_replicas(context, desired_workers)
    docker_compose(
        context,
        "ps netbox-worker",
    )

    # Kill one worker container to simulate real process interruption.
    # Scenario-aware readiness is validated before kill when a target sync is set.
    sync_name = os.environ.get("FORWARD_CHAOS_SYNC_NAME", "").strip()
    timeout_seconds = int(os.environ.get("FORWARD_CHAOS_WAIT_SECONDS", "600"))
    poll_seconds = int(os.environ.get("FORWARD_CHAOS_POLL_SECONDS", "5"))
    output_dir = os.environ.get("FORWARD_CHAOS_OUTPUT_DIR", "").strip()
    support_bundle_path = None
    support_bundle_verified = False
    if sync_name:
        _wait_for_chaos_scenario_ready(
            context,
            sync_name=sync_name,
            scenario=scenario,
            timeout_seconds=timeout_seconds,
            poll_seconds=poll_seconds,
        )

    worker_ids = (
        docker_compose(
            context,
            "ps -q netbox-worker",
            hide=True,
        )
        .stdout.strip()
        .splitlines()
    )
    if not worker_ids:
        raise Exit("No netbox-worker containers found to kill.", code=1)
    worker_id = worker_ids[0]
    context.run(f"docker kill {worker_id}")

    # Restore workers after the kill so local environment returns to steady state.
    _ensure_worker_replicas(context, restored_workers)
    docker_compose(context, "ps netbox-worker")

    # Optional support-bundle capture for run evidence after kill.
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    if sync_name and output_dir:
        _export_chaos_bundle(
            context,
            sync_name=sync_name,
            scenario=scenario,
            output_dir=output_dir,
        )
        support_bundle_path = _assert_chaos_bundle_recovery(
            output_dir=output_dir,
            scenario=scenario,
        )
        support_bundle_verified = True
    if output_dir:
        _write_chaos_kill_metadata(
            output_dir=output_dir,
            scenario=scenario,
            sync_name=sync_name,
            killed_worker_id=worker_id,
            restored_worker_replicas=restored_workers,
            support_bundle_path=support_bundle_path,
            support_bundle_recovery_verified=support_bundle_verified,
        )


@task(name="architecture-runtime-evidence")
def architecture_runtime_evidence(
    context,
    output_path="docs/03_Plans/evidence/architecture-runtime-evidence.json",
    sync_name="ui-harness-sync",
    capacity_source_name="",
    capacity_worker_replicas=0,
    capacity_query_fetch_concurrency=0,
    capacity_nqe_page_size=0,
    scale_sync_name="",
    scale_run_id="",
    scale_input_json="",
    scale_reconcile=False,
    run_field_scale=False,
    skip_chaos=False,
):
    """Collect runtime evidence artifacts for architecture completion audit."""
    repo_root = Path(__file__).resolve().parent
    evidence_path = repo_root / output_path
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    chaos_dir = repo_root / "docs/03_Plans/evidence/chaos"
    chaos_dir.mkdir(parents=True, exist_ok=True)

    if not bool(skip_chaos):
        docker_compose(context, "up -d")
    if not bool(skip_chaos) and int(capacity_worker_replicas or 0) > 0:
        _ensure_worker_replicas(context, int(capacity_worker_replicas))

    # Ensure synthetic sync exists for local non-customer chaos probes. Skip this
    # during non-disruptive refreshes so active field-scale runs are not touched.
    if not bool(skip_chaos):
        manage_py(context, "forward_seed_ui_harness")
    source_tuning_applied = False
    if (
        (capacity_source_name or "").strip()
        and int(capacity_query_fetch_concurrency or 0) > 0
        and int(capacity_nqe_page_size or 0) > 0
    ):
        source_tuning_applied = _apply_source_fetch_tuning(
            context,
            source_name=capacity_source_name,
            query_fetch_concurrency=capacity_query_fetch_concurrency,
            nqe_page_size=capacity_nqe_page_size,
        )

    if bool(skip_chaos):
        chaos_evidence = _reuse_runtime_check(
            evidence_path,
            "destructive_runtime_worker_kill_evidence_verified",
            fallback_reason=(
                "skip-chaos requested but no fresh prior destructive runtime "
                "evidence was available"
            ),
        )
    else:
        chaos_evidence = _collect_destructive_runtime_evidence(
            context=context,
            repo_root=repo_root,
            chaos_dir=chaos_dir,
            sync_name=sync_name,
            capacity_worker_replicas=capacity_worker_replicas,
        )

    field_scale_status = "not-run"
    field_scale_evidence = {
        "status": "failed",
        "evidence": (
            "Field-scale runtime matrix not executed in this local evidence run. "
            "Run with --run-field-scale=True in an environment with approved "
            "Forward smoke credentials and export artifacts to satisfy this check."
        ),
    }
    if run_field_scale:
        field_scale_evidence, field_scale_status = _run_field_scale_runtime_matrix(
            context
        )
    else:
        artifact_evidence, artifact_status = _field_scale_evidence_from_artifact()
        if artifact_evidence:
            field_scale_evidence = artifact_evidence
            field_scale_status = artifact_status
    if (scale_run_id or "").strip() or (scale_input_json or "").strip():
        scale_evidence_sync_name = ""
    else:
        scale_evidence_sync_name = (
            (scale_sync_name or "").strip()
            or (
                os.getenv("FORWARD_SMOKE_SYNC_NAME", "").strip()
                if run_field_scale
                else ""
            )
            or sync_name
        )

    capacity_review = _collect_runtime_capacity_review(
        context=context,
        repo_root=repo_root,
        source_name=capacity_source_name,
    )
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "invoke architecture-runtime-evidence",
        "checks": {
            "destructive_runtime_worker_kill_evidence_verified": chaos_evidence,
            "field_scale_runtime_matrix_verified": field_scale_evidence,
            "compatibility_cache_retirement_verified": _collect_compatibility_cache_evidence(
                context=context,
                repo_root=repo_root,
                sync_name=sync_name,
            ),
            "runtime_capacity_review_present": capacity_review,
            **_collect_scale_runtime_evidence(
                context=context,
                repo_root=repo_root,
                sync_name=scale_evidence_sync_name,
                run_id=scale_run_id,
                input_json=scale_input_json,
                reconcile=scale_reconcile,
                capacity_review=capacity_review,
            ),
        },
        "notes": {
            "field_scale_status": field_scale_status,
            "capacity_nqe_page_size": int(capacity_nqe_page_size or 0),
            "capacity_query_fetch_concurrency": int(
                capacity_query_fetch_concurrency or 0
            ),
            "capacity_source_name": (capacity_source_name or "").strip(),
            "capacity_source_tuning_applied": bool(source_tuning_applied),
            "capacity_worker_replicas": int(capacity_worker_replicas or 0),
            "scale_input_json": (scale_input_json or "").strip(),
            "scale_reconcile": bool(scale_reconcile),
            "scale_run_id": (scale_run_id or "").strip(),
            "scale_sync_name": scale_evidence_sync_name,
            "skip_chaos": bool(skip_chaos),
            "sync_name": sync_name,
        },
    }
    evidence_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(f"Wrote runtime evidence: {evidence_path}")


def _collect_destructive_runtime_evidence(
    *, context, repo_root, chaos_dir, sync_name, capacity_worker_replicas=0
):
    chaos_scenarios = [
        "stage-before-branch",
        "stage-after-branch",
        "stage-during-apply",
        "merge-during-exec",
    ]
    chaos_results = []
    for scenario in chaos_scenarios:
        scenario_started_at = time.time()
        manage_py(
            context,
            (
                f'forward_chaos_probe --sync-name "{sync_name}" '
                f'--scenario "{scenario}" --prepare-fixture'
            ),
            hide=True,
        )
        env = {
            "FORWARD_CHAOS_OUTPUT_DIR": str(chaos_dir),
            # Scenario state is prepared by forward_chaos_probe above; this kill
            # only needs to exercise Docker worker loss and restoration.
            "FORWARD_CHAOS_SYNC_NAME": "",
            "FORWARD_CHAOS_WORKER_REPLICAS": str(int(capacity_worker_replicas or 0)),
        }
        result = context.run(
            f"invoke docker-chaos-kill --scenario={scenario} --confirm",
            env=env,
            warn=True,
            hide=True,
        )
        manage_py(
            context,
            (
                f'forward_chaos_probe --sync-name "{sync_name}" '
                f'--scenario "{scenario}" '
                f'--export-dir "{_container_export_dir(chaos_dir, repo_root=repo_root)}"'
            ),
            warn=True,
            hide=True,
        )
        bundle = None
        metadata = None
        recovery_verified = False
        recovery_error = ""
        candidates = [
            path
            for path in sorted(chaos_dir.glob(f"chaos-{scenario}-run-*.json"))
            if path.stat().st_mtime >= scenario_started_at - 1
        ]
        if candidates:
            bundle_path = candidates[-1]
            bundle = str(bundle_path.relative_to(repo_root))
            try:
                verified_bundle_path = _assert_chaos_bundle_recovery(
                    output_dir=chaos_dir,
                    scenario=scenario,
                )
                recovery_verified = True
            except Exit as exc:
                verified_bundle_path = bundle_path
                recovery_error = str(exc)
            kill_metadata = _latest_chaos_kill_metadata(
                output_dir=chaos_dir,
                scenario=scenario,
            )
            metadata_path = _write_chaos_kill_metadata(
                output_dir=chaos_dir,
                scenario=scenario,
                sync_name=sync_name,
                killed_worker_id=str(kill_metadata.get("killed_worker_id") or ""),
                restored_worker_replicas=int(
                    kill_metadata.get("restored_worker_replicas") or 0
                ),
                support_bundle_path=verified_bundle_path,
                support_bundle_recovery_verified=recovery_verified,
            )
            metadata = str(metadata_path.relative_to(repo_root))
        chaos_results.append(
            {
                "scenario": scenario,
                "exit_code": result.exited,
                "ok": bool(result.ok and recovery_verified),
                "bundle": bundle,
                "metadata": metadata,
                "support_bundle_recovery_verified": recovery_verified,
                "recovery_validation_error": recovery_error,
            }
        )

    chaos_passed = all(item["ok"] for item in chaos_results)
    return {
        "status": "passed" if chaos_passed else "failed",
        "evidence": {
            "scenarios": chaos_results,
            "output_dir": str(chaos_dir.relative_to(repo_root)),
        },
    }


def _reuse_runtime_check(evidence_path, check_name, *, fallback_reason):
    existing = _read_existing_runtime_evidence(evidence_path)
    check = ((existing or {}).get("checks") or {}).get(check_name) or {}
    if (
        _runtime_evidence_fresh(existing)
        and check.get("status") == "passed"
        and check.get("evidence") is not None
    ):
        evidence = dict(check.get("evidence") or {})
        evidence["reused_from_generated_at"] = existing.get("generated_at")
        return {
            "status": "passed",
            "evidence": evidence,
        }
    return {
        "status": "failed",
        "evidence": {
            "reason": fallback_reason,
            "existing_generated_at": (existing or {}).get("generated_at"),
        },
    }


def _read_existing_runtime_evidence(evidence_path):
    try:
        return json.loads(Path(evidence_path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _runtime_evidence_fresh(payload, *, max_age_days=7):
    generated_at = (payload or {}).get("generated_at")
    if not generated_at:
        return False
    try:
        parsed = datetime.fromisoformat(str(generated_at).replace("Z", "+00:00"))
    except ValueError:
        return False
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - parsed <= timedelta(days=max_age_days)


def _collect_runtime_capacity_review(*, context, repo_root, source_name=""):
    report_rel = "docs/03_Plans/evidence/runtime-capacity-review.json"
    report_path = repo_root / report_rel
    try:
        report = _runtime_capacity_review(context, source_name=source_name)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        report_path.chmod(0o666)
    except Exception as exc:
        return {
            "status": "failed",
            "evidence": {
                "path": report_rel,
                "reason": f"runtime capacity review failed: {exc}",
            },
        }

    status = (
        "passed"
        if (report.get("scheduler_overlap_capacity_review") or {}).get("status")
        == "pass"
        else "failed"
    )
    return {
        "status": status,
        "evidence": {
            "path": report_rel,
            "workers": report.get("workers") or {},
            "host": report.get("host") or {},
            "source": report.get("source") or {},
            "scheduler_overlap_capacity_review": report.get(
                "scheduler_overlap_capacity_review"
            )
            or {},
        },
    }


def _collect_compatibility_cache_evidence(*, context, repo_root, sync_name):
    report_rel = "docs/03_Plans/evidence/compat-cache-prune-runtime.json"
    report_path = repo_root / report_rel
    report_path.parent.mkdir(parents=True, exist_ok=True)

    flags = ["--dry-run", f'--output-json "{report_rel}"']
    if sync_name:
        flags.append(f'--sync-name "{sync_name}"')
    manage_py(
        context,
        f"forward_prune_compatibility_cache {' '.join(flags)}",
        warn=True,
        hide=True,
    )
    if not report_path.exists():
        return {
            "status": "failed",
            "evidence": {
                "path": report_rel,
                "reason": "compatibility cache report was not generated",
            },
        }
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {
            "status": "failed",
            "evidence": {
                "path": report_rel,
                "reason": "compatibility cache report is not valid JSON",
            },
        }

    stale_payload_syncs = int(report.get("stale_payload_syncs") or 0)
    status = "passed" if stale_payload_syncs == 0 else "failed"
    return {
        "status": status,
        "evidence": {
            "path": report_rel,
            "inspected_syncs": int(report.get("inspected_syncs") or 0),
            "stale_payload_syncs": stale_payload_syncs,
            "pruned_syncs": int(report.get("pruned_syncs") or 0),
            "sync_name_filter": str(report.get("sync_name_filter") or ""),
        },
    }


def _collect_scale_runtime_evidence(
    *,
    context,
    repo_root,
    sync_name="",
    run_id="",
    input_json="",
    reconcile=False,
    capacity_review=None,
):
    report_rel = "docs/03_Plans/evidence/scale-runtime-evidence.json"
    report_path = repo_root / report_rel
    report_path.parent.mkdir(parents=True, exist_ok=True)
    min_runtime_steps = int(os.environ.get("FORWARD_ARCH_RUNTIME_MIN_STEPS", "4"))
    selector_values = [
        value for value in (sync_name, run_id, input_json) if str(value).strip()
    ]

    if len(selector_values) != 1:
        evidence = {
            "path": report_rel,
            "reason": (
                "exactly one scale evidence selector is required "
                "(scale_sync_name, scale_run_id, or scale_input_json)"
            ),
        }
        return {
            "runtime_fallback_reduction_verified": {
                "status": "failed",
                "evidence": evidence,
            },
            "scheduler_overlap_readiness_verified": {
                "status": "failed",
                "evidence": evidence,
            },
        }

    flags = []
    if sync_name:
        flags.append(f'--sync-name "{sync_name}"')
    if run_id:
        flags.append(f"--run-id {int(run_id)}")
    if input_json:
        flags.append(f'--input-json "{input_json}"')
    if reconcile:
        flags.append("--reconcile")
    flags.append(f'--output-json "{report_rel}"')
    manage_py(
        context,
        f"forward_scale_benchmark {' '.join(flags)}",
        warn=True,
        hide=True,
    )
    if not report_path.exists():
        evidence = {
            "path": report_rel,
            "reason": "scale benchmark report was not generated",
        }
        return {
            "runtime_fallback_reduction_verified": {
                "status": "failed",
                "evidence": evidence,
            },
            "scheduler_overlap_readiness_verified": {
                "status": "failed",
                "evidence": evidence,
            },
        }
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        evidence = {
            "path": report_rel,
            "reason": "scale benchmark report is not valid JSON",
        }
        return {
            "runtime_fallback_reduction_verified": {
                "status": "failed",
                "evidence": evidence,
            },
            "scheduler_overlap_readiness_verified": {
                "status": "failed",
                "evidence": evidence,
            },
        }

    checks = {item.get("code"): item for item in report.get("checks") or []}
    summary = report.get("summary") or {}
    step_count = int(summary.get("step_count") or 0)
    enough_runtime_steps = step_count >= min_runtime_steps
    core_checks = [
        checks.get("support_bundle_shape") or {},
        checks.get("run_completion") or {},
        checks.get("row_failures") or {},
    ]
    core_checks_ok = all(
        (item.get("status") or "fail") in {"pass", "info"} for item in core_checks
    )
    fallback_checks = [
        checks.get("pushdown_efficiency") or {},
        checks.get("pushdown_runtime") or {},
        checks.get("partition_retry_pressure") or {},
    ]
    scheduler_check = checks.get("throughput_smoothing") or {}
    fallback_status = (
        "passed"
        if enough_runtime_steps
        and core_checks_ok
        and all(
            (item.get("status") or "fail") in {"pass", "info"}
            for item in fallback_checks
        )
        else "failed"
    )
    readiness = (scheduler_check.get("evidence") or {}).get(
        "scheduler_overlap_readiness"
    ) or {}
    readiness_status = str(readiness.get("status") or "").strip()
    capacity_ok = bool((capacity_review or {}).get("status") == "passed")
    scheduler_check_status = scheduler_check.get("status") or "fail"
    scheduler_status = "failed"
    if enough_runtime_steps and core_checks_ok:
        if readiness_status == "not_warranted" and scheduler_check_status in {
            "pass",
            "info",
        }:
            scheduler_status = "passed"
        elif readiness_status == "candidate" and capacity_ok:
            scheduler_status = "passed"
        elif readiness_status == "blocked" and capacity_ok:
            blocking_reasons = set(readiness.get("blocking_reasons") or [])
            if blocking_reasons == {"capacity_evidence_missing"}:
                scheduler_status = "passed"

    return {
        "runtime_fallback_reduction_verified": {
            "status": fallback_status,
            "evidence": {
                "path": report_rel,
                "report_status": report.get("status"),
                "core_checks": core_checks,
                "fallback_checks": fallback_checks,
                "summary": summary,
                "step_count": step_count,
                "min_runtime_steps": min_runtime_steps,
                "enough_runtime_steps": enough_runtime_steps,
            },
        },
        "scheduler_overlap_readiness_verified": {
            "status": scheduler_status,
            "evidence": {
                "path": report_rel,
                "report_status": report.get("status"),
                "core_checks": core_checks,
                "throughput_smoothing": scheduler_check,
                "scheduler_overlap_readiness": readiness,
                "capacity_review": capacity_review or {},
                "step_count": step_count,
                "min_runtime_steps": min_runtime_steps,
                "enough_runtime_steps": enough_runtime_steps,
            },
        },
    }


@task(name="field-scale-runtime-matrix")
def field_scale_runtime_matrix(context, step="", resume=True, fail_on_error=True):
    """Run or resume the sanitized field-scale smoke matrix artifact."""
    evidence, status = _run_field_scale_runtime_matrix(
        context,
        step=step,
        resume=resume,
    )
    print(json.dumps(evidence, indent=2, sort_keys=True))
    if fail_on_error and evidence.get("status") != "passed":
        raise Exit(f"Field-scale runtime matrix did not pass: {status}", code=1)


@task(name="release-dataset-gate")
def release_dataset_gate(
    context,
    dataset_label="release-smoke",
    max_age_days=7,
    artifact_path="",
    allow_resumed_artifact=False,
):
    """Fail unless fresh full-matrix field-scale evidence matches the dataset label."""
    evidence = _collect_release_dataset_gate_evidence(
        dataset_label=dataset_label,
        max_age_days=max_age_days,
        artifact_path=artifact_path,
        allow_resumed_artifact=allow_resumed_artifact,
    )
    print(json.dumps(evidence, indent=2, sort_keys=True))
    if evidence.get("status") != "passed":
        raise Exit(
            "Release dataset gate failed: regenerate field-scale evidence with the "
            f"required dataset label `{dataset_label}`.",
            code=1,
        )


@task(name="release-runtime-preflight")
def release_runtime_preflight(context, dataset_label="release-smoke"):
    """Fail fast when runtime prerequisites for release evidence are missing."""
    evidence = _collect_release_runtime_preflight_evidence(
        context=context,
        dataset_label=dataset_label,
    )
    print(json.dumps(evidence, indent=2, sort_keys=True))
    if evidence.get("status") != "passed":
        raise Exit(
            "Release runtime preflight failed: resolve runtime prerequisites before "
            "running field-scale matrix evidence.",
            code=1,
        )


@task(name="release-readiness-audit")
def release_readiness_audit(
    context,
    dataset_label="release-smoke",
    max_age_days=7,
    artifact_path="",
    output_json="docs/03_Plans/evidence/release-readiness-audit.json",
    fail_on_error=True,
):
    """Aggregate 1.1 release readiness checks into one JSON report."""
    evidence = _collect_release_readiness_audit(
        context=context,
        dataset_label=dataset_label,
        max_age_days=max_age_days,
        artifact_path=artifact_path,
    )
    rendered = json.dumps(evidence, indent=2, sort_keys=True)
    print(rendered)
    if str(output_json or "").strip():
        output_path = Path(str(output_json).strip())
        if not output_path.is_absolute():
            output_path = Path(__file__).resolve().parent / output_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered + "\n", encoding="utf-8")
        print(f"Wrote release readiness audit: {output_path}")
    if bool(fail_on_error) and evidence.get("status") != "passed":
        raise Exit(
            "Release readiness audit failed: resolve failed checks before release.",
            code=1,
        )


def _collect_release_readiness_audit(
    *,
    context,
    dataset_label="release-smoke",
    max_age_days=7,
    artifact_path="",
):
    preflight = _collect_release_runtime_preflight_evidence(
        context=context,
        dataset_label=dataset_label,
    )
    dataset_gate = _collect_release_dataset_gate_evidence(
        dataset_label=dataset_label,
        max_age_days=max_age_days,
        artifact_path=artifact_path,
    )
    validation_org_gate = _collect_validation_org_query_audit_evidence(context)
    architecture_gate = _collect_architecture_completion_gate(context)
    checks = {
        "release_runtime_preflight": preflight,
        "release_dataset_gate": dataset_gate,
        "validation_org_query_audit": validation_org_gate,
        "architecture_completion_gate": architecture_gate,
    }
    failed_checks = [
        name
        for name, payload in checks.items()
        if payload.get("status") not in {"passed", "skipped"}
    ]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "passed" if not failed_checks else "failed",
        "dataset_label": str(dataset_label or "").strip(),
        "failed_checks": failed_checks,
        "checks": checks,
    }


def _collect_validation_org_query_audit_evidence(context):
    credential_env = (
        "FORWARD_VALIDATION_USERNAME",
        "FORWARD_VALIDATION_PASSWORD",
        "FORWARD_VALIDATION_NETWORK_ID",
    )
    missing_env = [name for name in credential_env if not os.getenv(name, "").strip()]
    if missing_env:
        return {
            "status": "skipped",
            "evidence": {
                "required_env": [
                    *credential_env,
                    "FORWARD_VALIDATION_SOURCE_NAME",
                    "FORWARD_VALIDATION_URL",
                ],
                "missing_env": missing_env,
                "reason": "validation org credentials are not configured",
            },
        }

    source_name = str(
        os.getenv("FORWARD_VALIDATION_SOURCE_NAME", "validation-source") or ""
    ).strip()
    url = str(os.getenv("FORWARD_VALIDATION_URL", "https://fwd.app") or "").strip()
    username = str(os.getenv("FORWARD_VALIDATION_USERNAME", "") or "").strip()
    password = str(os.getenv("FORWARD_VALIDATION_PASSWORD", "") or "").strip()
    network_id = str(os.getenv("FORWARD_VALIDATION_NETWORK_ID", "") or "").strip()
    repository = str(os.getenv("FORWARD_VALIDATION_REPOSITORY", "org") or "").strip()
    directory = str(
        os.getenv("FORWARD_VALIDATION_DIRECTORY", "/forward_netbox_validation/") or ""
    ).strip()
    command = (
        "forward_validation_org_query_audit "
        f'--source-name "{source_name}" '
        f'--url "{url}" '
        f'--username "{username}" '
        f'--password "{password}" '
        f'--network-id "{network_id}" '
        f'--repository "{repository}" '
        f'--directory "{directory}" '
        "--fail-on-gap"
    )
    result = manage_py(context, command, warn=True, hide=True)
    stdout = getattr(result, "stdout", "")
    parse_error = ""
    payload = {}
    try:
        payload = _parse_json_from_manage_output(stdout)
    except (ValueError, json.JSONDecodeError) as exc:
        parse_error = str(exc)

    evidence = {
        "command": command,
        "source_name": source_name,
        "url": url,
        "repository": repository,
        "directory": directory,
        "parse_error": parse_error,
    }
    if not bool(getattr(result, "ok", False)):
        failure_code, failure_hint = _classify_field_scale_step_failure(result)
        return {
            "status": "failed",
            "evidence": {
                **evidence,
                "exit_code": int(getattr(result, "exited", 1) or 1),
                "failure_code": str(failure_code or "command_failed"),
                "failure_hint": str(
                    failure_hint or "validation org query audit failed"
                ),
                "report": payload,
            },
        }

    report_status = str(payload.get("status") or "").strip()
    return {
        "status": "passed" if report_status == "pass" else "failed",
        "evidence": {
            **evidence,
            "report": payload,
            "reason": (
                ""
                if report_status == "pass"
                else "validation org query audit reported gaps"
            ),
        },
    }


def _collect_architecture_completion_gate(context):
    command = "forward_architecture_completion_audit"
    result = manage_py(context, command, warn=True, hide=True)
    if not bool(getattr(result, "ok", False)):
        failure_code, failure_hint = _classify_field_scale_step_failure(result)
        return {
            "status": "failed",
            "evidence": {
                "command": command,
                "exit_code": int(getattr(result, "exited", 1) or 1),
                "failure_code": str(failure_code or "command_failed"),
                "failure_hint": str(
                    failure_hint or "architecture completion audit failed"
                ),
            },
        }

    payload = None
    parse_error = ""
    try:
        payload = _parse_json_from_manage_output(getattr(result, "stdout", ""))
    except Exception as exc:  # pragma: no cover - defensive fallback
        parse_error = str(exc)
        payload = {}
    summary = payload.get("summary") if isinstance(payload, dict) else {}
    failed = int((summary or {}).get("failed") or 0)
    pending_external = int((summary or {}).get("needs_external_evidence") or 0)
    passed = failed == 0 and pending_external == 0
    reason = ""
    if not passed:
        reason = (
            "architecture completion audit has outstanding checks "
            f"(failed={failed}, needs_external_evidence={pending_external})"
        )
    return {
        "status": "passed" if passed else "failed",
        "evidence": {
            "command": command,
            "summary": summary,
            "parse_error": parse_error,
            "reason": reason,
        },
    }


def _collect_release_runtime_preflight_evidence(
    *, context, dataset_label="release-smoke"
):
    credential_env = (
        "FORWARD_SMOKE_USERNAME",
        "FORWARD_SMOKE_PASSWORD",
        "FORWARD_SMOKE_NETWORK_ID",
    )
    source_name = str(os.getenv("FORWARD_SMOKE_SOURCE_NAME", "") or "").strip()
    credential_missing = [
        name for name in credential_env if not os.getenv(name, "").strip()
    ]
    source_backed = bool(source_name)
    missing_env = []
    if credential_missing and not source_backed:
        missing_env.extend(credential_missing)
    if not os.getenv("FORWARD_SMOKE_DATASET_LABEL", "").strip():
        missing_env.append("FORWARD_SMOKE_DATASET_LABEL")
    required_label = str(dataset_label or "").strip()
    current_label = str(os.getenv("FORWARD_SMOKE_DATASET_LABEL", "") or "").strip()
    dataset_label_matches = bool(
        current_label and current_label.lower() == required_label.lower()
    )
    docker_preflight = _field_scale_runtime_preflight(context)
    docker_ok = bool(docker_preflight.get("ok"))

    reasons = []
    if missing_env:
        reasons.append("missing required env: " + ", ".join(missing_env))
    if not dataset_label_matches:
        reasons.append(
            "FORWARD_SMOKE_DATASET_LABEL does not match required dataset label"
        )
    if not docker_ok:
        reasons.append(
            "docker preflight failed: "
            + str(docker_preflight.get("failure_code") or "docker_preflight_failed")
        )

    return {
        "status": "passed" if not reasons else "failed",
        "evidence": {
            "required_dataset_label": required_label,
            "dataset_label": current_label,
            "dataset_label_matches": dataset_label_matches,
            "required_env": [
                *credential_env,
                "FORWARD_SMOKE_SOURCE_NAME",
                "FORWARD_SMOKE_DATASET_LABEL",
            ],
            "credential_env_missing": credential_missing,
            "source_name": source_name,
            "source_backed": source_backed,
            "missing_env": missing_env,
            "docker_preflight": docker_preflight,
            "reason": "; ".join(reasons) if reasons else "",
        },
    }


def _collect_release_dataset_gate_evidence(
    *,
    dataset_label="release-smoke",
    max_age_days=7,
    artifact_path="",
    allow_resumed_artifact=False,
):
    repo_root = Path(__file__).resolve().parent
    if str(artifact_path or "").strip():
        candidate = Path(str(artifact_path).strip())
        if not candidate.is_absolute():
            candidate = repo_root / candidate
        evidence_path = candidate
    else:
        evidence_path, _ = _field_scale_matrix_artifact_path()
    try:
        evidence_rel = str(evidence_path.relative_to(repo_root))
    except ValueError:
        evidence_rel = str(evidence_path)

    if not evidence_path.exists():
        return {
            "status": "failed",
            "evidence": {
                "artifact_path": evidence_rel,
                "reason": "field-scale artifact does not exist",
                "required_dataset_label": str(dataset_label or "").strip(),
            },
        }

    try:
        payload = json.loads(evidence_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {
            "status": "failed",
            "evidence": {
                "artifact_path": evidence_rel,
                "reason": f"field-scale artifact is unreadable: {exc}",
                "required_dataset_label": str(dataset_label or "").strip(),
            },
        }

    generated_at = payload.get("generated_at")
    created = _parse_iso_datetime(generated_at)
    freshness_days = max(1, int(max_age_days or 7))
    is_fresh = bool(
        created
        and datetime.now(timezone.utc) - created <= timedelta(days=freshness_days)
    )
    artifact_status = str(payload.get("status") or "").strip()
    metadata = (
        payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    )
    runs = payload.get("runs") if isinstance(payload.get("runs"), list) else []
    required_label = str(dataset_label or "").strip()
    found_label = str(metadata.get("dataset_label") or "").strip()
    resumed = _truthy_arg(metadata.get("resume"))
    allow_resumed = _truthy_arg(allow_resumed_artifact)
    required_run_names = (
        "run_a_branching_validate_only",
        "run_b_branching_plan_only",
        "run_c_fast_bootstrap_validate_only",
    )
    run_by_name = {
        str(run.get("name")): run
        for run in runs
        if isinstance(run, dict) and run.get("name")
    }
    missing_steps = [name for name in required_run_names if name not in run_by_name]
    failed_steps = [
        name
        for name in required_run_names
        if name in run_by_name
        and (
            run_by_name[name].get("ok") is not True
            or _truthy_arg(run_by_name[name].get("timed_out"))
        )
    ]
    failed_step_codes = {
        name: str((run_by_name.get(name) or {}).get("failure_code") or "").strip()
        for name in failed_steps
    }
    distinct_failed_codes = sorted(
        {code for code in failed_step_codes.values() if str(code or "").strip()}
    )

    reasons = []
    if artifact_status != "passed":
        reasons.append(f"artifact status is `{artifact_status or 'unknown'}`")
    if not is_fresh:
        reasons.append(
            f"artifact missing timestamp or older than {freshness_days} days"
        )
    if not found_label:
        reasons.append("dataset label is missing from artifact metadata")
    elif found_label.lower() != required_label.lower():
        reasons.append(
            f"dataset label `{found_label}` does not match required `{required_label}`"
        )
    if resumed and not allow_resumed:
        reasons.append("artifact was produced with resume=true")
    if missing_steps:
        reasons.append(
            "artifact is missing required matrix steps: " + ", ".join(missing_steps)
        )
    if failed_steps:
        reasons.append(
            "artifact has failing required matrix steps: " + ", ".join(failed_steps)
        )
    if distinct_failed_codes:
        reasons.append("failure codes: " + ", ".join(distinct_failed_codes))

    return {
        "status": "passed" if not reasons else "failed",
        "evidence": {
            "artifact_path": evidence_rel,
            "artifact_status": artifact_status or "unknown",
            "generated_at": generated_at,
            "fresh": is_fresh,
            "max_age_days": freshness_days,
            "required_dataset_label": required_label,
            "dataset_label": found_label,
            "allow_resumed_artifact": allow_resumed,
            "resumed": resumed,
            "required_steps": list(required_run_names),
            "missing_steps": missing_steps,
            "failed_steps": failed_steps,
            "failed_step_codes": failed_step_codes,
            "metadata": metadata,
            "reason": "; ".join(reasons) if reasons else "",
        },
    }


def _run_field_scale_runtime_matrix(context, *, step="", resume=False):
    artifact_path, artifact_rel = _field_scale_matrix_artifact_path()
    command_env = {
        "FORWARD_SMOKE_URL": os.getenv("FORWARD_SMOKE_URL", "https://fwd.app"),
        "FORWARD_SMOKE_USERNAME": os.getenv("FORWARD_SMOKE_USERNAME", ""),
        "FORWARD_SMOKE_PASSWORD": os.getenv("FORWARD_SMOKE_PASSWORD", ""),
        "FORWARD_SMOKE_NETWORK_ID": os.getenv("FORWARD_SMOKE_NETWORK_ID", ""),
        "FORWARD_SMOKE_SNAPSHOT_ID": os.getenv(
            "FORWARD_SMOKE_SNAPSHOT_ID", "latestProcessed"
        ),
        "FORWARD_SMOKE_SOURCE_NAME": os.getenv(
            "FORWARD_SMOKE_SOURCE_NAME", "smoke-source"
        ),
        "FORWARD_SMOKE_SYNC_NAME": os.getenv("FORWARD_SMOKE_SYNC_NAME", "smoke-sync"),
        "FORWARD_SMOKE_MODELS": os.getenv("FORWARD_SMOKE_MODELS", ""),
        "FORWARD_SMOKE_QUERY_LIMIT": os.getenv("FORWARD_SMOKE_QUERY_LIMIT", "10"),
        "FORWARD_SMOKE_MAX_CHANGES_PER_BRANCH": os.getenv(
            "FORWARD_SMOKE_MAX_CHANGES_PER_BRANCH", "10000"
        ),
        "FORWARD_SMOKE_DATASET_LABEL": os.getenv("FORWARD_SMOKE_DATASET_LABEL", ""),
    }

    smoke_url = shlex.quote(command_env["FORWARD_SMOKE_URL"])
    smoke_username = shlex.quote(command_env["FORWARD_SMOKE_USERNAME"])
    smoke_password = shlex.quote(command_env["FORWARD_SMOKE_PASSWORD"])
    smoke_network_id = shlex.quote(command_env["FORWARD_SMOKE_NETWORK_ID"])
    smoke_snapshot = shlex.quote(command_env["FORWARD_SMOKE_SNAPSHOT_ID"])
    smoke_source = shlex.quote(command_env["FORWARD_SMOKE_SOURCE_NAME"])
    smoke_sync = shlex.quote(command_env["FORWARD_SMOKE_SYNC_NAME"])
    smoke_models = command_env["FORWARD_SMOKE_MODELS"].strip()
    smoke_dataset_label = command_env["FORWARD_SMOKE_DATASET_LABEL"].strip()
    smoke_query_limit = max(1, int(command_env["FORWARD_SMOKE_QUERY_LIMIT"] or 10))
    smoke_max_changes_per_branch = max(
        1,
        int(command_env["FORWARD_SMOKE_MAX_CHANGES_PER_BRANCH"] or 10000),
    )
    step_timeout_seconds = max(
        1,
        int(os.getenv("FORWARD_SMOKE_STEP_TIMEOUT_SECONDS", "1200") or 1200),
    )

    common_flag_parts = [
        f"--url {smoke_url}",
        f"--snapshot-id {smoke_snapshot}",
        f"--source-name {smoke_source}",
        f"--sync-name {smoke_sync}",
    ]
    if command_env["FORWARD_SMOKE_USERNAME"].strip():
        common_flag_parts.append(f"--username {smoke_username}")
    if command_env["FORWARD_SMOKE_PASSWORD"].strip():
        common_flag_parts.append(f"--password {smoke_password}")
    if command_env["FORWARD_SMOKE_NETWORK_ID"].strip():
        common_flag_parts.append(f"--network-id {smoke_network_id}")
    common_manage_flags = " ".join(common_flag_parts)
    if smoke_models:
        common_manage_flags = (
            f"{common_manage_flags} --models {shlex.quote(smoke_models)}"
        )

    matrix = [
        {
            "name": "run_a_branching_validate_only",
            "execute": (
                f"forward_smoke_sync --validate-only --query-limit {smoke_query_limit} "
                f"{common_manage_flags}"
            ),
            "evidence_command": (
                f"forward_smoke_sync --validate-only --query-limit {smoke_query_limit}"
            ),
        },
        {
            "name": "run_b_branching_plan_only",
            "execute": (
                "forward_smoke_sync --plan-only "
                f"--max-changes-per-branch {smoke_max_changes_per_branch} "
                f"{common_manage_flags}"
            ),
            "evidence_command": (
                "forward_smoke_sync --plan-only "
                f"--max-changes-per-branch {smoke_max_changes_per_branch}"
            ),
        },
        {
            "name": "run_c_fast_bootstrap_validate_only",
            "execute": (
                "forward_smoke_sync --validate-only --query-limit 10 "
                "--execution-backend fast_bootstrap "
                f"{common_manage_flags}"
            ),
            "evidence_command": (
                "forward_smoke_sync --validate-only --query-limit 10 "
                "--execution-backend fast_bootstrap"
            ),
        },
    ]
    matrix_names = {item["name"] for item in matrix}
    selected_step = str(step or "").strip()
    if selected_step and selected_step not in matrix_names:
        _write_field_scale_matrix_artifact(
            artifact_path=artifact_path,
            status="failed",
            runs=[],
            metadata={
                "reason": "unsupported_step",
                "step": selected_step,
                "available_steps": sorted(matrix_names),
            },
        )
        return (
            {
                "status": "failed",
                "evidence": {
                    "reason": "unsupported_step",
                    "step": selected_step,
                    "available_steps": sorted(matrix_names),
                    "artifact_path": artifact_rel,
                },
            },
            "unsupported-step",
        )

    result_by_name = {}
    if resume:
        result_by_name.update(_field_scale_existing_run_results(artifact_path))
    selected_names = {selected_step} if selected_step else matrix_names
    matrix_metadata = {
        "dataset_label": smoke_dataset_label,
        "models": smoke_models or "default_required_models",
        "max_changes_per_branch": smoke_max_changes_per_branch,
        "query_limit": smoke_query_limit,
        "resume": bool(resume),
        "selected_step": selected_step,
        "step_timeout_seconds": step_timeout_seconds,
        "step_count": len(matrix),
    }
    preflight = _field_scale_runtime_preflight(context)
    if not preflight.get("ok"):
        preflight_runs = [
            {
                "name": item["name"],
                "command": item["evidence_command"],
                "ok": False,
                "exit_code": preflight.get("exit_code"),
                "elapsed_ms": 0,
                "timed_out": False,
                "timeout_seconds": step_timeout_seconds,
                "failure_code": str(preflight.get("failure_code") or "command_failed"),
                "failure_hint": str(
                    preflight.get("failure_hint") or "runtime preflight failed"
                ),
            }
            for item in matrix
            if item["name"] in selected_names
        ]
        _write_field_scale_matrix_artifact(
            artifact_path=artifact_path,
            status="failed",
            runs=preflight_runs,
            metadata={
                **matrix_metadata,
                "preflight_failure_code": preflight.get("failure_code"),
                "preflight_failure_hint": preflight.get("failure_hint"),
            },
        )
        return (
            {
                "status": "failed",
                "evidence": {
                    "runs": preflight_runs,
                    "artifact_path": artifact_rel,
                    "note": (
                        "Output is intentionally redacted to avoid storing customer "
                        "identifiers. Use job logs and support bundles for deep triage."
                    ),
                    "preflight_failure_code": preflight.get("failure_code"),
                    "preflight_failure_hint": preflight.get("failure_hint"),
                },
            },
            "failed",
        )
    _write_field_scale_matrix_artifact(
        artifact_path=artifact_path,
        status="running",
        runs=_ordered_field_scale_runs(matrix, result_by_name),
        metadata=matrix_metadata,
    )
    for item in matrix:
        if item["name"] not in selected_names:
            continue
        previous = result_by_name.get(item["name"]) or {}
        if resume and previous.get("ok") is True and not previous.get("timed_out"):
            continue
        started = time.time()
        try:
            result = _field_scale_manage_py(
                context,
                item["execute"],
                env=command_env,
                warn=True,
                hide=True,
                timeout=step_timeout_seconds,
            )
        except CommandTimedOut:
            elapsed_ms = int((time.time() - started) * 1000)
            result_by_name[item["name"]] = {
                "name": item["name"],
                "command": item["evidence_command"],
                "ok": False,
                "exit_code": None,
                "elapsed_ms": elapsed_ms,
                "timed_out": True,
                "timeout_seconds": step_timeout_seconds,
                "failure_code": "step_timeout",
                "failure_hint": "step exceeded timeout",
            }
            _write_field_scale_matrix_artifact(
                artifact_path=artifact_path,
                status="running",
                runs=_ordered_field_scale_runs(matrix, result_by_name),
                metadata=matrix_metadata,
            )
            continue
        elapsed_ms = int((time.time() - started) * 1000)
        failure_code, failure_hint = _classify_field_scale_step_failure(result)
        result_by_name[item["name"]] = {
            "name": item["name"],
            "command": item["evidence_command"],
            "ok": bool(result.ok),
            "exit_code": int(result.exited),
            "elapsed_ms": elapsed_ms,
            "timed_out": False,
            "timeout_seconds": step_timeout_seconds,
            "failure_code": failure_code,
            "failure_hint": failure_hint,
        }
        _write_field_scale_matrix_artifact(
            artifact_path=artifact_path,
            status="running",
            runs=_ordered_field_scale_runs(matrix, result_by_name),
            metadata=matrix_metadata,
        )

    run_results = _ordered_field_scale_runs(matrix, result_by_name)
    completed_names = {run.get("name") for run in run_results}
    all_matrix_steps_passed = all(
        (
            item["name"] in completed_names
            and (result_by_name.get(item["name"]) or {}).get("ok") is True
            and not (result_by_name.get(item["name"]) or {}).get("timed_out")
        )
        for item in matrix
    )
    selected_failed = any(
        (result_by_name.get(name) or {}).get("ok") is False
        or (result_by_name.get(name) or {}).get("timed_out") is True
        for name in selected_names
        if name in result_by_name
    )
    if all_matrix_steps_passed:
        final_status = "passed"
    elif selected_failed:
        final_status = "failed"
    else:
        final_status = "partial"
    _write_field_scale_matrix_artifact(
        artifact_path=artifact_path,
        status=final_status,
        runs=run_results,
        metadata=matrix_metadata,
    )
    return (
        {
            "status": final_status,
            "evidence": {
                "runs": run_results,
                "artifact_path": artifact_rel,
                "note": (
                    "Output is intentionally redacted to avoid storing customer "
                    "identifiers. Use job logs and support bundles for deep triage."
                ),
            },
        },
        "completed" if final_status == "passed" else final_status,
    )


def _field_scale_existing_run_results(artifact_path):
    try:
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    runs = payload.get("runs") if isinstance(payload.get("runs"), list) else []
    return {
        str(run.get("name")): dict(run)
        for run in runs
        if isinstance(run, dict) and run.get("name")
    }


def _ordered_field_scale_runs(matrix, result_by_name):
    return [
        result_by_name[item["name"]]
        for item in matrix
        if item["name"] in result_by_name
    ]


def _field_scale_runtime_preflight(context):
    try:
        result = docker_compose(
            context,
            "ps --status running --services",
            warn=True,
            hide=True,
        )
    except Exception as exc:  # pragma: no cover - defensive fallback
        return {
            "ok": False,
            "exit_code": None,
            "failure_code": "docker_preflight_exception",
            "failure_hint": f"docker preflight raised {exc.__class__.__name__}",
        }
    failure_code, failure_hint = _classify_field_scale_step_failure(result)
    running_services = {
        line.strip()
        for line in str(getattr(result, "stdout", "") or "").splitlines()
        if line.strip()
    }
    missing_services = sorted({"postgres", "redis"} - running_services)
    if bool(getattr(result, "ok", False)) and not missing_services:
        return {"ok": True}
    if missing_services:
        failure_code = "docker_runtime_not_ready"
        failure_hint = "required service(s) not running: " + ", ".join(missing_services)
    return {
        "ok": False,
        "exit_code": int(getattr(result, "exited", 1) or 1),
        "failure_code": str(failure_code or "docker_preflight_failed"),
        "failure_hint": str(failure_hint or "docker preflight failed"),
    }


def _classify_field_scale_step_failure(result):
    if bool(getattr(result, "ok", False)):
        return "", ""
    text_parts = [
        str(getattr(result, "stderr", "") or ""),
        str(getattr(result, "stdout", "") or ""),
    ]
    message = "\n".join(text_parts).lower()
    if "permission denied while trying to connect to the docker api" in message:
        return (
            "docker_api_unreachable",
            "cannot connect to local Docker API",
        )
    if "docker.sock" in message and "permission denied" in message:
        return (
            "docker_socket_permission_denied",
            "local Docker socket permission denied",
        )
    if "python: command not found" in message:
        return (
            "python_not_found",
            "python executable missing in runtime shell",
        )
    return ("command_failed", "step command exited non-zero")


def _field_scale_matrix_artifact_path():
    repo_root = Path(__file__).resolve().parent
    configured = os.getenv(
        "FORWARD_FIELD_SCALE_EVIDENCE_PATH",
        "docs/03_Plans/evidence/field-scale-runtime-matrix.json",
    ).strip()
    path = Path(configured)
    if not path.is_absolute():
        path = repo_root / path
    try:
        rel = str(path.relative_to(repo_root))
    except ValueError:
        rel = str(path)
    return path, rel


def _field_scale_evidence_from_artifact():
    artifact_path, artifact_rel = _field_scale_matrix_artifact_path()
    if not artifact_path.exists():
        return None, "not-run"
    try:
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return (
            {
                "status": "failed",
                "evidence": {
                    "artifact_path": artifact_rel,
                    "reason": f"field scale artifact is unreadable: {exc}",
                },
            },
            "artifact-unreadable",
        )

    generated_at = payload.get("generated_at")
    created = _parse_iso_datetime(generated_at)
    is_fresh = bool(
        created and datetime.now(timezone.utc) - created <= timedelta(days=7)
    )
    runs = payload.get("runs") if isinstance(payload.get("runs"), list) else []
    artifact_status = str(payload.get("status") or "").strip()
    status = "passed" if artifact_status == "passed" and is_fresh else "failed"
    reason = None
    if not is_fresh:
        reason = "field scale artifact missing timestamp or older than 7 days"
    elif artifact_status != "passed":
        reason = f"field scale artifact status is `{artifact_status or 'unknown'}`"
    evidence = {
        "artifact_path": artifact_rel,
        "artifact_status": artifact_status or "unknown",
        "generated_at": generated_at,
        "fresh": is_fresh,
        "metadata": payload.get("metadata") or {},
        "runs": runs,
        "note": (
            "Reused sanitized field-scale runtime matrix artifact. "
            "Run architecture-runtime-evidence --run-field-scale to refresh it."
        ),
    }
    if reason:
        evidence["reason"] = reason
    return {"status": status, "evidence": evidence}, (
        "artifact-passed" if status == "passed" else "artifact-failed"
    )


def _parse_iso_datetime(value):
    if not value or not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _write_field_scale_matrix_artifact(*, artifact_path, status, runs, metadata):
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "metadata": metadata,
        "runs": list(runs),
        "note": (
            "Sanitized field-scale smoke evidence. Commands intentionally omit "
            "credentials, network IDs, snapshot IDs, and raw command output."
        ),
    }
    artifact_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifact_path.chmod(0o666)


def _wait_for_chaos_scenario_ready(
    context,
    *,
    sync_name,
    scenario,
    timeout_seconds,
    poll_seconds,
):
    started = time.time()
    while True:
        if _is_chaos_scenario_ready(context, sync_name=sync_name, scenario=scenario):
            return
        if time.time() - started >= timeout_seconds:
            raise Exit(
                f"Timed out waiting for scenario `{scenario}` readiness on sync `{sync_name}`.",
                code=1,
            )
        time.sleep(max(1, poll_seconds))


def _is_chaos_scenario_ready(context, *, sync_name, scenario):
    output = manage_py(
        context,
        f'forward_chaos_probe --sync-name "{sync_name}" --scenario "{scenario}"',
        hide=True,
        warn=True,
    ).stdout.strip()
    return output.endswith("1")


def _export_chaos_bundle(context, *, sync_name, scenario, output_dir):
    output_path = Path(output_dir)
    result = manage_py(
        context,
        (
            f'forward_chaos_probe --sync-name "{sync_name}" '
            f'--scenario "{scenario}" '
            f'--export-dir "{_container_export_dir(output_path)}"'
        ),
    )
    stdout = str(getattr(result, "stdout", "") or "")
    for line in reversed(stdout.splitlines()):
        candidate = Path(line.strip())
        if candidate.exists():
            return candidate
    candidates = sorted(output_path.glob(f"chaos-{scenario}-run-*.json"))
    return candidates[-1] if candidates else None


def _container_export_dir(output_dir, *, repo_root=None):
    repo_root = Path(repo_root or Path(__file__).resolve().parent).resolve()
    output_path = Path(output_dir)
    if output_path.is_absolute():
        try:
            relative_path = output_path.resolve().relative_to(repo_root)
        except ValueError:
            return str(output_path)
    else:
        relative_path = output_path
    return f"/source/{relative_path.as_posix()}"


def _assert_chaos_bundle_recovery(*, output_dir, scenario):
    target_dir = Path(output_dir)
    candidates = sorted(target_dir.glob(f"chaos-{scenario}-run-*.json"))
    if not candidates:
        raise Exit(
            f"Chaos support bundle export missing for scenario `{scenario}`.",
            code=1,
        )
    bundle_path = candidates[-1]
    try:
        payload = json.loads(bundle_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise Exit(
            f"Chaos support bundle `{bundle_path}` is unreadable: {exc}",
            code=1,
        ) from exc

    run = payload.get("run") or {}
    steps = payload.get("steps") or []
    recommendation = payload.get("recovery_recommendation") or {}
    action = str(recommendation.get("action") or "").strip()
    allowed_actions = {
        "none",
        "wait",
        "wait_for_review",
        "retry_current_step",
        "requeue_merge",
        "discard_branch_retry",
        "complete",
        "reconcile",
        "monitor",
    }
    expected_actions = _chaos_expected_actions_for_scenario(scenario)
    if not run.get("id"):
        raise Exit(
            f"Chaos support bundle `{bundle_path}` is missing run metadata.",
            code=1,
        )
    if not isinstance(steps, list) or not steps:
        raise Exit(
            f"Chaos support bundle `{bundle_path}` has no execution steps.",
            code=1,
        )
    if action not in allowed_actions:
        raise Exit(
            (
                f"Chaos support bundle `{bundle_path}` has unsupported recovery "
                f"action `{action}`."
            ),
            code=1,
        )
    if expected_actions and action not in expected_actions:
        raise Exit(
            (
                f"Chaos support bundle `{bundle_path}` recovery action `{action}` "
                f"does not match scenario `{scenario}` expectations."
            ),
            code=1,
        )
    _assert_chaos_scenario_step_state(
        steps=steps,
        scenario=scenario,
        bundle_path=str(bundle_path),
    )
    return bundle_path


def _write_chaos_kill_metadata(
    *,
    output_dir,
    scenario,
    sync_name="",
    killed_worker_id="",
    restored_worker_replicas=0,
    support_bundle_path=None,
    support_bundle_recovery_verified=False,
):
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    support_bundle = Path(support_bundle_path) if support_bundle_path else None
    bundle_payload = _read_chaos_json(support_bundle) if support_bundle else {}
    run = bundle_payload.get("run") or {}
    steps = bundle_payload.get("steps") or []
    recommendation = bundle_payload.get("recovery_recommendation") or {}
    active_step = _chaos_representative_step(
        steps=steps,
        scenario=scenario,
        recommendation=recommendation,
    )
    support_bundle_value = str(support_bundle) if support_bundle else ""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    metadata = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scenario": str(scenario or ""),
        "sync_name": str(sync_name or ""),
        "killed_worker_id": str(killed_worker_id or ""),
        "restored_worker_replicas": int(restored_worker_replicas or 0),
        "support_bundle": support_bundle_value,
        "support_bundle_recovery_verified": bool(support_bundle_recovery_verified),
        "execution_run_id": run.get("id"),
        "active_step_id": active_step.get("id"),
        "active_step_index": active_step.get("index"),
        "active_step_kind": active_step.get("kind"),
        "active_step_status": active_step.get("status"),
        "active_step_job_id": _chaos_step_job_id(active_step, scenario),
        "branch_id": active_step.get("branch"),
        "branch_name": active_step.get("branch_name") or "",
        "recovery_action": recommendation.get("action") or "",
        "recovery_severity": recommendation.get("severity") or "",
        "recovery_step_index": recommendation.get("step_index"),
    }
    metadata_path = target_dir / f"chaos-{scenario}-metadata-{timestamp}.json"
    metadata_path.write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return metadata_path


def _latest_chaos_kill_metadata(*, output_dir, scenario):
    candidates = sorted(Path(output_dir).glob(f"chaos-{scenario}-metadata-*.json"))
    for candidate in reversed(candidates):
        payload = _read_chaos_json(candidate)
        if payload:
            return payload
    return {}


def _read_chaos_json(path):
    if path is None:
        return {}
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _chaos_representative_step(*, steps, scenario, recommendation=None):
    step_list = [step for step in steps if isinstance(step, dict)]
    recommendation = recommendation or {}
    recommended_index = recommendation.get("step_index")
    if recommended_index is not None:
        for step in step_list:
            if step.get("kind") == "stage" and step.get("index") == recommended_index:
                return step

    scenario_value = str(scenario or "").strip()
    stage_steps = [step for step in step_list if step.get("kind") == "stage"]
    if scenario_value == "stage-before-branch":
        for step in stage_steps:
            if not step.get("branch") and not step.get("branch_name"):
                return step
    if scenario_value == "stage-after-branch":
        for step in stage_steps:
            if step.get("branch") or step.get("branch_name"):
                return step
    if scenario_value == "stage-during-apply":
        for step in stage_steps:
            if (
                int(step.get("attempted_row_count") or 0) > 0
                or int(step.get("applied_row_count") or 0) > 0
                or int(step.get("fetched_row_count") or 0) > 0
            ):
                return step
    if scenario_value == "merge-during-exec":
        for step in stage_steps:
            if step.get("merge_job") or (step.get("merge_job_detail") or {}).get("pk"):
                return step
    return stage_steps[0] if stage_steps else {}


def _chaos_step_job_id(step, scenario):
    if not step:
        return None
    if str(scenario or "").strip() == "merge-during-exec":
        return step.get("merge_job") or (step.get("merge_job_detail") or {}).get("pk")
    return step.get("job") or (step.get("job_detail") or {}).get("pk")


def _chaos_expected_actions_for_scenario(scenario):
    defaults = {"wait", "monitor", "reconcile"}
    mapping = {
        "stage-before-branch": {
            *defaults,
            "retry_current_step",
        },
        "stage-after-branch": {
            *defaults,
            "discard_branch_retry",
            "retry_current_step",
        },
        "stage-during-apply": {
            *defaults,
            "discard_branch_retry",
            "retry_current_step",
        },
        "merge-during-exec": {
            *defaults,
            "requeue_merge",
            "retry_current_step",
        },
    }
    return mapping.get(str(scenario or "").strip(), defaults)


def _assert_chaos_scenario_step_state(*, steps, scenario, bundle_path):
    stage_steps = [step for step in steps if step.get("kind") == "stage"]
    if not stage_steps:
        raise Exit(
            f"Chaos support bundle `{bundle_path}` has no stage steps.",
            code=1,
        )

    scenario_value = str(scenario or "").strip()
    if scenario_value == "stage-before-branch":
        if not any(
            not step.get("branch")
            and not step.get("branch_name")
            and not step.get("ingestion")
            for step in stage_steps
        ):
            raise Exit(
                (
                    f"Chaos support bundle `{bundle_path}` is missing a pre-branch "
                    "stage step (no branch/ingestion linkage)."
                ),
                code=1,
            )
        return

    if scenario_value == "stage-after-branch":
        if not any(
            step.get("branch") or step.get("branch_name") for step in stage_steps
        ):
            raise Exit(
                (
                    f"Chaos support bundle `{bundle_path}` is missing a stage step "
                    "with branch linkage."
                ),
                code=1,
            )
        return

    if scenario_value == "stage-during-apply":
        if not any(
            int(step.get("attempted_row_count") or 0) > 0
            or int(step.get("applied_row_count") or 0) > 0
            or int(step.get("fetched_row_count") or 0) > 0
            for step in stage_steps
        ):
            raise Exit(
                (
                    f"Chaos support bundle `{bundle_path}` is missing row-progress "
                    "evidence for stage-during-apply."
                ),
                code=1,
            )
        return

    if scenario_value == "merge-during-exec":
        merge_steps = [
            step
            for step in stage_steps
            if step.get("status") in {"merge_queued", "merge_timeout", "merged"}
            and (
                step.get("merge_job") or (step.get("merge_job_detail") or {}).get("pk")
            )
        ]
        if not merge_steps:
            raise Exit(
                (
                    f"Chaos support bundle `{bundle_path}` is missing merge-stage "
                    "job evidence for merge-during-exec."
                ),
                code=1,
            )


@task(name="playwright-test")
def playwright_test(context):
    _run_playwright_with_shared_runtime_fallback(context)


@task
def package(context):
    context.run(f"{shlex.quote(sys.executable)} -m build")


@task
def docs(context):
    context.run(f"{shlex.quote(sys.executable)} -m mkdocs build --strict")


@task(name="smoke-sync")
def smoke_sync(
    context,
    validate_only=False,
    query_limit=5,
    plan_only=False,
    no_auto_merge=False,
    execution_backend="branching",
    max_changes_per_branch=10000,
    enable_bulk_orm=True,
):
    flags = []
    if validate_only:
        flags.append("--validate-only")
    if plan_only:
        flags.append("--plan-only")
    if no_auto_merge:
        flags.append("--no-auto-merge")
    if not bool(enable_bulk_orm):
        flags.append("--disable-bulk-orm")
    if execution_backend != "branching":
        flags.append(f"--execution-backend {execution_backend}")
    if query_limit != 5:
        flags.append(f"--query-limit {int(query_limit)}")
    if max_changes_per_branch != 10000:
        flags.append(f"--max-changes-per-branch {int(max_changes_per_branch)}")
    flag_string = f" {' '.join(flags)}" if flags else ""
    manage_py(context, f"forward_smoke_sync{flag_string}")


@task(name="scale-soak")
def scale_soak(
    context,
    runs=3,
    execution_backend="branching",
    max_changes_per_branch=10000,
    pause_seconds=30,
):
    run_count = int(runs)
    if run_count < 1:
        raise Exit("`--runs` must be at least 1.", code=2)
    for index in range(run_count):
        smoke_sync(
            context,
            execution_backend=execution_backend,
            max_changes_per_branch=int(max_changes_per_branch),
        )
        if index < run_count - 1:
            time.sleep(max(0, int(pause_seconds)))


@task(name="module-readiness")
def module_readiness(
    context,
    sync_name="",
    source_name="",
    output_dir="",
):
    flags = []
    if sync_name:
        flags.append(f'--sync-name "{sync_name}"')
    if source_name:
        flags.append(f'--source-name "{source_name}"')
    if output_dir:
        flags.append(f'--output-dir "{output_dir}"')
    flag_string = f" {' '.join(flags)}" if flags else ""
    manage_py(context, f"forward_module_readiness{flag_string}")


@task(name="pushdown-profile")
def pushdown_profile(
    context,
    sync_name="",
    model="",
    query_name="",
    sample_shard_keys=200,
    top_slow_models=0,
    output_json="",
):
    if not sync_name:
        raise Exit("`--sync-name` is required.", code=2)
    if not model and int(top_slow_models) <= 0:
        raise Exit("Provide `--model` or set `--top-slow-models`.", code=2)
    if int(sample_shard_keys) < 1:
        raise Exit("`--sample-shard-keys` must be at least 1.", code=2)
    flags = [
        f'--sync-name "{sync_name}"',
        f"--sample-shard-keys {int(sample_shard_keys)}",
    ]
    if model:
        flags.append(f'--model "{model}"')
    if int(top_slow_models) > 0:
        flags.append(f"--top-slow-models {int(top_slow_models)}")
    if query_name:
        flags.append(f'--query-name "{query_name}"')
    if output_json:
        flags.append(f'--output-json "{output_json}"')
    manage_py(context, f"forward_pushdown_profile {' '.join(flags)}")


@task(name="architecture-audit")
def architecture_audit(context, sync_name="", output_json="", fail_on_gap=False):
    flags = []
    if sync_name:
        flags.append(f'--sync-name "{sync_name}"')
    if output_json:
        flags.append(f'--output-json "{output_json}"')
    if fail_on_gap:
        flags.append("--fail-on-gap")
    manage_py(context, f"forward_architecture_audit {' '.join(flags)}")


@task(name="architecture-audit-check")
def architecture_audit_check(context):
    """Fail fast when model eligibility classification has architecture gaps."""
    architecture_audit.body(context, fail_on_gap=True)


@task(name="validation-org-query-audit")
def validation_org_query_audit(
    context,
    source_name="",
    url="",
    username="",
    password="",
    network_id="",
    repository="org",
    directory="/forward_netbox_validation/",
    commit_message="",
    repair=False,
    fail_on_gap=False,
    output_json="",
):
    flags = []
    if source_name:
        flags.append(f'--source-name "{source_name}"')
    if url:
        flags.append(f'--url "{url}"')
    if username:
        flags.append(f'--username "{username}"')
    if password:
        flags.append(f'--password "{password}"')
    if network_id:
        flags.append(f'--network-id "{network_id}"')
    if repository:
        flags.append(f'--repository "{repository}"')
    if directory:
        flags.append(f'--directory "{directory}"')
    if commit_message:
        flags.append(f'--commit-message "{commit_message}"')
    if repair:
        flags.append("--repair")
    if fail_on_gap:
        flags.append("--fail-on-gap")
    if output_json:
        flags.append(f'--output-json "{output_json}"')
    manage_py(context, f"forward_validation_org_query_audit {' '.join(flags)}")


@task(name="validation-org-query-audit-ci")
def validation_org_query_audit_ci(context):
    """Run the validation-org query audit when the required credentials exist."""
    if not (
        os.getenv("FORWARD_VALIDATION_USERNAME")
        and os.getenv("FORWARD_VALIDATION_PASSWORD")
        and os.getenv("FORWARD_VALIDATION_NETWORK_ID")
    ):
        return
    validation_org_query_audit.body(
        context,
        source_name=os.getenv("FORWARD_VALIDATION_SOURCE_NAME", "validation-source"),
        url=os.getenv("FORWARD_VALIDATION_URL", "https://fwd.app"),
        username=os.getenv("FORWARD_VALIDATION_USERNAME", ""),
        password=os.getenv("FORWARD_VALIDATION_PASSWORD", ""),
        network_id=os.getenv("FORWARD_VALIDATION_NETWORK_ID", ""),
        repository=os.getenv("FORWARD_VALIDATION_REPOSITORY", "org"),
        directory=os.getenv(
            "FORWARD_VALIDATION_DIRECTORY", "/forward_netbox_validation/"
        ),
        fail_on_gap=True,
    )


@task(name="architecture-completion-audit")
def architecture_completion_audit(context, output_json=""):
    flags = []
    if output_json:
        flags.append(f'--output-json "{output_json}"')
    manage_py(context, f"forward_architecture_completion_audit {' '.join(flags)}")


@task(name="scale-benchmark")
def scale_benchmark(
    context,
    sync_name="",
    run_id="",
    input_json="",
    output_json="docs/03_Plans/evidence/scale-benchmark.json",
    reconcile=False,
    fail_on_warn=False,
    fail_on_fail=False,
):
    """Evaluate scale-readiness from execution-run support-bundle metrics."""
    selectors = [value for value in (sync_name, run_id, input_json) if value]
    if len(selectors) != 1:
        raise Exit(
            "Provide exactly one of --sync-name, --run-id, or --input-json.", code=2
        )
    if input_json and reconcile:
        raise Exit("--reconcile can only be used with --sync-name or --run-id.", code=2)
    flags = []
    if sync_name:
        flags.append(f'--sync-name "{sync_name}"')
    if run_id:
        flags.append(f"--run-id {int(run_id)}")
    if input_json:
        flags.append(f'--input-json "{input_json}"')
    if reconcile:
        flags.append("--reconcile")
    if output_json:
        flags.append(f'--output-json "{output_json}"')
    if fail_on_warn:
        flags.append("--fail-on-warn")
    if fail_on_fail:
        flags.append("--fail-on-fail")
    manage_py(context, f"forward_scale_benchmark {' '.join(flags)}")


@task(name="execution-run-recovery")
def execution_run_recovery(
    context,
    run_id="",
    sync_name="",
    skip_reconcile=False,
    enqueue_next=False,
    output_json="",
):
    """Inspect/reconcile/resume a ledger execution run through native NetBox jobs."""
    selectors = [value for value in (sync_name, run_id) if value]
    if len(selectors) != 1:
        raise Exit("Provide exactly one of --sync-name or --run-id.", code=2)
    flags = []
    if sync_name:
        flags.append(f'--sync-name "{sync_name}"')
    if run_id:
        flags.append(f"--run-id {int(run_id)}")
    if skip_reconcile:
        flags.append("--skip-reconcile")
    if enqueue_next:
        flags.append("--enqueue-next")
    if output_json:
        flags.append(f'--output-json "{output_json}"')
    manage_py(context, f"forward_execution_run_recovery {' '.join(flags)}")


@task(name="sync-health-gate")
def sync_health_gate(
    context,
    sync_id=0,
    sync_name="",
    max_polls=60,
    interval_seconds=60,
    fail_on_warning=True,
    fail_on_suppressed_warning=False,
    fail_on_error=True,
    allow_nonterminal=False,
    include_all_ingestions=False,
    failed_status_threshold=2,
):
    """Poll watch/blocker/warning audits and fail on blocker/warning/error regressions."""
    selector_count = int(bool(sync_id)) + int(bool(sync_name))
    if selector_count != 1:
        raise Exit("Provide exactly one of --sync-id or --sync-name.", code=2)

    watch_flags = ["--interval-seconds 1", "--max-polls 1", "--allow-nonterminal"]
    if sync_id:
        watch_flags.append(f"--sync-id {int(sync_id)}")
    else:
        watch_flags.append(f'--sync-name "{sync_name}"')

    failed_threshold = max(1, int(failed_status_threshold))
    failed_streak = 0
    terminal_run_failures = {"failed", "timeout", "cancelled"}
    inflight_run_statuses = {"running", "queued", "waiting"}
    for poll in range(1, int(max_polls) + 1):
        watch_payload = _manage_py_json_retry(
            context, f"forward_watch_sync {' '.join(watch_flags)}"
        )
        resolved_sync_id = int(watch_payload.get("sync_id") or 0)
        sync_status = str(watch_payload.get("sync_status") or "").strip().lower()
        run_status = (
            str(((watch_payload.get("execution_run") or {}).get("status") or ""))
            .strip()
            .lower()
        )

        blocker_payload = _manage_py_json_retry(
            context, f"forward_blocker_audit --sync-id {resolved_sync_id}"
        )
        warning_command = f"forward_warning_audit --sync-id {resolved_sync_id}"
        if bool(include_all_ingestions):
            warning_command += " --all-ingestions"
        warning_payload = _manage_py_json_retry(context, warning_command)

        blocking = int(((blocker_payload.get("counts") or {}).get("blocking") or 0))
        warnings = int(warning_payload.get("warning_count") or 0)
        suppressed_warnings = int(warning_payload.get("suppressed_warning_count") or 0)
        errors = int(warning_payload.get("error_count") or 0)
        print(
            "sync-health-gate poll "
            f"{poll}/{int(max_polls)} sync_id={resolved_sync_id} status={sync_status} "
            f"run_status={run_status or 'n/a'} "
            f"blocking={blocking} warnings={warnings} "
            f"suppressed_warnings={suppressed_warnings} errors={errors}"
        )

        if blocking > 0:
            raise Exit(
                f"sync-health-gate failed: blocking issues detected (count={blocking}).",
                code=3,
            )
        if bool(fail_on_warning) and warnings > 0:
            raise Exit(
                f"sync-health-gate failed: warning issues detected (count={warnings}).",
                code=3,
            )
        if bool(fail_on_suppressed_warning) and suppressed_warnings > 0:
            raise Exit(
                "sync-health-gate failed: suppressed warning issues detected "
                f"(count={suppressed_warnings}).",
                code=3,
            )
        if bool(fail_on_error) and errors > 0:
            raise Exit(
                f"sync-health-gate failed: error issues detected (count={errors}).",
                code=3,
            )

        sync_failed = sync_status == "failed"
        run_failed = run_status in terminal_run_failures
        run_inflight = run_status in inflight_run_statuses
        if run_failed or (sync_failed and not run_inflight):
            failed_streak += 1
            if failed_streak >= failed_threshold:
                raise Exit(
                    "sync-health-gate failed: sync/execution run reached failed status.",
                    code=3,
                )
            if poll < int(max_polls):
                time.sleep(max(1, int(interval_seconds)))
                continue
        else:
            failed_streak = 0

        if sync_status in {"completed", "failed"}:
            print("sync-health-gate passed: sync completed with clean audits.")
            return

        if poll < int(max_polls):
            time.sleep(max(1, int(interval_seconds)))

    if bool(allow_nonterminal):
        print("sync-health-gate passed: audits are clean on non-terminal run state.")
        return

    raise Exit(
        (
            "sync-health-gate timed out before completion. Increase --max-polls "
            "or --interval-seconds for long-running dataset validation, or set "
            "--allow-nonterminal=True for in-progress health checks."
        ),
        code=4,
    )


@task(name="sync-health-monitor")
def sync_health_monitor(
    context,
    sync_ids="",
    max_polls=120,
    interval_seconds=60,
    allow_nonterminal=True,
    include_all_ingestions=False,
    fail_on_suppressed_warning=False,
    output_json="",
    failed_status_threshold=2,
):
    """Continuously sample health for one or more sync IDs and fail on blocker/warning/error findings."""
    parsed_sync_ids = []
    for token in str(sync_ids or "").split(","):
        token = token.strip()
        if not token:
            continue
        try:
            parsed_sync_ids.append(int(token))
        except ValueError as exc:
            raise Exit(
                f"Invalid sync id `{token}` in --sync-ids list.", code=2
            ) from exc
    if not parsed_sync_ids:
        raise Exit("Provide at least one sync id via --sync-ids.", code=2)

    failed_threshold = max(1, int(failed_status_threshold))
    failed_streak = {sync_id: 0 for sync_id in parsed_sync_ids}
    terminal_run_failures = {"failed", "timeout", "cancelled"}
    inflight_run_statuses = {"running", "queued", "waiting"}
    samples = []
    all_terminal = False
    output_path = Path(output_json).expanduser() if output_json else None
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)

    def _flush_health_output(*, completed: bool):
        if output_path is None:
            return
        output_payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "sync_ids": parsed_sync_ids,
            "max_polls": int(max_polls),
            "interval_seconds": int(interval_seconds),
            "allow_nonterminal": bool(allow_nonterminal),
            "include_all_ingestions": bool(include_all_ingestions),
            "completed": bool(completed),
            "samples": samples,
        }
        output_path.write_text(json.dumps(output_payload, indent=2), encoding="utf-8")

    for poll in range(1, int(max_polls) + 1):
        all_terminal = True
        sampled_at = datetime.now(timezone.utc).isoformat()
        for sync_id in parsed_sync_ids:
            watch_payload = _manage_py_json_retry(
                context,
                (
                    "forward_watch_sync "
                    f"--sync-id {sync_id} --interval-seconds 1 --max-polls 1 "
                    "--allow-nonterminal"
                ),
            )
            blocker_payload = _manage_py_json_retry(
                context, f"forward_blocker_audit --sync-id {sync_id}"
            )
            warning_command = f"forward_warning_audit --sync-id {sync_id}"
            if bool(include_all_ingestions):
                warning_command += " --all-ingestions"
            warning_payload = _manage_py_json_retry(context, warning_command)

            sync_status = str(watch_payload.get("sync_status") or "").strip().lower()
            run_status = (
                str(((watch_payload.get("execution_run") or {}).get("status") or ""))
                .strip()
                .lower()
            )
            blocking = int(((blocker_payload.get("counts") or {}).get("blocking") or 0))
            warnings = int(warning_payload.get("warning_count") or 0)
            suppressed_warnings = int(
                warning_payload.get("suppressed_warning_count") or 0
            )
            errors = int(warning_payload.get("error_count") or 0)

            print(
                "sync-health-monitor poll "
                f"{poll}/{int(max_polls)} sync_id={sync_id} status={sync_status} "
                f"run_status={run_status or 'n/a'} "
                f"blocking={blocking} warnings={warnings} "
                f"suppressed_warnings={suppressed_warnings} errors={errors}"
            )

            sample = {
                "sampled_at": sampled_at,
                "poll": poll,
                "sync_id": sync_id,
                "sync_status": sync_status,
                "execution_run_status": run_status,
                "blocking": blocking,
                "warnings": warnings,
                "suppressed_warnings": suppressed_warnings,
                "errors": errors,
            }
            samples.append(sample)
            _flush_health_output(completed=False)

            if blocking > 0:
                raise Exit(
                    f"sync-health-monitor failed: blocking issues detected for sync {sync_id} (count={blocking}).",
                    code=3,
                )
            if warnings > 0:
                raise Exit(
                    f"sync-health-monitor failed: warning issues detected for sync {sync_id} (count={warnings}).",
                    code=3,
                )
            if bool(fail_on_suppressed_warning) and suppressed_warnings > 0:
                raise Exit(
                    "sync-health-monitor failed: suppressed warning issues detected "
                    f"for sync {sync_id} (count={suppressed_warnings}).",
                    code=3,
                )
            if errors > 0:
                raise Exit(
                    f"sync-health-monitor failed: error issues detected for sync {sync_id} (count={errors}).",
                    code=3,
                )

            sync_failed = sync_status == "failed"
            run_failed = run_status in terminal_run_failures
            run_inflight = run_status in inflight_run_statuses
            if run_failed or (sync_failed and not run_inflight):
                failed_streak[sync_id] += 1
                if failed_streak[sync_id] >= failed_threshold:
                    raise Exit(
                        (
                            "sync-health-monitor failed: sync "
                            f"{sync_id} reached failed status threshold "
                            f"(sync_status={sync_status}, run_status={run_status or 'n/a'})."
                        ),
                        code=3,
                    )
            else:
                failed_streak[sync_id] = 0

            if sync_status not in {"completed", "failed"}:
                all_terminal = False

        if all_terminal:
            break
        if poll < int(max_polls):
            time.sleep(max(1, int(interval_seconds)))

    if output_path is not None:
        _flush_health_output(completed=True)
        print(f"Wrote sync health monitor evidence: {output_path}")

    if all_terminal:
        print("sync-health-monitor passed: all syncs reached terminal status cleanly.")
        return
    if bool(allow_nonterminal):
        print("sync-health-monitor passed: audits are clean on non-terminal run state.")
        return
    raise Exit(
        (
            "sync-health-monitor timed out before all syncs reached terminal status. "
            "Increase --max-polls/--interval-seconds or allow non-terminal runs."
        ),
        code=4,
    )


@task(name="sync-autorecover-monitor")
def sync_autorecover_monitor(
    context,
    sync_ids="",
    max_polls=120,
    interval_seconds=60,
    allow_nonterminal=True,
    include_all_ingestions=False,
    fail_on_suppressed_warning=False,
    output_json="",
    failed_status_threshold=2,
    orphan_pending_min_seconds=120,
    stalled_inflight_min_seconds=900,
    fail_on_recovery=False,
):
    """Monitor sync health and auto-recover dead in-flight execution steps."""
    parsed_sync_ids = []
    for token in str(sync_ids or "").split(","):
        token = token.strip()
        if not token:
            continue
        try:
            parsed_sync_ids.append(int(token))
        except ValueError as exc:
            raise Exit(
                f"Invalid sync id `{token}` in --sync-ids list.", code=2
            ) from exc
    if not parsed_sync_ids:
        raise Exit("Provide at least one sync id via --sync-ids.", code=2)

    failed_threshold = max(1, int(failed_status_threshold))
    terminal_run_failures = {"failed", "timeout", "cancelled"}
    inflight_run_statuses = {"running", "queued", "waiting"}
    failed_streak = {sync_id: 0 for sync_id in parsed_sync_ids}
    orphan_pending_streak = {}
    stalled_inflight_streak = {}
    staged_waiting_streak = {}
    actionable_step_failure_streak = {}
    stalled_inflight_threshold = 4
    orphan_pending_min_seconds = max(1, int(orphan_pending_min_seconds))
    stalled_inflight_min_seconds = max(1, int(stalled_inflight_min_seconds))
    samples = []
    recovery_actions = []
    all_terminal = False
    output_path = Path(output_json).expanduser() if output_json else None
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)

    def _flush_autorecover_output(*, completed: bool):
        if output_path is None:
            return
        output_payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "sync_ids": parsed_sync_ids,
            "max_polls": int(max_polls),
            "interval_seconds": int(interval_seconds),
            "allow_nonterminal": bool(allow_nonterminal),
            "include_all_ingestions": bool(include_all_ingestions),
            "completed": bool(completed),
            "samples": samples,
            "recovery_actions": recovery_actions,
        }
        output_path.write_text(json.dumps(output_payload, indent=2), encoding="utf-8")

    for poll in range(1, int(max_polls) + 1):
        all_terminal = True
        sampled_at = datetime.now(timezone.utc).isoformat()
        for sync_id in parsed_sync_ids:
            watch_payload = _manage_py_json_retry(
                context,
                (
                    "forward_watch_sync "
                    f"--sync-id {sync_id} --interval-seconds 1 --max-polls 1 "
                    "--allow-nonterminal"
                ),
            )
            blocker_payload = _manage_py_json_retry(
                context, f"forward_blocker_audit --sync-id {sync_id}"
            )
            warning_command = f"forward_warning_audit --sync-id {sync_id}"
            if bool(include_all_ingestions):
                warning_command += " --all-ingestions"
            warning_payload = _manage_py_json_retry(context, warning_command)

            sync_name = str(watch_payload.get("sync_name") or "").strip()
            sync_status = str(watch_payload.get("sync_status") or "").strip().lower()
            execution_run = watch_payload.get("execution_run") or {}
            run_status = str(execution_run.get("status") or "").strip().lower()
            run_heartbeat_age_seconds = execution_run.get(
                "latest_heartbeat_age_seconds"
            )
            try:
                run_heartbeat_age_seconds = (
                    float(run_heartbeat_age_seconds)
                    if run_heartbeat_age_seconds is not None
                    else None
                )
            except (TypeError, ValueError):
                run_heartbeat_age_seconds = None
            active_step = execution_run.get("active_step") or {}
            step_id = active_step.get("id")
            step_status = str(active_step.get("status") or "").strip().lower()
            step_job_live = active_step.get("job_live")
            step_job_id = active_step.get("job_id")
            step_attempted = int(active_step.get("attempted_row_count") or 0)
            step_applied = int(active_step.get("applied_row_count") or 0)
            step_fetched = int(active_step.get("fetched_row_count") or 0)
            step_created_age_seconds = active_step.get("created_age_seconds")
            try:
                step_created_age_seconds = (
                    float(step_created_age_seconds)
                    if step_created_age_seconds is not None
                    else None
                )
            except (TypeError, ValueError):
                step_created_age_seconds = None
            step_heartbeat_age_seconds = active_step.get("heartbeat_age_seconds")
            try:
                step_heartbeat_age_seconds = (
                    float(step_heartbeat_age_seconds)
                    if step_heartbeat_age_seconds is not None
                    else None
                )
            except (TypeError, ValueError):
                step_heartbeat_age_seconds = None

            blocking = int(((blocker_payload.get("counts") or {}).get("blocking") or 0))
            warnings = int(warning_payload.get("warning_count") or 0)
            suppressed_warnings = int(
                warning_payload.get("suppressed_warning_count") or 0
            )
            errors = int(warning_payload.get("error_count") or 0)

            print(
                "sync-autorecover-monitor poll "
                f"{poll}/{int(max_polls)} sync_id={sync_id} status={sync_status} "
                f"run_status={run_status or 'n/a'} step_status={step_status or 'n/a'} "
                f"job_live={step_job_live} blocking={blocking} warnings={warnings} "
                f"suppressed_warnings={suppressed_warnings} errors={errors}"
            )

            sample = {
                "sampled_at": sampled_at,
                "poll": poll,
                "sync_id": sync_id,
                "sync_name": sync_name,
                "sync_status": sync_status,
                "execution_run_status": run_status,
                "execution_run_heartbeat_age_seconds": run_heartbeat_age_seconds,
                "active_step_status": step_status,
                "active_step_id": step_id,
                "active_step_job_live": step_job_live,
                "active_step_job_id": step_job_id,
                "active_step_attempted_row_count": step_attempted,
                "active_step_applied_row_count": step_applied,
                "active_step_fetched_row_count": step_fetched,
                "active_step_created_age_seconds": step_created_age_seconds,
                "active_step_heartbeat_age_seconds": step_heartbeat_age_seconds,
                "blocking": blocking,
                "warnings": warnings,
                "suppressed_warnings": suppressed_warnings,
                "errors": errors,
            }
            samples.append(sample)
            _flush_autorecover_output(completed=False)

            if blocking > 0:
                raise Exit(
                    f"sync-autorecover-monitor failed: blocking issues detected for sync {sync_id} (count={blocking}).",
                    code=3,
                )
            if warnings > 0:
                raise Exit(
                    f"sync-autorecover-monitor failed: warning issues detected for sync {sync_id} (count={warnings}).",
                    code=3,
                )
            if bool(fail_on_suppressed_warning) and suppressed_warnings > 0:
                raise Exit(
                    "sync-autorecover-monitor failed: suppressed warning issues "
                    f"detected for sync {sync_id} (count={suppressed_warnings}).",
                    code=3,
                )
            if errors > 0:
                raise Exit(
                    f"sync-autorecover-monitor failed: error issues detected for sync {sync_id} (count={errors}).",
                    code=3,
                )

            dead_inflight_job = (
                step_job_live is False
                and step_job_id is not None
                and step_status in {"queued", "running", "merge_queued"}
            )
            actionable_step_failure = step_status in {
                "failed",
                "timeout",
                "merge_timeout",
            }
            actionable_failure_identity = (
                step_id,
                execution_run.get("next_step_index"),
                execution_run.get("id"),
                step_status,
            )
            if actionable_step_failure:
                previous = actionable_step_failure_streak.get(sync_id)
                if previous and previous.get("identity") == actionable_failure_identity:
                    actionable_step_failure_streak[sync_id] = {
                        "identity": actionable_failure_identity,
                        "count": int(previous.get("count", 0)) + 1,
                    }
                else:
                    actionable_step_failure_streak[sync_id] = {
                        "identity": actionable_failure_identity,
                        "count": 1,
                    }
            else:
                actionable_step_failure_streak.pop(sync_id, None)
            actionable_step_failure_count = int(
                (actionable_step_failure_streak.get(sync_id) or {}).get("count", 0)
            )
            orphan_pending_stale_heartbeat = (
                step_heartbeat_age_seconds is None
                or step_heartbeat_age_seconds >= orphan_pending_min_seconds
            )
            orphan_pending_step = (
                step_status == "pending"
                and step_job_id is None
                and run_status == "running"
                and (
                    run_heartbeat_age_seconds is None
                    or run_heartbeat_age_seconds >= orphan_pending_min_seconds
                )
                and step_attempted == 0
                and step_applied == 0
                and step_created_age_seconds is not None
                and step_created_age_seconds >= orphan_pending_min_seconds
            )
            orphan_pending_identity = (
                step_id,
                execution_run.get("next_step_index"),
                execution_run.get("id"),
            )
            if orphan_pending_step:
                previous = orphan_pending_streak.get(sync_id)
                if previous and previous.get("identity") == orphan_pending_identity:
                    orphan_pending_streak[sync_id] = {
                        "identity": orphan_pending_identity,
                        "count": int(previous.get("count", 0)) + 1,
                    }
                else:
                    orphan_pending_streak[sync_id] = {
                        "identity": orphan_pending_identity,
                        "count": 1,
                    }
            else:
                orphan_pending_streak.pop(sync_id, None)
            orphan_pending_count = int(
                (orphan_pending_streak.get(sync_id) or {}).get("count", 0)
            )
            stalled_inflight_candidate = (
                run_status == "running"
                and step_status == "running"
                and step_job_live is True
                and step_job_id is not None
                and step_heartbeat_age_seconds is not None
                and step_heartbeat_age_seconds >= stalled_inflight_min_seconds
            )
            stalled_inflight_identity = (
                step_id,
                execution_run.get("next_step_index"),
                execution_run.get("id"),
                step_job_id,
            )
            stalled_inflight_progress = (step_attempted, step_applied, step_fetched)
            if stalled_inflight_candidate:
                previous = stalled_inflight_streak.get(sync_id)
                if previous and previous.get("identity") == stalled_inflight_identity:
                    if previous.get("progress") == stalled_inflight_progress:
                        stalled_inflight_streak[sync_id] = {
                            "identity": stalled_inflight_identity,
                            "progress": stalled_inflight_progress,
                            "count": int(previous.get("count", 0)) + 1,
                        }
                    else:
                        stalled_inflight_streak[sync_id] = {
                            "identity": stalled_inflight_identity,
                            "progress": stalled_inflight_progress,
                            "count": 1,
                        }
                else:
                    stalled_inflight_streak[sync_id] = {
                        "identity": stalled_inflight_identity,
                        "progress": stalled_inflight_progress,
                        "count": 1,
                    }
            else:
                stalled_inflight_streak.pop(sync_id, None)
            stalled_inflight_count = int(
                (stalled_inflight_streak.get(sync_id) or {}).get("count", 0)
            )
            staged_waiting_candidate = (
                run_status in {"running", "waiting"}
                and step_status == "staged"
                and step_job_id is not None
                and step_job_live is False
                and (
                    step_heartbeat_age_seconds is None
                    or step_heartbeat_age_seconds >= orphan_pending_min_seconds
                )
            )
            staged_waiting_identity = (
                step_id,
                execution_run.get("next_step_index"),
                execution_run.get("id"),
                step_job_id,
            )
            if staged_waiting_candidate:
                previous = staged_waiting_streak.get(sync_id)
                if previous and previous.get("identity") == staged_waiting_identity:
                    staged_waiting_streak[sync_id] = {
                        "identity": staged_waiting_identity,
                        "count": int(previous.get("count", 0)) + 1,
                    }
                else:
                    staged_waiting_streak[sync_id] = {
                        "identity": staged_waiting_identity,
                        "count": 1,
                    }
            else:
                staged_waiting_streak.pop(sync_id, None)
            staged_waiting_count = int(
                (staged_waiting_streak.get(sync_id) or {}).get("count", 0)
            )
            recovered_this_poll = False
            if dead_inflight_job and sync_name:
                recovery_payload = _manage_py_json_retry(
                    context,
                    (
                        "forward_execution_run_recovery "
                        f'--sync-name "{sync_name}" --enqueue-next'
                    ),
                )
                recovery_actions.append(
                    {
                        "sampled_at": sampled_at,
                        "poll": poll,
                        "sync_id": sync_id,
                        "sync_name": sync_name,
                        "reason": "dead_inflight_job",
                        "recovery_payload": recovery_payload,
                    }
                )
                recovered_this_poll = True
                _flush_autorecover_output(completed=False)
                print(
                    "sync-autorecover-monitor recovery "
                    f"sync_id={sync_id} reason=dead_inflight_job"
                )
            if (
                actionable_step_failure
                and actionable_step_failure_count >= 2
                and sync_name
            ):
                recovery_payload = _manage_py_json_retry(
                    context,
                    (
                        "forward_execution_run_recovery "
                        f'--sync-name "{sync_name}" --enqueue-next'
                    ),
                )
                recovery_actions.append(
                    {
                        "sampled_at": sampled_at,
                        "poll": poll,
                        "sync_id": sync_id,
                        "sync_name": sync_name,
                        "reason": "actionable_step_failure",
                        "actionable_step_failure_count": actionable_step_failure_count,
                        "step_status": step_status,
                        "recovery_payload": recovery_payload,
                    }
                )
                recovered_this_poll = True
                _flush_autorecover_output(completed=False)
                print(
                    "sync-autorecover-monitor recovery "
                    f"sync_id={sync_id} reason=actionable_step_failure step_status={step_status}"
                )
            orphan_pending_threshold = 2 if orphan_pending_stale_heartbeat else 6
            if (
                orphan_pending_step
                and orphan_pending_count >= orphan_pending_threshold
                and sync_name
            ):
                recovery_payload = _manage_py_json_retry(
                    context,
                    (
                        "forward_execution_run_recovery "
                        f'--sync-name "{sync_name}" --enqueue-next'
                    ),
                )
                recovery_actions.append(
                    {
                        "sampled_at": sampled_at,
                        "poll": poll,
                        "sync_id": sync_id,
                        "sync_name": sync_name,
                        "reason": "orphan_pending_step",
                        "orphan_pending_count": orphan_pending_count,
                        "orphan_pending_threshold": orphan_pending_threshold,
                        "step_created_age_seconds": step_created_age_seconds,
                        "orphan_pending_min_seconds": orphan_pending_min_seconds,
                        "run_heartbeat_age_seconds": run_heartbeat_age_seconds,
                        "orphan_pending_stale_heartbeat": orphan_pending_stale_heartbeat,
                        "step_status": step_status,
                        "recovery_payload": recovery_payload,
                    }
                )
                recovered_this_poll = True
                _flush_autorecover_output(completed=False)
                print(
                    "sync-autorecover-monitor recovery "
                    f"sync_id={sync_id} reason=orphan_pending_step"
                )
            if (
                stalled_inflight_candidate
                and stalled_inflight_count >= stalled_inflight_threshold
                and sync_name
            ):
                recovery_payload = _manage_py_json_retry(
                    context,
                    (
                        "forward_execution_run_recovery "
                        f'--sync-name "{sync_name}" --enqueue-next'
                    ),
                )
                recovery_actions.append(
                    {
                        "sampled_at": sampled_at,
                        "poll": poll,
                        "sync_id": sync_id,
                        "sync_name": sync_name,
                        "reason": "stalled_inflight_progress",
                        "stalled_inflight_count": stalled_inflight_count,
                        "step_status": step_status,
                        "step_job_id": step_job_id,
                        "attempted_row_count": step_attempted,
                        "applied_row_count": step_applied,
                        "fetched_row_count": step_fetched,
                        "step_heartbeat_age_seconds": step_heartbeat_age_seconds,
                        "stalled_inflight_min_seconds": stalled_inflight_min_seconds,
                        "recovery_payload": recovery_payload,
                    }
                )
                recovered_this_poll = True
                _flush_autorecover_output(completed=False)
                print(
                    "sync-autorecover-monitor recovery "
                    f"sync_id={sync_id} reason=stalled_inflight_progress "
                    f"count={stalled_inflight_count}"
                )
            if staged_waiting_candidate and staged_waiting_count >= 2 and sync_name:
                recovery_payload = _manage_py_json_retry(
                    context,
                    (
                        "forward_execution_run_recovery "
                        f'--sync-name "{sync_name}" --enqueue-next'
                    ),
                )
                recovery_actions.append(
                    {
                        "sampled_at": sampled_at,
                        "poll": poll,
                        "sync_id": sync_id,
                        "sync_name": sync_name,
                        "reason": "staged_waiting_merge",
                        "staged_waiting_count": staged_waiting_count,
                        "step_status": step_status,
                        "step_job_id": step_job_id,
                        "step_heartbeat_age_seconds": step_heartbeat_age_seconds,
                        "orphan_pending_min_seconds": orphan_pending_min_seconds,
                        "recovery_payload": recovery_payload,
                    }
                )
                recovered_this_poll = True
                _flush_autorecover_output(completed=False)
                print(
                    "sync-autorecover-monitor recovery "
                    f"sync_id={sync_id} reason=staged_waiting_merge "
                    f"count={staged_waiting_count}"
                )

            sync_failed = sync_status == "failed"
            run_failed = run_status in terminal_run_failures
            run_inflight = run_status in inflight_run_statuses
            if run_failed and sync_name:
                recovery_payload = _manage_py_json_retry(
                    context,
                    (
                        "forward_execution_run_recovery "
                        f'--sync-name "{sync_name}" --enqueue-next'
                    ),
                )
                recovery_actions.append(
                    {
                        "sampled_at": sampled_at,
                        "poll": poll,
                        "sync_id": sync_id,
                        "sync_name": sync_name,
                        "reason": "terminal_run_status",
                        "run_status": run_status,
                        "recovery_payload": recovery_payload,
                    }
                )
                recovered_this_poll = True
                _flush_autorecover_output(completed=False)
                print(
                    "sync-autorecover-monitor recovery "
                    f"sync_id={sync_id} reason=terminal_run_status run_status={run_status}"
                )
            if run_failed or (sync_failed and not run_inflight):
                failed_streak[sync_id] += 1
                if failed_streak[sync_id] >= failed_threshold:
                    raise Exit(
                        (
                            "sync-autorecover-monitor failed: sync "
                            f"{sync_id} reached failed status threshold "
                            f"(sync_status={sync_status}, run_status={run_status or 'n/a'})."
                        ),
                        code=3,
                    )
                if recovered_this_poll and poll < int(max_polls):
                    continue
            else:
                failed_streak[sync_id] = 0

            if sync_status not in {"completed", "failed"}:
                all_terminal = False

        if all_terminal:
            break
        if poll < int(max_polls):
            time.sleep(max(1, int(interval_seconds)))

    if output_path is not None:
        _flush_autorecover_output(completed=True)
        print(f"Wrote sync auto-recover monitor evidence: {output_path}")

    if bool(fail_on_recovery) and recovery_actions:
        raise Exit(
            (
                "sync-autorecover-monitor failed: recovery actions were required "
                f"(count={len(recovery_actions)})."
            ),
            code=3,
        )

    if all_terminal:
        print(
            "sync-autorecover-monitor passed: all syncs reached terminal status cleanly."
        )
        return
    if bool(allow_nonterminal):
        print(
            "sync-autorecover-monitor passed: audits are clean on non-terminal run state."
        )
        return
    raise Exit(
        (
            "sync-autorecover-monitor timed out before all syncs reached terminal "
            "status. Increase --max-polls/--interval-seconds or allow non-terminal runs."
        ),
        code=4,
    )


@task(name="sync-release-gate")
def sync_release_gate(
    context,
    sync_ids="",
    max_polls=6,
    interval_seconds=10,
    include_all_ingestions=False,
    output_prefix="",
):
    """Run strict release gating checks for one or more sync IDs and write evidence."""
    parsed_sync_ids = []
    for token in str(sync_ids or "").split(","):
        token = token.strip()
        if not token:
            continue
        try:
            parsed_sync_ids.append(int(token))
        except ValueError as exc:
            raise Exit(
                f"Invalid sync id `{token}` in --sync-ids list.", code=2
            ) from exc
    if not parsed_sync_ids:
        raise Exit("Provide at least one sync id via --sync-ids.", code=2)

    evidence_dir = Path("docs/03_Plans/evidence")
    evidence_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    default_prefix = (
        f"sync-release-gate-{'-'.join(str(v) for v in parsed_sync_ids)}-{stamp}"
    )
    prefix = str(output_prefix or "").strip() or default_prefix
    autorecover_output = evidence_dir / f"{prefix}-autorecover.json"
    health_output = evidence_dir / f"{prefix}-health.json"
    summary_output = evidence_dir / f"{prefix}-summary.json"

    sync_ids_csv = ",".join(str(v) for v in parsed_sync_ids)
    sync_autorecover_monitor.body(
        context,
        sync_ids=sync_ids_csv,
        max_polls=int(max_polls),
        interval_seconds=int(interval_seconds),
        allow_nonterminal=True,
        include_all_ingestions=bool(include_all_ingestions),
        fail_on_suppressed_warning=True,
        output_json=str(autorecover_output),
        fail_on_recovery=True,
    )
    sync_health_monitor.body(
        context,
        sync_ids=sync_ids_csv,
        max_polls=int(max_polls),
        interval_seconds=int(interval_seconds),
        allow_nonterminal=True,
        include_all_ingestions=bool(include_all_ingestions),
        fail_on_suppressed_warning=True,
        output_json=str(health_output),
    )

    sync_results = []
    for sync_id in parsed_sync_ids:
        warning_command = f"forward_warning_audit --sync-id {sync_id}"
        if bool(include_all_ingestions):
            warning_command += " --all-ingestions"
        warning_payload = _manage_py_json_retry(context, warning_command)
        blocker_payload = _manage_py_json_retry(
            context, f"forward_blocker_audit --sync-id {sync_id}"
        )
        warning_count = int(warning_payload.get("warning_count") or 0)
        suppressed_warning_count = int(
            warning_payload.get("suppressed_warning_count") or 0
        )
        error_count = int(warning_payload.get("error_count") or 0)
        blocking_count = int(
            ((blocker_payload.get("counts") or {}).get("blocking") or 0)
        )
        if warning_count > 0:
            raise Exit(
                f"sync-release-gate failed: warning issues detected for sync {sync_id} (count={warning_count}).",
                code=3,
            )
        if suppressed_warning_count > 0:
            raise Exit(
                "sync-release-gate failed: suppressed warning issues detected "
                f"for sync {sync_id} (count={suppressed_warning_count}).",
                code=3,
            )
        if error_count > 0:
            raise Exit(
                f"sync-release-gate failed: error issues detected for sync {sync_id} (count={error_count}).",
                code=3,
            )
        if blocking_count > 0:
            raise Exit(
                f"sync-release-gate failed: blocking issues detected for sync {sync_id} (count={blocking_count}).",
                code=3,
            )
        sync_results.append(
            {
                "sync_id": sync_id,
                "warning_count": warning_count,
                "suppressed_warning_count": suppressed_warning_count,
                "error_count": error_count,
                "blocking_count": blocking_count,
            }
        )

    summary_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sync_ids": parsed_sync_ids,
        "max_polls": int(max_polls),
        "interval_seconds": int(interval_seconds),
        "include_all_ingestions": bool(include_all_ingestions),
        "autorecover_output": str(autorecover_output),
        "health_output": str(health_output),
        "sync_results": sync_results,
    }
    summary_output.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")
    print(f"Wrote sync release gate summary: {summary_output}")
    print("sync-release-gate passed: strict release checks are clean.")


@task(name="prune-compat-cache")
def prune_compat_cache(context, sync_name="", dry_run=True, output_json=""):
    """Prune stale legacy `_branch_run` payloads once execution-ledger history exists."""
    flags = []
    if sync_name:
        flags.append(f'--sync-name "{sync_name}"')
    if output_json:
        flags.append(f'--output-json "{output_json}"')
    if bool(dry_run):
        flags.append("--dry-run")
    manage_py(context, f"forward_prune_compatibility_cache {' '.join(flags)}")


@task(
    pre=[
        sensitive_check,
        harness_check,
        harness_test,
        lint,
        build,
        start,
        architecture_audit_check,
        check,
        scenario_test_ci,
        ingestion_delete_regression_ci,
        scale_chaos_test,
        test_ci,
        validation_org_query_audit_ci,
        playwright_test,
        docs,
        package,
    ]
)
def ci(context):
    """Run the local CI-equivalent validation flow."""
