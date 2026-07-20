import json
import os
import re
import runpy
import shlex
import socket
import sys
import time
import tomllib
from datetime import datetime
from datetime import timezone
from pathlib import Path
from types import SimpleNamespace

from dotenv import load_dotenv
from invoke.collection import Collection
from invoke.exceptions import Exit
from invoke.tasks import task as invoke_task

INIT_FILE = "forward_netbox/__init__.py"
ALLOW_SHARED_RUNTIME_TESTS_ENV = "FORWARD_NETBOX_ALLOW_SHARED_RUNTIME_TESTS"
ACTIVE_SYNC_STATUSES = ("queued", "syncing", "merging")
ISOLATED_TEST_PROJECT_NAME = "forward-netbox-test"
ISOLATED_PLAYWRIGHT_PROJECT_NAME = "forward-netbox-ui-test"
RELEASE_ARTIFACT_PROJECT_NAME = "forward-netbox-artifact-test"
ISOLATED_REDIS_DATABASE = 14
ISOLATED_REDIS_CACHE_DATABASE = 15
CYCLONEDX_BOM_VERSION = "7.3.0"
REPO_ROOT = Path(__file__).resolve().parent
_DEVELOPMENT_SECRETS = runpy.run_path(
    str(REPO_ROOT / "scripts" / "development_secrets.py")
)
ensure_development_secrets = _DEVELOPMENT_SECRETS["ensure_development_secrets"]
SCENARIO_TEST_LABELS = " ".join(
    (
        "forward_netbox.tests.test_bulk_merge.BulkMergeIntegrationTest",
        "forward_netbox.tests.test_bulk_merge.SingleBranchExecutorTest",
        "forward_netbox.tests.test_stuck_recovery.StuckRecoveryTest",
    )
)
INGESTION_DELETE_REGRESSION_LABELS = " ".join(
    (
        "forward_netbox.tests.test_bulk_merge.SingleBranchExecutorTest."
        "test_single_branch_repeat_run_applies_delete_phase",
        "forward_netbox.tests.test_sync.ForwardBranchBudgetPlanTest."
        "test_branch_plan_splits_mixed_workloads_into_apply_then_delete_phases",
    )
)
ARCHITECTURE_AUDIT_TEST_LABELS = " ".join(
    (
        "forward_netbox.tests.test_sync.ForwardBranchBudgetPlanTest."
        "test_shard_fetch_contracts_cover_all_supported_models",
        "forward_netbox.tests.test_sync.ForwardSyncRunnerTest."
        "test_apply_engine_classifies_all_supported_models",
        "forward_netbox.tests.test_sync.ForwardSyncRunnerTest."
        "test_apply_engine_classifies_all_supported_models_when_bulk_orm_enabled",
        "forward_netbox.tests.test_query_registry.QueryRegistryTest."
        "test_builtin_query_contract_summary_passes_for_parameterized_maps",
        "forward_netbox.tests.test_query_registry.QueryRegistryTest."
        "test_optional_plugin_query_contract_summary_passes_for_aci_maps",
    )
)

load_dotenv(os.path.dirname(os.path.abspath(__file__)) + "/development/.env")

namespace = Collection("forward_netbox")
namespace.configure(
    {
        "forward_netbox": {
            "netbox_ver": os.environ.get("NETBOX_VER", "v4.6.5"),
            "project_name": os.environ.get(
                "FORWARD_NETBOX_DOCKER_PROJECT",
                "forward-netbox",
            ),
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
    ensure_development_secrets()
    build_env = {
        "NETBOX_VER": context.forward_netbox.netbox_ver,
        **kwargs.pop("env", {}),
    }
    if getattr(context.forward_netbox, "isolated_runtime", False):
        # Never let a host bind-path override leak into an alternate project.
        # The declared volume is namespaced by the Compose project.
        build_env["FORWARD_NETBOX_POSTGRES_DATA_PATH"] = "netbox-postgres-data"
    compose_command_tokens = [
        "docker compose",
        f"--project-name {context.forward_netbox.project_name}",
        f'--project-directory "{context.forward_netbox.compose_dir}"',
        command,
    ]
    compose_command = " ".join(compose_command_tokens)
    return context.run(compose_command, env=build_env, **kwargs)


def _compose_project_context(context, project_name):
    requested_project = str(project_name or "").strip()
    shared_project = str(context.forward_netbox.project_name or "").strip()
    if not requested_project or requested_project == shared_project:
        raise Exit(
            "An isolated Compose project name must be non-empty and different "
            "from the shared runtime project.",
            code=2,
        )
    return SimpleNamespace(
        run=context.run,
        forward_netbox=SimpleNamespace(
            netbox_ver=context.forward_netbox.netbox_ver,
            project_name=requested_project,
            compose_dir=context.forward_netbox.compose_dir,
            isolated_runtime=True,
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


def _truthy_arg(value):
    if isinstance(value, bool):
        return value
    return _truthy_env(value)


def _shared_runtime_test_guard_bypassed():
    return _truthy_env(os.environ.get(ALLOW_SHARED_RUNTIME_TESTS_ENV))


def _shared_runtime_active_syncs(context):
    statuses = json.dumps(list(ACTIVE_SYNC_STATUSES))
    python_code = (
        "import json; "
        "from forward_netbox.models import ForwardSync; "
        f"statuses={statuses}; "
        "qs=ForwardSync.objects.filter(status__in=statuses).order_by('-id'); "
        "print(json.dumps({"
        '"active_count": qs.count(), '
        '"syncs": list(qs.values("id", "name", "status")[:5])'
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
            "syncs": [],
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
            "syncs": [],
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
        "syncs": [],
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
    active = _shared_runtime_active_syncs(context)
    if active.get("guard_available") is False:
        raise Exit(
            (
                "Could not inspect the shared local NetBox runtime for active "
                "Forward syncs. Run tests with `invoke test-isolated`, "
                "fix the shared runtime, or set "
                f"{ALLOW_SHARED_RUNTIME_TESTS_ENV}=1 to bypass intentionally. "
                f"Reason: {active.get('reason') or 'unknown'}."
            ),
            code=2,
        )
    active_count = int(active.get("active_count") or 0)
    if active_count <= 0:
        return
    syncs = active.get("syncs") or []
    examples = ", ".join(
        f"sync {item.get('id')} {item.get('name') or ''} {item.get('status')}"
        for item in syncs
    )
    raise Exit(
        (
            "Active Forward sync(s) detected in the shared local NetBox "
            "runtime. Running Django tests against this runtime can move live RQ "
            "jobs to failed/abandoned state. Stop or finish the ingestion, run "
            "tests in an isolated stack, or set "
            f"{ALLOW_SHARED_RUNTIME_TESTS_ENV}=1 to bypass intentionally. "
            f"Detected {active_count} active sync(s)"
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
    test_env=None,
):
    isolated = _compose_project_context(context, project_name)
    docker_compose(isolated, "down --remove-orphans -v")
    docker_compose(isolated, "build netbox netbox-worker")
    docker_compose(isolated, "up -d postgres redis")
    try:
        _wait_for_isolated_postgres(isolated)
        isolated_test_env = {
            **(test_env or {}),
            "REDIS_DATABASE": ISOLATED_REDIS_DATABASE,
            "REDIS_CACHE_DATABASE": ISOLATED_REDIS_CACHE_DATABASE,
        }
        environment_prefix = " ".join(
            f"{key}={shlex.quote(str(value))}"
            for key, value in sorted(isolated_test_env.items())
        )
        test_command = (
            "cd /opt/netbox/netbox && "
            + (f"{environment_prefix} " if environment_prefix else "")
            + f"python manage.py test --keepdb --noinput {test_label}"
        )
        docker_compose(
            isolated,
            ("run --rm -T netbox bash -lc " f"{shlex.quote(test_command)}"),
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


def _run_ci_tests_in_isolated_runtime(context, *, test_label):
    if _shared_runtime_test_guard_bypassed():
        manage_py(context, f"test --keepdb --noinput {test_label}")
        return

    print("Running Django CI tests in an isolated runtime.")
    _run_tests_in_isolated_runtime(
        context,
        test_label=test_label,
        project_name=f"{ISOLATED_TEST_PROJECT_NAME}-ci",
        keep_runtime=False,
    )


def _run_playwright_ui(context, *, env=None):
    playwright_env = {**(env or {})}
    playwright_env.setdefault(
        "PLAYWRIGHT_DOCKER_PROJECT_NAME",
        context.forward_netbox.project_name,
    )
    playwright_env.setdefault(
        "PLAYWRIGHT_DOCKER_PROJECT_DIRECTORY",
        context.forward_netbox.compose_dir,
    )
    if not (
        playwright_env.get("PLAYWRIGHT_EXECUTABLE_PATH")
        or os.environ.get("PLAYWRIGHT_EXECUTABLE_PATH")
    ):
        for candidate in (
            "/usr/bin/chromium",
            "/usr/bin/chromium-browser",
            "/usr/bin/google-chrome",
        ):
            if Path(candidate).is_file():
                playwright_env["PLAYWRIGHT_EXECUTABLE_PATH"] = candidate
                break
    context.run("npm run test:ui", env=playwright_env)


def _available_loopback_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return str(listener.getsockname()[1])


def _run_playwright_in_isolated_runtime(context, *, project_name=None, host_port=None):
    project_name = str(project_name or ISOLATED_PLAYWRIGHT_PROJECT_NAME)
    host_port = str(host_port or os.environ.get("FORWARD_NETBOX_PLAYWRIGHT_HOST_PORT"))
    if not host_port or host_port.lower() == "none":
        host_port = _available_loopback_port()
    isolated = _compose_project_context(context, project_name)
    compose_env = {"FORWARD_NETBOX_HOST_PORT": host_port}
    docker_compose(isolated, "down --remove-orphans -v", env=compose_env)
    try:
        docker_compose(
            isolated,
            "up -d --build --wait --wait-timeout 600 netbox",
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

    active = _shared_runtime_active_syncs(context)
    if active.get("guard_available") is False:
        print(
            "Shared runtime active-sync guard unavailable; "
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
        "Active syncs detected in shared runtime; "
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
        f"{shlex.quote(sys.executable)} scripts/check_sensitive_content.py --protected-history"
    )


@task(name="harness-check")
def harness_check(context):
    context.run(f"{shlex.quote(sys.executable)} scripts/check_harness.py")


@task(name="release-authorization-check")
def release_authorization_check(context, version="2.6.0"):
    context.run(
        f"{shlex.quote(sys.executable)} scripts/check_release_authorization.py "
        f"--version {shlex.quote(str(version))}"
    )


@task(
    help={
        "version": "Target version, e.g. 1.5.11",
        "summary": "One-line release summary for the compatibility tables",
        "write": "Write the prepare edits and run the local CI mirror",
        "publish": "Branch + push (rollout). Off by default.",
        "finish": "After CI is green: promote, tag, and publish (rollout)",
    }
)
def release(
    context,
    version,
    summary="",
    write=False,
    publish=False,
    finish=False,
):
    """Run the release flow (scripts/release.py). Default is prepare + verify;
    rollout only happens with --publish/--finish."""
    args = [shlex.quote(sys.executable), "scripts/release.py", shlex.quote(version)]
    if summary:
        args += ["--summary", shlex.quote(summary)]
    if write:
        args.append("--write")
    if publish:
        args.append("--publish")
    if finish:
        args.append("--finish")
    context.run(" ".join(args))


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
        f"test --keepdb --noinput {SCENARIO_TEST_LABELS}",
    )


@task(name="ingestion-delete-regression")
def ingestion_delete_regression(context):
    _guard_shared_runtime_tests(context)
    manage_py(
        context,
        f"test --keepdb --noinput {INGESTION_DELETE_REGRESSION_LABELS}",
    )


@task(name="test-ci")
def test_ci(context):
    _run_ci_tests_in_isolated_runtime(
        context,
        test_label="forward_netbox.tests",
    )


@task(name="scenario-test-ci")
def scenario_test_ci(context):
    _run_ci_tests_in_isolated_runtime(
        context,
        test_label=SCENARIO_TEST_LABELS,
    )


@task(name="bulk-merge-retry-scale-test")
def bulk_merge_retry_scale_test(context):
    """Enforce the 1M-row crash-resume timeout projection on 20,005 rows."""
    _run_tests_in_isolated_runtime(
        context,
        test_label=(
            "forward_netbox.tests.test_bulk_merge_scale.BulkMergeScaleTest."
            "test_scale_merge_no_silent_loss_and_idempotent"
        ),
        project_name=f"{ISOLATED_TEST_PROJECT_NAME}-retry-scale",
        keep_runtime=False,
        test_env={
            "FORWARD_SCALE_TEST": "1",
            "FORWARD_SCALE_TEST_ROWS": "20000",
        },
    )


@task(name="ingestion-delete-regression-ci")
def ingestion_delete_regression_ci(context):
    _run_ci_tests_in_isolated_runtime(
        context,
        test_label=INGESTION_DELETE_REGRESSION_LABELS,
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
        "status": worker_status,
    }


def _runtime_capacity_storage(context):
    docker_root = _docker_root_dir(context)
    postgres_mount = _postgres_data_mount(context)
    storage = {
        "docker_root_dir": docker_root,
        "postgres_data_source": postgres_mount.get("source", ""),
        "postgres_data_type": postgres_mount.get("type", ""),
        "postgres_data_destination": postgres_mount.get("destination", ""),
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


@task(name="playwright-test")
def playwright_test(context):
    _run_playwright_with_shared_runtime_fallback(context)


@task
def package(context):
    context.run(
        f"{shlex.quote(sys.executable)} scripts/build_reproducible_distribution.py"
    )


def _release_artifact_inputs():
    with (REPO_ROOT / "pyproject.toml").open("rb") as pyproject:
        version = tomllib.load(pyproject)["tool"]["poetry"]["version"]
    wheels = sorted((REPO_ROOT / "dist").glob(f"forward_netbox-{version}-*.whl"))
    if len(wheels) != 1:
        raise Exit(
            "Expected exactly one wheel for the current package version; "
            "run `invoke package` first.",
            code=2,
        )
    return version, wheels[0]


def _prepare_sbom_output(version):
    sbom_dir = REPO_ROOT / "sbom"
    sbom_dir.mkdir(exist_ok=True)
    sbom_path = sbom_dir / f"forward-netbox-{version}-runtime.cdx.json"
    sbom_path.unlink(missing_ok=True)
    return sbom_path


@task(name="artifact-test")
def artifact_test(context):
    """Install the built wheel into the exact runtime and validate it."""
    version, wheel = _release_artifact_inputs()
    sbom_path = _prepare_sbom_output(version)
    netbox_version = str(context.forward_netbox.netbox_ver or "").strip()
    if netbox_version != "v4.6.5":
        raise Exit(
            "Release artifact validation requires NETBOX_VER=v4.6.5.",
            code=2,
        )

    image_tag = f"forward-netbox-artifact:{version}"
    package_path = f"/source/dist/{wheel.name}"
    netbox_package_version = netbox_version.removeprefix("v")
    context.run(
        " ".join(
            (
                "docker build",
                f"--file {shlex.quote(str(REPO_ROOT / 'development/Dockerfile'))}",
                f"--build-arg NETBOX_VER={shlex.quote(netbox_version)}",
                f"--build-arg PACKAGE={shlex.quote(package_path)}",
                f"--tag {shlex.quote(image_tag)}",
                shlex.quote(str(REPO_ROOT)),
            )
        )
    )

    artifact_context = _compose_project_context(
        context,
        RELEASE_ARTIFACT_PROJECT_NAME,
    )
    validation_script = "\n".join(
        (
            "set -eu",
            "rm -rf /source/forward_netbox",
            "python /source/scripts/validate_installed_artifact.py "
            f"--expected-version {shlex.quote(version)}",
            "python - <<'PY'",
            "import socket",
            "import time",
            "for host, port in (('postgres', 5432), ('redis', 6379)):",
            "    for attempt in range(30):",
            "        try:",
            "            with socket.create_connection((host, port), timeout=1):",
            "                break",
            "        except OSError:",
            "            if attempt == 29:",
            "                raise",
            "            time.sleep(1)",
            "PY",
            "python manage.py migrate --noinput",
            "python manage.py check",
            "python manage.py makemigrations --check --dry-run forward_netbox",
        )
    )
    run_command = " ".join(
        (
            "docker run --rm",
            f"--network {shlex.quote(RELEASE_ARTIFACT_PROJECT_NAME + '_default')}",
            f"--env-file {shlex.quote(str(REPO_ROOT / 'development/env/netbox.env'))}",
            "--volume "
            + shlex.quote(
                f"{REPO_ROOT / 'development/secrets/api_token_pepper_1'}:"
                "/run/secrets/api_token_pepper_1:ro"
            ),
            "--volume "
            + shlex.quote(
                f"{REPO_ROOT / 'development/secrets/db_password'}:"
                "/run/secrets/db_password:ro"
            ),
            "--volume "
            + shlex.quote(
                f"{REPO_ROOT / 'development/secrets/redis_password'}:"
                "/run/secrets/redis_password:ro"
            ),
            "--volume "
            + shlex.quote(
                f"{REPO_ROOT / 'development/secrets/redis_password'}:"
                "/run/secrets/redis_cache_password:ro"
            ),
            "--volume "
            + shlex.quote(
                f"{REPO_ROOT / 'development/secrets/secret_key'}:"
                "/run/secrets/secret_key:ro"
            ),
            "--env LOGLEVEL=WARNING",
            "--volume "
            + shlex.quote(
                f"{REPO_ROOT / 'development/configuration'}:/etc/netbox/config:ro"
            ),
            "--tmpfs /var/log/netbox:rw,mode=1777",
            "--entrypoint /bin/bash",
            shlex.quote(image_tag),
            f"-lc {shlex.quote(validation_script)}",
        )
    )
    sbom_script = "\n".join(
        (
            "set -eu",
            "printf '%s\\n' '[project]' 'name = \"netbox\"' "
            f"'version = \"{netbox_package_version}\"' "
            "'description = \"NetBox runtime host for Forward NetBox\"' "
            "'requires-python = \">=3.14,<3.15\"' "
            "'dependencies = [' "
            f"'  \"forward-netbox=={version}\",' "
            "'  \"netbox-cisco-aci==0.4.0\",' "
            "'  \"netbox-dlm==0.4.1\",' "
            "'  \"netbox-peering-manager==0.3.0\",' "
            "'  \"netbox-routing==0.4.3\",' "
            "']' "
            "> /tmp/netbox-runtime-pyproject.toml",
            "UV_CACHE_DIR=/tmp/uv-cache uv tool run --isolated "
            f"--from cyclonedx-bom=={CYCLONEDX_BOM_VERSION} "
            "cyclonedx-py environment "
            "--pyproject /tmp/netbox-runtime-pyproject.toml "
            "--output-reproducible --output-format JSON --spec-version 1.6 "
            f"--output-file /sbom/{shlex.quote(sbom_path.name)} "
            "/opt/netbox/venv/bin/python",
        )
    )
    sbom_command = " ".join(
        (
            "docker run --rm",
            f"--user {os.getuid()}:{os.getgid()}",
            f"--volume {shlex.quote(str(sbom_path.parent))}:/sbom",
            "--entrypoint /bin/bash",
            shlex.quote(image_tag),
            f"-lc {shlex.quote(sbom_script)}",
        )
    )

    try:
        docker_compose(artifact_context, "up -d postgres redis")
        context.run(run_command)
        context.run(sbom_command)
        context.run(
            f"{shlex.quote(sys.executable)} "
            f"{shlex.quote(str(REPO_ROOT / 'scripts/validate_sbom.py'))} "
            f"--sbom {shlex.quote(str(sbom_path))} "
            f"--expected-version {shlex.quote(version)}"
        )
    finally:
        docker_compose(
            artifact_context,
            "down --volumes --remove-orphans",
            warn=True,
        )
        context.run(f"docker image rm {shlex.quote(image_tag)}", warn=True)


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
    max_changes_per_staging_item=10000,
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
    if query_limit != 5:
        flags.append(f"--query-limit {int(query_limit)}")
    if max_changes_per_staging_item != 10000:
        flags.append(
            f"--max-changes-per-staging-item {int(max_changes_per_staging_item)}"
        )
    flag_string = f" {' '.join(flags)}" if flags else ""
    manage_py(context, f"forward_smoke_sync{flag_string}")


@task(name="scale-soak")
def scale_soak(
    context,
    runs=3,
    max_changes_per_staging_item=10000,
    pause_seconds=30,
):
    run_count = int(runs)
    if run_count < 1:
        raise Exit("`--runs` must be at least 1.", code=2)
    for index in range(run_count):
        smoke_sync(
            context,
            max_changes_per_staging_item=int(max_changes_per_staging_item),
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


@task(name="architecture-audit-check")
def architecture_audit_check(context):
    """Run the focused model, fetch, and query architecture contract gate."""
    _run_ci_tests_in_isolated_runtime(
        context,
        test_label=ARCHITECTURE_AUDIT_TEST_LABELS,
    )


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
    evidence = _collect_architecture_completion_gate(context)
    rendered = json.dumps(evidence, indent=2, sort_keys=True)
    print(rendered)
    if output_json:
        output_path = Path(str(output_json).strip())
        if not output_path.is_absolute():
            output_path = Path(__file__).resolve().parent / output_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered + "\n", encoding="utf-8")
        print(f"Wrote architecture completion audit: {output_path}")
    if evidence.get("status") != "passed":
        raise Exit("Architecture completion audit failed.", code=1)


def _collect_architecture_completion_gate(context):
    try:
        _run_ci_tests_in_isolated_runtime(
            context,
            test_label=ARCHITECTURE_AUDIT_TEST_LABELS,
        )
    except Exception as exc:
        return {
            "status": "failed",
            "evidence": {
                "command": "architecture-audit-check",
                "failure_code": "architecture_contract_failed",
                "failure_hint": f"architecture contract tests raised {exc.__class__.__name__}",
            },
        }
    return {
        "status": "passed",
        "evidence": {
            "command": "architecture-audit-check",
            "test_labels": ARCHITECTURE_AUDIT_TEST_LABELS,
        },
    }


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
    for poll in range(1, int(max_polls) + 1):
        watch_payload = _manage_py_json_retry(
            context, f"forward_watch_sync {' '.join(watch_flags)}"
        )
        resolved_sync_id = int(watch_payload.get("sync_id") or 0)
        sync_status = str(watch_payload.get("sync_status") or "").strip().lower()

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

        if sync_status in {"failed", "timeout"}:
            failed_streak += 1
            if failed_streak >= failed_threshold:
                raise Exit(
                    f"sync-health-gate failed: sync reached {sync_status} status.",
                    code=3,
                )
            if poll < int(max_polls):
                time.sleep(max(1, int(interval_seconds)))
                continue
        else:
            failed_streak = 0

        if sync_status == "completed":
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
            blocking = int(((blocker_payload.get("counts") or {}).get("blocking") or 0))
            warnings = int(warning_payload.get("warning_count") or 0)
            suppressed_warnings = int(
                warning_payload.get("suppressed_warning_count") or 0
            )
            errors = int(warning_payload.get("error_count") or 0)

            print(
                "sync-health-monitor poll "
                f"{poll}/{int(max_polls)} sync_id={sync_id} status={sync_status} "
                f"blocking={blocking} warnings={warnings} "
                f"suppressed_warnings={suppressed_warnings} errors={errors}"
            )

            sample = {
                "sampled_at": sampled_at,
                "poll": poll,
                "sync_id": sync_id,
                "sync_status": sync_status,
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

            if sync_status in {"failed", "timeout"}:
                failed_streak[sync_id] += 1
                if failed_streak[sync_id] >= failed_threshold:
                    raise Exit(
                        (
                            "sync-health-monitor failed: sync "
                            f"{sync_id} reached failed status threshold "
                            f"(sync_status={sync_status})."
                        ),
                        code=3,
                    )
            else:
                failed_streak[sync_id] = 0

            if sync_status != "completed":
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
    health_output = evidence_dir / f"{prefix}-health.json"
    summary_output = evidence_dir / f"{prefix}-summary.json"

    sync_ids_csv = ",".join(str(v) for v in parsed_sync_ids)
    sync_health_monitor.body(
        context,
        sync_ids=sync_ids_csv,
        max_polls=int(max_polls),
        interval_seconds=int(interval_seconds),
        allow_nonterminal=False,
        include_all_ingestions=bool(include_all_ingestions),
        fail_on_suppressed_warning=True,
        output_json=str(health_output),
    )
    ownership_payload = _manage_py_json_retry(context, "forward_ownership_audit")
    if not bool(ownership_payload.get("release_ready")):
        raise Exit(
            "sync-release-gate failed: ownership is inconsistent or branches remain open.",
            code=3,
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
        "health_output": str(health_output),
        "ownership": ownership_payload,
        "sync_results": sync_results,
    }
    summary_output.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")
    print(f"Wrote sync release gate summary: {summary_output}")
    print("sync-release-gate passed: strict release checks are clean.")


@task(
    pre=[
        sensitive_check,
        harness_check,
        harness_test,
        lint,
        build,
        start,
        check,
        scenario_test_ci,
        test_ci,
        bulk_merge_retry_scale_test,
        validation_org_query_audit_ci,
        playwright_test,
        docs,
        package,
    ]
)
def ci(context):
    """Run the local CI-equivalent validation flow."""
