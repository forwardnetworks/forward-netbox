import json
import os
import shlex
import time
from datetime import datetime
from datetime import timezone
from pathlib import Path

from dotenv import load_dotenv
from invoke.collection import Collection
from invoke.exceptions import Exit
from invoke.tasks import task as invoke_task


INIT_FILE = "forward_netbox/__init__.py"

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


def manage_py(context, command, **kwargs):
    return docker_compose(
        context,
        f'exec -T netbox bash -lc "cd /opt/netbox/netbox && python manage.py {command}"',
        **kwargs,
    )


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
    context.run("pre-commit run --all-files")


@task(name="sensitive-check")
def sensitive_check(context):
    context.run("python scripts/check_sensitive_content.py")
    context.run("python scripts/check_sensitive_content.py --all-history")


@task(name="harness-check")
def harness_check(context):
    context.run("python scripts/check_harness.py")


@task(name="harness-test")
def harness_test(context):
    context.run("python -m unittest discover -s scripts/tests -p 'test_*.py'")


@task
def check(context):
    manage_py(context, "check")


@task
def test(context):
    manage_py(context, "test --keepdb --noinput forward_netbox.tests")


@task(name="scenario-test")
def scenario_test(context):
    manage_py(
        context,
        "test --keepdb --noinput forward_netbox.tests.test_synthetic_scenarios",
    )


@task(name="scale-chaos-test")
def scale_chaos_test(context):
    manage_py(
        context,
        (
            "test --keepdb --noinput "
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

    # Ensure worker containers are up before starting a destructive action.
    docker_compose(context, "up -d netbox-worker")
    docker_compose(
        context,
        "ps netbox-worker",
    )

    # Kill one worker container to simulate real process interruption.
    # Scenario-aware readiness is validated before kill when a target sync is set.
    sync_name = os.environ.get("FORWARD_CHAOS_SYNC_NAME", "").strip()
    timeout_seconds = int(os.environ.get("FORWARD_CHAOS_WAIT_SECONDS", "600"))
    poll_seconds = int(os.environ.get("FORWARD_CHAOS_POLL_SECONDS", "5"))
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
    docker_compose(context, "up -d netbox-worker")
    docker_compose(context, "ps netbox-worker")

    # Optional support-bundle capture for run evidence after kill.
    if sync_name:
        output_dir = os.environ.get("FORWARD_CHAOS_OUTPUT_DIR", "").strip()
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
            _export_chaos_bundle(
                context,
                sync_name=sync_name,
                scenario=scenario,
                output_dir=output_dir,
            )


@task(name="architecture-runtime-evidence")
def architecture_runtime_evidence(
    context,
    output_path="docs/03_Plans/evidence/architecture-runtime-evidence.json",
    sync_name="ui-harness-sync",
    run_adp=False,
):
    """Collect runtime evidence artifacts for architecture completion audit."""
    repo_root = Path(__file__).resolve().parent
    evidence_path = repo_root / output_path
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    chaos_dir = repo_root / "docs/03_Plans/evidence/chaos"
    chaos_dir.mkdir(parents=True, exist_ok=True)

    docker_compose(context, "up -d")

    # Ensure synthetic sync exists for local non-customer chaos probes.
    manage_py(context, "forward_seed_ui_harness")

    chaos_scenarios = [
        "stage-before-branch",
        "stage-after-branch",
        "stage-during-apply",
        "merge-during-exec",
    ]
    chaos_results = []
    for scenario in chaos_scenarios:
        env = {
            "FORWARD_CHAOS_OUTPUT_DIR": str(chaos_dir),
            # Probe export works even when readiness is false; we avoid
            # hard waiting for synthetic runs to hit exact readiness edges.
            "FORWARD_CHAOS_SYNC_NAME": "",
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
                f'--scenario "{scenario}" --export-dir "{chaos_dir}"'
            ),
            warn=True,
            hide=True,
        )
        bundle = None
        candidates = sorted(chaos_dir.glob(f"chaos-{scenario}-run-*.json"))
        if candidates:
            bundle = str(candidates[-1].relative_to(repo_root))
        chaos_results.append(
            {
                "scenario": scenario,
                "exit_code": result.exited,
                "ok": result.ok,
                "bundle": bundle,
            }
        )

    chaos_passed = all(item["ok"] for item in chaos_results)
    chaos_evidence = {
        "status": "passed" if chaos_passed else "failed",
        "evidence": {
            "scenarios": chaos_results,
            "output_dir": str(chaos_dir.relative_to(repo_root)),
        },
    }

    adp_status = "not-run"
    adp_evidence = {
        "status": "failed",
        "evidence": (
            "ADP runtime matrix not executed in this local evidence run. "
            "Run with --run-adp=True in an environment with approved ADP credentials "
            "and export artifacts to satisfy this check."
        ),
    }
    if run_adp:
        adp_evidence, adp_status = _run_adp_runtime_matrix(context)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "invoke architecture-runtime-evidence",
        "checks": {
            "destructive_runtime_worker_kill_evidence_verified": chaos_evidence,
            "adp_scale_runtime_matrix_verified": adp_evidence,
        },
        "notes": {
            "adp_status": adp_status,
            "sync_name": sync_name,
        },
    }
    evidence_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(f"Wrote runtime evidence: {evidence_path}")


def _run_adp_runtime_matrix(context):
    required_env = (
        "FORWARD_SMOKE_USERNAME",
        "FORWARD_SMOKE_PASSWORD",
        "FORWARD_SMOKE_NETWORK_ID",
    )
    missing = [name for name in required_env if not os.getenv(name, "").strip()]
    if missing:
        return (
            {
                "status": "failed",
                "evidence": {
                    "reason": "missing_required_environment",
                    "missing": missing,
                    "required": list(required_env),
                },
            },
            "missing-env",
        )

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
    }

    smoke_url = shlex.quote(command_env["FORWARD_SMOKE_URL"])
    smoke_username = shlex.quote(command_env["FORWARD_SMOKE_USERNAME"])
    smoke_password = shlex.quote(command_env["FORWARD_SMOKE_PASSWORD"])
    smoke_network_id = shlex.quote(command_env["FORWARD_SMOKE_NETWORK_ID"])
    smoke_snapshot = shlex.quote(command_env["FORWARD_SMOKE_SNAPSHOT_ID"])
    smoke_source = shlex.quote(command_env["FORWARD_SMOKE_SOURCE_NAME"])
    smoke_sync = shlex.quote(command_env["FORWARD_SMOKE_SYNC_NAME"])

    common_manage_flags = (
        f"--url {smoke_url} "
        f"--username {smoke_username} "
        f"--password {smoke_password} "
        f"--network-id {smoke_network_id} "
        f"--snapshot-id {smoke_snapshot} "
        f"--source-name {smoke_source} "
        f"--sync-name {smoke_sync}"
    )

    matrix = [
        {
            "name": "run_a_branching_validate_only",
            "execute": (
                "forward_smoke_sync --validate-only --query-limit 10 "
                f"{common_manage_flags}"
            ),
            "evidence_command": "forward_smoke_sync --validate-only --query-limit 10",
        },
        {
            "name": "run_b_branching_plan_only",
            "execute": (
                "forward_smoke_sync --plan-only --max-changes-per-branch 10000 "
                f"{common_manage_flags}"
            ),
            "evidence_command": (
                "forward_smoke_sync --plan-only --max-changes-per-branch 10000"
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

    run_results = []
    all_ok = True
    for item in matrix:
        started = time.time()
        result = manage_py(
            context,
            item["execute"],
            env=command_env,
            warn=True,
            hide=True,
        )
        elapsed_ms = int((time.time() - started) * 1000)
        all_ok = all_ok and bool(result.ok)
        run_results.append(
            {
                "name": item["name"],
                "command": item["evidence_command"],
                "ok": bool(result.ok),
                "exit_code": int(result.exited),
                "elapsed_ms": elapsed_ms,
            }
        )

    return (
        {
            "status": "passed" if all_ok else "failed",
            "evidence": {
                "runs": run_results,
                "note": (
                    "Output is intentionally redacted to avoid storing customer "
                    "identifiers. Use job logs and support bundles for deep triage."
                ),
            },
        },
        "completed" if all_ok else "failed",
    )


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
    manage_py(
        context,
        (
            f'forward_chaos_probe --sync-name "{sync_name}" '
            f'--scenario "{scenario}" --export-dir "{output_dir}"'
        ),
    )


@task(name="playwright-test")
def playwright_test(context):
    context.run("npm run test:ui")


@task
def package(context):
    context.run("python -m build")


@task
def docs(context):
    context.run("mkdocs build --strict")


@task(name="smoke-sync")
def smoke_sync(
    context,
    validate_only=False,
    query_limit=5,
    plan_only=False,
    no_auto_merge=False,
    execution_backend="branching",
    max_changes_per_branch=10000,
):
    flags = []
    if validate_only:
        flags.append("--validate-only")
    if plan_only:
        flags.append("--plan-only")
    if no_auto_merge:
        flags.append("--no-auto-merge")
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


@task(name="architecture-completion-audit")
def architecture_completion_audit(context, output_json=""):
    flags = []
    if output_json:
        flags.append(f'--output-json "{output_json}"')
    manage_py(context, f"forward_architecture_completion_audit {' '.join(flags)}")


@task(
    pre=[
        sensitive_check,
        harness_check,
        harness_test,
        architecture_audit_check,
        lint,
        build,
        start,
        check,
        scenario_test,
        scale_chaos_test,
        test,
        playwright_test,
        docs,
        package,
    ]
)
def ci(context):
    """Run the local CI-equivalent validation flow."""
