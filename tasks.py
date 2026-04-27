import os

from dotenv import load_dotenv
from invoke.collection import Collection
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
    max_changes_per_branch=10000,
):
    flags = []
    if validate_only:
        flags.append("--validate-only")
    if plan_only:
        flags.append("--plan-only")
    if no_auto_merge:
        flags.append("--no-auto-merge")
    if query_limit != 5:
        flags.append(f"--query-limit {int(query_limit)}")
    if max_changes_per_branch != 10000:
        flags.append(f"--max-changes-per-branch {int(max_changes_per_branch)}")
    flag_string = f" {' '.join(flags)}" if flags else ""
    manage_py(context, f"forward_smoke_sync{flag_string}")


@task(
    pre=[
        sensitive_check,
        harness_check,
        harness_test,
        lint,
        build,
        start,
        check,
        scenario_test,
        test,
        playwright_test,
        docs,
        package,
    ]
)
def ci(context):
    """Run the local CI-equivalent validation flow."""
