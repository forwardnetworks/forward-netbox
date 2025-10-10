import os
import re
import sys

from dotenv import load_dotenv
from invoke.collection import Collection
from invoke.tasks import task as invoke_task

try:
    import tomllib  # Python 3.11+ Tomlib is included in standard library
except ImportError:
    import toml as tomllib


INIT_FILE = "ipfabric_netbox/__init__.py"


def get_version():
    """Get the version from the pyproject.toml file."""
    # first check if file is in changes directory
    if os.path.exists("changes/pyproject.toml"):
        with open("changes/pyproject.toml", "rb") as f:
            data = tomllib.load(f)
            return data["tool"]["poetry"]["version"]
    else:
        print(
            "No pyproject.toml found in changes directory, checking root directory..."
        )
        with open("pyproject.toml", "rb") as f:
            data = tomllib.load(f)
            return data["tool"]["poetry"]["version"]


load_dotenv(os.path.dirname(os.path.abspath(__file__)) + "/development/.env")

namespace = Collection("ipfabric_netbox")
namespace.configure(
    {
        "ipfabric_netbox": {
            "netbox_ver": os.environ["NETBOX_VER"],
            "ipfabric_ver": os.environ["IPFABRIC_VER"],
            "project_name": "ipfabric-netbox",
            "compose_dir": os.path.join(os.path.dirname(__file__), "development"),
        }
    }
)


def task(function=None, *args, **kwargs):
    """Task decorator to override the default Invoke task decorator and add each task to the invoke namespace."""

    def task_wrapper(function=None):
        """Wrapper around invoke.task to add the task to the namespace as well."""
        if args or kwargs:
            task_func = invoke_task(*args, **kwargs)(function)
        else:
            task_func = invoke_task(function)
        namespace.add_task(task_func)
        return task_func

    if function:
        # The decorator was called with no arguments
        return task_wrapper(function)
    # The decorator was called with arguments
    return task_wrapper


def docker_compose(context, command, **kwargs):
    build_env = {
        "NETBOX_VER": context.ipfabric_netbox.netbox_ver,
        "IPFABRIC_VER": context.ipfabric_netbox.ipfabric_ver,
        **kwargs.pop("env", {}),
    }
    print(build_env)

    compose_command_tokens = [
        "docker compose",
        f"--project-name {context.ipfabric_netbox.project_name}",
        f'--project-directory "{context.ipfabric_netbox.compose_dir}"',
        command,
    ]

    compose_command = " ".join(compose_command_tokens)
    print(f"Executing command: {compose_command}")
    return context.run(compose_command, env=build_env, **kwargs)


@task(
    help={
        "force_rm": "Always remove intermediate containers",
        "cache": "Whether to use Docker's cache when building the image (defaults to enabled)",
    }
)
def build(context, force_rm=False, cache=True):
    """Build the NetBox container"""

    command = "build"

    if not cache:
        command += " --no-cache"
    if force_rm:
        command += " --force-rm"

    print(f"Building NetBox with {context.ipfabric_netbox.netbox_ver}...")
    docker_compose(context, command)


@task
def debug(context):
    """Start the NetBox development environment attached."""
    print("Starting NetBox in debug mode...")
    docker_compose(context, "up")


@task
def start(context):
    """Start the NetBox development environment in detached mode."""
    print("Starting NetBox in detached mode...")
    docker_compose(context, "up -d")


@task
def stop(context):
    """Stop the NetBox development environment."""
    print("Stopping NetBox...")
    docker_compose(context, "down --remove-orphans")


@task
def restart(context):
    """Restart the NetBox development environment."""
    print("Restarting NetBox...")
    docker_compose(context, "restart")


@task
def destroy(context):
    """Destroy the NetBox development environment."""
    print("Destroying NetBox...")
    docker_compose(context, "down -v --remove-orphans")


@task(
    help={
        "follow": "Flag to follow logs (default: False)",
        "tail": "Tail N number of lines (default: all)",
    }
)
def logs(context, follow=False, tail=0):
    """Show NetBox logs."""
    print("Showing NetBox Logs...")
    command = "logs "

    if follow:
        command += "--follow "
    if tail:
        command += f"--tail={tail} "

    docker_compose(context, command)


def run_command(context, command, **kwargs):
    docker_compose_status = "ps --services --filter status=running"

    results = docker_compose(context, docker_compose_status, hide="out")
    print(results.stdout)
    if "netbox" not in results.stdout:
        print("NetBox container is not running. Starting it now...")
        start(context)
    compose_command = f"exec netbox {command}"

    pty = kwargs.pop("pty", True)
    warn = kwargs.pop("warn", False)

    # For the test command, we want to capture the exit code properly
    # Set warn=True to prevent invoke from raising an exception on non-zero exit
    result = docker_compose(context, compose_command, warn=warn, pty=pty, **kwargs)

    # If the command failed and we're not in warn mode, exit with the same code
    if result.exited != 0 and not warn:
        import sys

        sys.exit(result.exited)

    return result


@task
def nbshell(context):
    """Start a NetBox shell."""
    print("Starting NetBox shell...")
    run_command(
        context, "/opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py nbshell"
    )


@task
def cli(context):
    """Start a bash shell within the NetBox container."""
    run_command(context, "bash")


@task
def createsuperuser(context):
    """Create admin user."""
    run_command(
        context,
        "/opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py createsuperuser",
    )


@task(
    help={
        "name": "name of the migration to be created; if unspecified, will autogenerate a name",
    }
)
def makemigrations(context, name=""):
    """Perform makemigrations operation in Django."""
    command = "/opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py makemigrations ipfabric_netbox"

    if name:
        command += f" --name {name}"

    run_command(context, command)


@task
def migrate(context, app_label="", migration_name=""):
    """Perform migrate operation in Django."""
    command = "/opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py migrate"
    if app_label:
        command += f" {app_label}"
        if migration_name:
            command += f" {migration_name}"

    run_command(context, command)


@task
def showmigrations(context, app_label=""):
    """Show all available migrations."""
    command = "/opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py showmigrations"
    if app_label:
        command += f" {app_label}"

    run_command(context, command)


@task(
    help={
        "clean": "Remove DB created in previous test run.",
    }
)
def test(context, clean=False):
    """Run tests and report on code coverage."""
    # First run the tests and capture exit code
    test_command = """\
        /bin/bash -c \" \
        uv pip install coverage && \
        /opt/netbox/venv/bin/coverage run --source=ipfabric_netbox,/source/ipfabric_netbox /opt/netbox/netbox/manage.py test ipfabric_netbox \
    """
    if not clean:
        test_command += " --keepdb"
    test_command += " --noinput"
    # Disabling parallelization since tracebacks cannot be picked, causing another exception to be raised on failure.
    test_command += " --parallel 1"
    test_command += '"'

    # Run tests and capture the result
    result = run_command(context, test_command, warn=True)
    test_exit_code = result.exited

    # Always generate coverage reports
    coverage_command = """\
        /bin/bash -c \" \
        mv .coverage /source && \
        cd /source && \
        coverage report && \
        coverage xml && \
        coverage html\"
    """
    run_command(context, coverage_command)

    # Exit with the original test exit code
    sys.exit(test_exit_code)


@task
def format(context):
    """Perform code formatting and Linting."""
    print("Running linting...")
    command = "pre-commit run --all-files"
    context.run(command)


@task
def generate_packages(context):
    """Generate Python packages."""
    command = "poetry build"
    context.run(command)


@task
def bump_version_of_netbox_plugin(context):
    """Bump the version of the NetBox plugin in ipfabric_netbox/__init__.py"""
    version = get_version()
    with open(INIT_FILE, "r") as f:
        src = f.read()

    def replacer(match):
        return f'{match.group(1)}"{version}"'

    new_src, n = re.subn(
        r'(^\s*version\s?=\s?)(["\']\S*["\'])',
        replacer,
        src,
        flags=re.MULTILINE,
    )
    if n == 0:
        raise RuntimeError("No version assignment found in __init__.py")
    with open(INIT_FILE, "w") as f:
        f.write(new_src)
    print(f"Bumped version to {version}")


@task()
def serve_docs(context, detached=False):
    """Serve the documentation using mkdocs."""
    command = f"up {'-d ' if detached else ''} docs"

    docker_compose(context, command)
