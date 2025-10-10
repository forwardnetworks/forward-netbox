---
description: Comprehensive installation instructions for the Forward Networks NetBox plugin with version-specific details, multiple approaches, and troubleshooting tips.
---

# NetBox Plugin Installation

This guide provides detailed instructions for installing and configuring the Forward Networks NetBox plugin across different environments. The plugin enables seamless integration between Forward Networks's network discovery capabilities and NetBox's infrastructure management platform.

These instructions contain configuration of [netbox-branching](https://docs.netboxlabs.com/netbox-extensions/branching/) plugin since it is a hard requirement for this plugin to work.

## 1. System Requirements

Before installation, ensure:

- You follow requirements for version [compatibility between Forward Networks and NetBox](../index.md#netbox-compatibility)
- You have administrative access to your NetBox instance
- Your NetBox installation meets the minimum version requirements
- You have Python and pip available in your environment
- Your system has sufficient resources for additional plugin operations

## 2. Standard Installation (Bare Metal/VM)

### 2.1 Install via Package Manager

The plugin is available as a Python package on PyPI and can be installed using pip:

```bash
# Activate the NetBox virtual environment
source /opt/netbox/venv/bin/activate

# Install the plugin
(venv) $ pip install forward_netbox
```
> **Note:** Forward Networks API integration features require the Forward Networks SDK. Install it separately based on your organization's distribution process before enabling those capabilities.

In case you want to install a specific version of the plugin, you can specify the version number:
```bash
# Install the plugin with specific version
(venv) $ pip install forward_netbox==$FORWARD_NETBOX_VERSION
```

To ensure the plugin is automatically reinstalled during future NetBox upgrades:

1. Create or edit the `local_requirements.txt` file in the NetBox root directory:

   ```bash
   (venv) $ echo "forward_netbox" >> /opt/netbox/local_requirements.txt
   ```
   In case we defined specific version of the plugin:

   ```bash
   (venv) $ echo "forward_netbox==$FORWARD_NETBOX_VERSION" >> /opt/netbox/local_requirements.txt
   ```

2. This ensures the plugin will be reinstalled whenever NetBox is upgraded using the standard upgrade procedures.

### 2.3 Enable the Plugin in NetBox Configuration

After installing the plugin, enable it in the NetBox configuration:

1. Open the NetBox configuration file:

   ```bash
   (venv) $ nano /opt/netbox/netbox/netbox/configuration.py
   ```

2. Add the plugins to the `PLUGINS` list:

   ```python
   PLUGINS = [
       'forward_netbox',
       # other plugins...
       'netbox_branching',
   ]
   ```

3. Optionally, configure plugin-specific settings in the `PLUGINS_CONFIG` dictionary:

   ```python
   PLUGINS_CONFIG = {
       'forward_netbox': {
           # Plugin-specific settings can be added here
       }
   }
   ```

4. Additionally, configure plugin-specific logging for debugging purposes, for instance:

   ```python
   LOGGING = {
       "version": 1,
       "formatters": {
            "simple": {
                "format": "{levelname} {message}",
                "style": "{",
            },
       },
       "handlers": {
            "console": {
                "level": "DEBUG",
                "class": "logging.StreamHandler",
                "formatter": "simple",
            },
        },
       "loggers": {
            "forward_netbox": {
                "level": "DEBUG",
                "handlers": ["console"],
            },
            "netbox_branching": {
                "level": "DEBUG",
                "handlers": ["console"],
            },
        },
   }
   ```

### 2.4. Configure database router to support branching:

1. Modify your `configuration.py` with the following content. Replace `$ORIGINAL_DATABASE_CONFIG` with your original `DATABASE` configuration dictionary. If you are using other `DATABASE_ROUTERS`, make sure to include them in the list.

   ```python
   from netbox_branching.utilities import DynamicSchemaDict

   # Wrap DATABASES with DynamicSchemaDict for dynamic schema support
   DATABASES = DynamicSchemaDict({
       'default': $ORIGINAL_DATABASE_CONFIG,
   })

   # Employ netbox-branching custom database router
   DATABASE_ROUTERS = [
       'netbox_branching.database.BranchAwareRouter',
   ]
   ```

### 2.5 Apply Database Migrations

Run the following commands to apply database migrations and collect static files:

```bash
# Activate the virtual environment if not already activated
source /opt/netbox/venv/bin/activate

# Navigate to the NetBox directory
(venv) $ cd /opt/netbox/netbox/

# Apply database migrations
(venv) $ python3 manage.py migrate
```

Collect static files within `venv`:

`python3 manage.py collectstatic --no-input`


### 2.6 Restart NetBox Services

Restart the NetBox services to apply all changes:

```bash
# For systemd-based systems
sudo systemctl restart netbox netbox-rq
```

## 3. Docker Installation

### 3.1 Using Docker Compose Override

For NetBox instances running in Docker, follow these steps:

1. Create or modify the `docker-compose.override.yml` file in your NetBox Docker directory:

   ```yaml
   version: '3'
   services:
     netbox:
       build:
         context: .
         dockerfile: Dockerfile
         args:
           - NETBOX_LOCAL_REQUIREMENTS=forward_netbox
   ```

2. Update `configuration.py` as described in the previous section.

3. Rebuild and restart your Docker containers:

   ```bash
   docker-compose build --no-cache netbox
   docker-compose up -d
   ```

### 3.2 Using Plugin Mounts (Alternative Method)

For development or testing purposes, you can mount the plugin directly:

1. Clone the plugin repository:

   ```bash
   git clone https://github.com/forward-networks/forward-netbox.git
   ```

2. Add a volume mount in your `docker-compose.override.yml`:

   ```yaml
   version: '3'
   services:
     netbox:
       volumes:
         - ./forward-netbox:/opt/netbox/netbox/plugins/forward_netbox
   ```

3. Update `configuration.py` as described in the previous section.

4. Rebuild and restart your Docker containers.

## 4. Version-Specific Installations

### 4.1 Installing the Forward Networks SDK

The plugin no longer installs the Forward Networks SDK automatically. When you need SDK-backed functionality, install the version that matches your Forward Networks deployment:

```bash
# Example for Forward Networks 7.0.x
pip install forward==7.0.0
```

Consult Forward Networks support for the correct SDK package name and version that aligns with your platform release.


## 5. Verification

### 5.1 Verify Plugin Installation

To verify the plugin is installed correctly:

1. Log in to the NetBox web interface
2. Navigate to the Plugins menu
3. Confirm "Forward Networks NetBox" appears in the list of installed plugins
4. Check the plugin version matches your expected version

### 5.2 Verify Database Migrations

To verify database migrations were applied successfully:

```bash
source /opt/netbox/venv/bin/activate
(venv) $ cd /opt/netbox/netbox/
(venv) $ python3 manage.py showmigrations forward_netbox
```

All migrations should be marked as applied (with [X]).

## 6. Troubleshooting

### 6.1 Common Issues

| Issue | Possible Cause | Solution |
|-------|----------------|----------|
| Plugin not appearing in NetBox | Plugin not enabled in configuration | Check PLUGINS list in configuration.py |
| Database migration errors | Incompatible NetBox version | Verify NetBox version meets requirements |
| SDK version conflicts | Mismatched Forward Networks and SDK versions | Install with correct version-specific extras |
| Static files not loading | `collectstatic` not run | Run `python manage.py collectstatic` |
| Permission errors | File system permissions | Check permissions on NetBox directories |

### 6.2 Logs and Debugging

To troubleshoot installation issues:

1. Check the NetBox application logs:
   ```bash
   sudo tail -f /var/log/netbox/netbox.log
   ```

2. For Docker installations, check container logs:
   ```bash
   docker-compose logs -f netbox
   ```

3. Enable debug mode in NetBox configuration for more verbose logging:
   ```python
   DEBUG = True
   ```

## 7. Upgrade Procedures

### 7.1 Standard Upgrade

To upgrade the plugin to a newer version:

```bash
source /opt/netbox/venv/bin/activate
(venv) $ pip install --upgrade forward_netbox
(venv) $ cd /opt/netbox/netbox/
(venv) $ python3 manage.py migrate
(venv) $ python3 manage.py collectstatic --no-input
sudo systemctl restart netbox netbox-rq
```

### 7.2 Docker Upgrade

For Docker installations, update your configuration to specify the new version and rebuild:

```bash
docker-compose build --no-cache netbox
docker-compose up -d
```

### 7.3 Upgrading plugin to v4.0+ (NetBox v4.3.0+)

!!! warning
    For a smooth upgrade to v4.0+, we strongly suggest to upgrade to Forward Networks `v4.0.1` before upgrading NetBox to `v4.3.0+`. The preferred version of NetBox for this upgrade is `v4.2.6`, but anything greater than `v4.2.4` and before `v4.3.0` should work.

The plugin now depends on `netbox-branching` plugin and these extra steps are simplified installation instructions of the plugin:

1. Modify your `configuration.py` with the following content. Replace `$ORIGINAL_DATABASE_CONFIG` with your original `DATABASE` configuration dictionary. If you are using other `DATABASE_ROUTERS`, make sure to include them in the list.
    ```python
    from netbox_branching.utilities import DynamicSchemaDict

    # Wrap DATABASES with DynamicSchemaDict for dynamic schema support
    DATABASES = DynamicSchemaDict({
        'default': $ORIGINAL_DATABASE_CONFIG,
    })

    # Employ netbox-branching custom database router
    DATABASE_ROUTERS = [
        'netbox_branching.database.BranchAwareRouter',
    ]

    # Add `netbox-branching` to plugins list (must be last!)
    PLUGINS = [
        # ...
        'netbox_branching',
    ]
    ```

!!! warning
    If you've upgraded NetBox first or run migrations only for NetBox, you'll see the following error when attempting to upgrade plugin:

    ```commandline
    django.db.migrations.exceptions.InvalidBasesError: Cannot resolve bases for [<ModelState: 'forward_netbox.ForwardBranch'>]
    ```

    Follow [Cannot resolve bases for `[<ModelState: 'forward_netbox.ForwardBranch'>]`](FAQ.md#cannot-resolve-bases-for-modelstate-forward_netboxforwardbranch) instructions to resolve this issue.

## 8. Additional Resources

- [NetBox Documentation](https://netboxlabs.com/docs/netbox/)
- [NetBox Plugin documentation](https://netboxlabs.com/docs/netbox/en/stable/plugins/installation/).
