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

### 2.1 Install from source

The plugin is currently distributed from source. Use `pip` with the GitHub
repository URL (or clone locally and install in editable mode):

```bash
# Activate the NetBox virtual environment
source /opt/netbox/venv/bin/activate

# Install the latest tagged release from GitHub
(venv) $ pip install git+https://github.com/forwardnetworks/forward-netbox.git

# …or clone and install editable for development workflows
(venv) $ git clone https://github.com/forwardnetworks/forward-netbox.git
(venv) $ pip install -e forward-netbox
```

> **Note:** The plugin interacts directly with the Forward Enterprise REST / NQE
> API. No additional Forward SDK packages are required.

To keep the plugin pinned across NetBox upgrades, add the Git requirement to
`/opt/netbox/local_requirements.txt`:

```bash
(venv) $ echo "git+https://github.com/forwardnetworks/forward-netbox.git@main#egg=forward_netbox" >> /opt/netbox/local_requirements.txt
```

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

## 4. Verification

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

## 5. Troubleshooting

### 6.1 Common Issues

| Issue | Possible Cause | Solution |
|-------|----------------|----------|
| Plugin not appearing in NetBox | Plugin not enabled in configuration | Check PLUGINS list in configuration.py |
| Database migration errors | Incompatible NetBox version | Verify NetBox version meets requirements |
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

## 6. Upgrade Procedures

### 6.1 Standard Upgrade

To upgrade the plugin to a newer version:

```bash
source /opt/netbox/venv/bin/activate
(venv) $ pip install --upgrade git+https://github.com/forwardnetworks/forward-netbox.git
(venv) $ cd /opt/netbox/netbox/
(venv) $ python3 manage.py migrate
(venv) $ python3 manage.py collectstatic --no-input
sudo systemctl restart netbox netbox-rq
```

### 6.2 Docker Upgrade

For Docker installations, update your configuration to specify the new version and rebuild:

```bash
docker-compose build --no-cache netbox
docker-compose up -d
```

### 6.3 Upgrading plugin to future releases

These steps will evolve with future releases. Always review the relevant release notes before upgrading.

## 7. Additional Resources

- [NetBox Documentation](https://netboxlabs.com/docs/netbox/)
- [NetBox Plugin documentation](https://netboxlabs.com/docs/netbox/en/stable/plugins/installation/).
