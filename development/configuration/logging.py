from os import environ

# Set LOGLEVEL in netbox.env or docker-compose.override.yml to override the default logging level.
LOGLEVEL = environ.get("LOGLEVEL", "DEBUG")

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {
            "format": "{levelname} {asctime} {module} {process:d} {thread:d} {message}",
            "style": "{",
        },
        "simple": {
            "format": "{levelname} {asctime} {message}",
            "style": "{",
        },
    },
    "handlers": {
        "console": {
            "level": LOGLEVEL,
            "class": "logging.StreamHandler",
            "formatter": "simple",
        },
        "netbox_file": {
            "level": "DEBUG",
            "class": "logging.FileHandler",
            "formatter": "simple",
            "filename": "/var/log/netbox/netbox.log",
        },
        "forward_file": {
            "level": "DEBUG",
            "class": "logging.FileHandler",
            "formatter": "simple",
            "filename": "/var/log/netbox/forward_netbox.log",
        },
    },
    "loggers": {
        "django": {
            "handlers": ["console"],
            "level": LOGLEVEL,
            "propagate": True,
        },
        "django.request": {
            "handlers": ["console"],
            "level": "ERROR",
            "propagate": False,
        },
        "django_auth_ldap": {
            "handlers": ["console"],
            "level": LOGLEVEL,
        },
        "netbox": {
            "handlers": ["console", "netbox_file"],
            "level": LOGLEVEL,
        },
        "netbox_branching": {
            "handlers": ["console"],
            "level": LOGLEVEL,
        },
        "forward_netbox": {
            "handlers": ["console", "forward_file"],
            "level": LOGLEVEL,
        },
    },
}
