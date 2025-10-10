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
            "format": "{levelname} {message}",
            "style": "{",
        },
    },
    "handlers": {
        "console": {
            "level": LOGLEVEL,
            "class": "logging.StreamHandler",
            "formatter": "simple",
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
        "forward_netbox": {
            "handlers": ["console"],
            "level": LOGLEVEL,
        },
        "netbox_branching": {
            "handlers": ["console"],
            "level": LOGLEVEL,
        },
    },
}
