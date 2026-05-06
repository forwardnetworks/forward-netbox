#!/bin/bash
exec /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py runserver --insecure 0.0.0.0:8000
