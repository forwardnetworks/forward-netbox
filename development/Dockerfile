ARG NETBOX_VER=v4.7.1

FROM netboxcommunity/netbox:${NETBOX_VER}

ARG PACKAGE=${PACKAGE:-'-e .'}

# The upgrade gate stands up a *previous* release, whose supported runtime is
# not this one: 2.6.6 pinned netboxlabs-netbox-branching==1.1.1, and pinning
# 1.1.2 over it makes the resolution unsatisfiable rather than merely old. The
# from-side of an upgrade therefore builds against its own constraints file.
ARG CONSTRAINTS=/source/constraints.txt

WORKDIR /source
COPY . /source
COPY development/launch-netbox.sh /opt/netbox/launch-netbox.sh
RUN chmod +x /opt/netbox/launch-netbox.sh

# Dev/test image only. On the Python 3.14.4 in the NetBox image the cyclic
# collector crashes (`Fatal Python error: Segmentation fault`, "Garbage-collecting"
# at the top of the traceback) or corrupts a migration (`'int' object has no
# attribute 'state_forwards'`) while Django renders migration state for
# `manage.py migrate` and the test runner's database setup. It failed release and
# pre-push gates at random, so a retry was the only remedy. Reference counting
# still frees almost everything; only cyclic garbage waits for the process to
# exit. This is a `.pth` file because the image's own `sitecustomize` shadows one
# in site-packages.
COPY development/gc_off_for_migrate.pth /tmp/gc_off_for_migrate.pth
RUN mv /tmp/gc_off_for_migrate.pth "$(/opt/netbox/venv/bin/python -c 'import site; print(site.getsitepackages()[0])')/gc_off_for_migrate.pth"

RUN uv pip install --constraint ${CONSTRAINTS} ${PACKAGE}
# Four of the five optional plugins run on NetBox 4.7, all from released
# versions: netbox-dlm 0.10.0, netbox-validity 3.6.0, netbox-peering-manager
# 0.3.1 and netbox-routing 0.5.0.
# netbox-cisco-aci 0.4.0 still declares `max_version = "4.6.99"`, and NetBox
# refuses to start with a plugin outside its declared range, so the image would
# not boot with it; it returns here the moment its upstream raises the ceiling.
# The 4.6 lane keeps all five on 2.9.x.
RUN uv pip install --constraint ${CONSTRAINTS} netbox-routing netbox-dlm netbox-validity netbox-peering-manager
RUN uv pip install "watchdog[watchmedo]"
RUN uv pip install coverage

WORKDIR /opt/netbox/netbox

CMD [ "/opt/netbox/docker-entrypoint.sh", "/opt/netbox/launch-netbox.sh" ]
