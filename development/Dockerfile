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

RUN uv pip install --constraint ${CONSTRAINTS} ${PACKAGE}
# Four of the five optional plugins run on NetBox 4.7: netbox-dlm 0.10.0,
# netbox-validity 3.6.0 and netbox-peering-manager 0.3.1 raised their ceilings
# in released versions; netbox-routing's 4.7 support is merged on upstream
# main (0.4.4) but not yet tagged, so it installs from the branch archive until it
# is; constraints.txt cannot pin a version PyPI does not have, so
# validated_runtime.py is what holds the installed version to exactly 0.4.4.
# netbox-cisco-aci 0.4.0 still declares `max_version = "4.6.99"`, and NetBox
# refuses to start with a plugin outside its declared range, so the image would
# not boot with it; it returns here the moment its upstream raises the ceiling.
# The 4.6 lane keeps all five on 2.9.x.
# Upstream main goes first: netbox-peering-manager depends on netbox-routing,
# and resolving that from PyPI would find no 4.7-capable release. It comes as
# GitHub's archive of the branch rather than a git ref because the NetBox
# image ships no git binary for uv to clone with.
RUN uv pip install --constraint ${CONSTRAINTS} "netbox-routing @ https://github.com/DanSheps/netbox-routing/archive/refs/heads/main.tar.gz"
RUN uv pip install --constraint ${CONSTRAINTS} netbox-dlm netbox-validity netbox-peering-manager
RUN uv pip install "watchdog[watchmedo]"
RUN uv pip install coverage

WORKDIR /opt/netbox/netbox

CMD [ "/opt/netbox/docker-entrypoint.sh", "/opt/netbox/launch-netbox.sh" ]
