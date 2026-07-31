ARG NETBOX_VER=v4.6.6

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
RUN uv pip install --constraint ${CONSTRAINTS} \
    netbox-cisco-aci netbox-dlm netbox-peering-manager netbox-routing
RUN uv pip install "watchdog[watchmedo]"
RUN uv pip install coverage

WORKDIR /opt/netbox/netbox

CMD [ "/opt/netbox/docker-entrypoint.sh", "/opt/netbox/launch-netbox.sh" ]
