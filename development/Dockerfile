ARG NETBOX_VER=v4.7.0

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
# netbox-dlm is the one optional plugin on NetBox 4.7: 0.10.0 raised its
# ceiling to 4.7.99. The other four - netbox-cisco-aci, netbox-peering-manager,
# netbox-routing, netbox-validity - still declare `max_version = "4.6.99"`, and
# NetBox refuses to start with a plugin outside its declared range, so the
# image would not boot. Each returns here the moment its upstream raises its
# ceiling. The 4.6 lane keeps all five on 2.9.x.
RUN uv pip install --constraint ${CONSTRAINTS} netbox-dlm
RUN uv pip install "watchdog[watchmedo]"
RUN uv pip install coverage

WORKDIR /opt/netbox/netbox

CMD [ "/opt/netbox/docker-entrypoint.sh", "/opt/netbox/launch-netbox.sh" ]
