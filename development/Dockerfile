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
# The five optional plugins are NOT installed on NetBox 4.7: every one of them
# declares `max_version = "4.6.99"`, and NetBox refuses to start with a plugin
# outside its declared range, so the image would not boot. They return here the
# moment an upstream raises its ceiling. The 4.6 lane keeps them on 2.9.x.
RUN uv pip install "watchdog[watchmedo]"
RUN uv pip install coverage

WORKDIR /opt/netbox/netbox

CMD [ "/opt/netbox/docker-entrypoint.sh", "/opt/netbox/launch-netbox.sh" ]
