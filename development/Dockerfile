ARG NETBOX_VER=${NETBOX_VER}

FROM netboxcommunity/netbox:${NETBOX_VER}

ARG IPFABRIC_VER=${IPFABRIC_VER}
ARG PACKAGE=${PACKAGE:-'-e .'}

WORKDIR /source
COPY . /source
COPY development/launch-netbox.sh /opt/netbox/launch-netbox.sh
RUN chmod +x /opt/netbox/launch-netbox.sh

RUN uv pip install ${PACKAGE}
# Using `uv pip` ignores `ipfabric` dependency, seems like uv bug
# This works if installed directly using `pip`
RUN uv pip install ipfabric~=${IPFABRIC_VER}
RUN uv pip install "watchdog[watchmedo]"

WORKDIR /opt/netbox/netbox

CMD [ "/opt/netbox/docker-entrypoint.sh", "/opt/netbox/launch-netbox.sh" ]
