ARG NETBOX_VER=${NETBOX_VER}

FROM netboxcommunity/netbox:${NETBOX_VER}

ARG PACKAGE=${PACKAGE:-'-e .'}

WORKDIR /source
COPY . /source
COPY development/launch-netbox.sh /opt/netbox/launch-netbox.sh
RUN chmod +x /opt/netbox/launch-netbox.sh

RUN uv pip install ${PACKAGE}
RUN uv pip install netbox-routing==0.4.2 netbox-peering-manager==0.2.2
RUN uv pip install "watchdog[watchmedo]"

WORKDIR /opt/netbox/netbox

CMD [ "/opt/netbox/docker-entrypoint.sh", "/opt/netbox/launch-netbox.sh" ]
