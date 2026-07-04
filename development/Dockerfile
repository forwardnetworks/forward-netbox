ARG NETBOX_VER=${NETBOX_VER}
ARG ACI_PLUGIN_PACKAGE=${ACI_PLUGIN_PACKAGE}

FROM netboxcommunity/netbox:${NETBOX_VER}

ARG PACKAGE=${PACKAGE:-'-e .'}
ARG ACI_PLUGIN_PACKAGE=${ACI_PLUGIN_PACKAGE}

WORKDIR /source
COPY . /source
COPY development/launch-netbox.sh /opt/netbox/launch-netbox.sh
RUN chmod +x /opt/netbox/launch-netbox.sh

RUN uv pip install ${PACKAGE}
RUN uv pip install netbox-routing==0.4.2 netbox-peering-manager==0.2.2
RUN if [ -n "${ACI_PLUGIN_PACKAGE}" ]; then uv pip install "${ACI_PLUGIN_PACKAGE}"; fi
RUN uv pip install "watchdog[watchmedo]"
RUN uv pip install coverage

WORKDIR /opt/netbox/netbox

CMD [ "/opt/netbox/docker-entrypoint.sh", "/opt/netbox/launch-netbox.sh" ]
