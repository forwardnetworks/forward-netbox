import logging

from dcim.models import Site
from netbox.plugins import PluginTemplateExtension

from ipfabric_netbox.models import IPFabricSource

logger = logging.getLogger("ipfabric_netbox.template_content")


class SiteTopologyButtons(PluginTemplateExtension):
    models = ["dcim.site"]

    def buttons(self):
        try:
            site = self.context.get("object")
            source = None
            if isinstance(site, Site) and (
                source_id := site.custom_field_data.get("ipfabric_source")
            ):
                source = IPFabricSource.objects.filter(id=source_id).first()
            # `source_id` saved in CF might be obsolete, so always fall back to search by site
            source = source or IPFabricSource.get_for_site(site).first()
            return self.render(
                "ipfabric_netbox/inc/site_topology_button.html",
                extra_context={"source": source},
            )
        except Exception as e:
            logger.error(f"Could not render topology button: {e}.")
            return "render error"


template_extensions = [SiteTopologyButtons]
