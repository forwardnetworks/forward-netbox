import logging

from dcim.models import Site
from netbox.plugins import PluginTemplateExtension

from forward_netbox.models import ForwardSource

logger = logging.getLogger("forward_netbox.template_content")


class SiteTopologyButtons(PluginTemplateExtension):
    models = ["dcim.site"]

    def buttons(self):
        try:
            site = self.context.get("object")
            source = None
            if isinstance(site, Site) and (
                source_id := site.custom_field_data.get("forward_source")
            ):
                source = ForwardSource.objects.filter(id=source_id).first()
            # `source_id` saved in CF might be obsolete, so always fall back to search by site
            source = source or ForwardSource.get_for_site(site).first()
            return self.render(
                "forward_netbox/inc/site_topology_button.html",
                extra_context={"source": source},
            )
        except Exception as e:
            logger.error(f"Could not render topology button: {e}.")
            return "render error"


template_extensions = [SiteTopologyButtons]
