from netbox.plugins import PluginTemplateExtension


class SiteTopologyButtons(PluginTemplateExtension):
    model = "dcim.site"

    def buttons(self):
        return self.render(
            "forward_netbox/inc/site_topology_button.html", extra_context={}
        )


template_extensions = [SiteTopologyButtons]
