# Forward NetBox plugin template content hooks.
from netbox.plugins import PluginTemplateExtension


class ForwardDeviceAnalysisPanel(PluginTemplateExtension):
    """Read-only Forward Analysis panel on the device detail page.

    Renders the most recently refreshed ForwardDeviceAnalysis row for the device
    (see the Refresh device analysis action on the sync). No live Forward call.
    """

    models = ["dcim.device"]

    def right_page(self):
        device = self.context["object"]
        from forward_netbox.models import ForwardDeviceAnalysis

        analysis = (
            ForwardDeviceAnalysis.objects.filter(device_name=device.name)
            .order_by("-updated")
            .first()
        )
        if analysis is None:
            return ""
        return self.render(
            "forward_netbox/inc/device_analysis_panel.html",
            extra_context={"analysis": analysis},
        )


template_extensions = [ForwardDeviceAnalysisPanel]
