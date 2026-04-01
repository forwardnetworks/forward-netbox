from django import template

register = template.Library()


@register.inclusion_tag(
    "forward_netbox/partials/ingestion_progress.html",
    takes_context=False,
)
def render_progress_card(
    job,
    statistics: dict,
    active_stage: str = "sync",
    merge_job=None,
    merge_disabled=False,
):
    return {
        "job": job,
        "merge_job": merge_job,
        "statistics": statistics or {},
        "merge_disabled": merge_disabled,
        "active_stage": active_stage if active_stage in ("sync", "merge") else "sync",
    }
