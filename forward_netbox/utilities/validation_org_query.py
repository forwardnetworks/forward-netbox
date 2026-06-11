from forward_netbox.choices import ForwardSourceDeploymentChoices
from forward_netbox.models import ForwardSource


def build_validation_org_query_source(
    *,
    source_name: str,
    url: str,
    username: str,
    password: str,
    network_id: str,
) -> ForwardSource:
    existing_source = ForwardSource.objects.filter(name=source_name).first()
    existing_parameters = dict(getattr(existing_source, "parameters", {}) or {})
    source_type = (
        existing_source.type
        if existing_source is not None
        else (
            ForwardSourceDeploymentChoices.SAAS
            if (url or "https://fwd.app").rstrip("/") == "https://fwd.app"
            else ForwardSourceDeploymentChoices.CUSTOM
        )
    )
    parameters = dict(existing_parameters)
    parameters.update(
        {
            "username": username,
            "password": password,
            "network_id": network_id,
            "verify": parameters.get("verify", True),
        }
    )
    return ForwardSource(
        name=source_name,
        type=source_type,
        url=(url or getattr(existing_source, "url", "") or "https://fwd.app").rstrip(
            "/"
        ),
        parameters=parameters,
    )
