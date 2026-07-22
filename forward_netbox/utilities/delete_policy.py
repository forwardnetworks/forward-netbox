ACI_MODEL_PREFIX = "netbox_cisco_aci."


def should_suppress_aci_deletes(sync, model_string):
    """Return whether ACI deletes are disabled for this sync."""
    if not model_string.startswith(ACI_MODEL_PREFIX):
        return False
    return not bool((sync.parameters or {}).get("aci_allow_deletes"))
