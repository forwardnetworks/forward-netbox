# Validation policies as data: ready-made packs, and export and import of any policy.
#
# A policy exports as plain JSON (rules, and its scope by slug) so it can be kept in
# version control, reviewed and moved between NetBox instances. Importing creates
# the policy and refuses, naming the problem, anything it cannot place: an unknown
# scope entry or a rule with bad parameters creates nothing.
from django.db import transaction

from .choices import ForwardValidationEngineChoices as Engine
from .choices import ForwardValidationSeverityChoices as Severity
from .validation import rule_parameter_problems

# Ready-made rule sets. They use Forward's own intent checks, which every network
# already has, so they need no query published to the organisation.
PACKS = {
    "connectivity": {
        "name": "Connectivity",
        "description": (
            "No device loses reachability and no subnet becomes isolated, judged "
            "by the network's own Forward isolation and reachability checks."
        ),
        "min_score": 80,
        "require_passed_run": True,
        "run_on_submission": True,
        "rules": [
            {
                "name": "Isolation checks",
                "engine": Engine.CHECK,
                "category": "connectivity",
                "severity": Severity.CRITICAL,
                "parameters": {"check_types": ["isolation"]},
            },
            {
                "name": "Reachability checks",
                "engine": Engine.CHECK,
                "category": "connectivity",
                "severity": Severity.CRITICAL,
                "parameters": {"check_types": ["reachability"]},
            },
        ],
    },
    "intent": {
        "name": "Intent and configuration",
        "description": (
            "The network's own existential, predefined and NQE intent checks must "
            "hold on the snapshot."
        ),
        "min_score": 70,
        "require_passed_run": False,
        "run_on_submission": False,
        "rules": [
            {
                "name": "Existential checks",
                "engine": Engine.CHECK,
                "category": "intent",
                "severity": Severity.ERROR,
                "parameters": {"check_types": ["existential"]},
            },
            {
                "name": "Predefined checks",
                "engine": Engine.CHECK,
                "category": "intent",
                "severity": Severity.WARNING,
                "parameters": {"check_types": ["predefined"]},
            },
            {
                "name": "NQE checks",
                "engine": Engine.CHECK,
                "category": "intent",
                "severity": Severity.ERROR,
                "parameters": {"check_types": ["nqe"]},
            },
        ],
    },
}

_SCOPE = (
    ("sites", "dcim.Site", "slug"),
    ("regions", "dcim.Region", "slug"),
    ("site_groups", "dcim.SiteGroup", "slug"),
    ("device_roles", "dcim.DeviceRole", "slug"),
    ("platforms", "dcim.Platform", "slug"),
    ("scope_tags", "extras.Tag", "slug"),
)


def export_policy(policy):
    data = {
        "name": policy.name,
        "description": policy.description,
        "enabled": policy.enabled,
        "min_score": policy.min_score,
        "require_passed_run": policy.require_passed_run,
        "run_on_submission": policy.run_on_submission,
        "scope": {
            key: sorted(getattr(obj, attr) for obj in getattr(policy, key).all())
            for key, _model, attr in _SCOPE
        },
        "rules": [
            {
                "name": rule.name,
                "engine": rule.engine,
                "category": rule.category,
                "severity": rule.severity,
                "enabled": rule.enabled,
                "parameters": rule.parameters,
            }
            for rule in policy.rules.all()
        ],
    }
    return data


def _problems(data):
    from django.apps import apps

    problems = []
    if not isinstance(data, dict):
        return ["The policy must be a JSON object."]
    if not str(data.get("name") or "").strip():
        problems.append("The policy needs a name.")
    score = data.get("min_score", 0)
    if not isinstance(score, int) or not 0 <= score <= 100:
        problems.append("`min_score` must be a whole number from 0 to 100.")
    for index, rule in enumerate(data.get("rules") or []):
        label = (
            f"rule {index + 1} ({rule.get('name', '?')})"
            if isinstance(rule, dict)
            else f"rule {index + 1}"
        )
        if not isinstance(rule, dict) or not str(rule.get("name") or "").strip():
            problems.append(f"{label} needs a name.")
            continue
        if rule.get("engine") not in set(Engine.values()):
            problems.append(f"{label}: unknown engine {rule.get('engine')!r}.")
            continue
        if rule.get("severity", Severity.ERROR) not in set(Severity.values()):
            problems.append(f"{label}: unknown severity {rule.get('severity')!r}.")
        problems += [
            f"{label}: {p}"
            for p in rule_parameter_problems(
                rule["engine"], rule.get("parameters") or {}
            )
        ]
    names = [r.get("name") for r in data.get("rules") or [] if isinstance(r, dict)]
    if len(names) != len(set(names)):
        problems.append("Rule names must be unique within a policy.")
    for key, model, attr in _SCOPE:
        wanted = (data.get("scope") or {}).get(key) or []
        found = set(
            apps.get_model(model)
            .objects.filter(**{f"{attr}__in": wanted})
            .values_list(attr, flat=True)
        )
        for missing in sorted(set(wanted) - found):
            problems.append(f"scope {key}: {missing!r} does not exist here.")
    return problems


def import_policy(data):
    """Create a policy from exported data. Returns ``(policy, [])`` or ``(None, problems)``."""
    from django.apps import apps

    from ..models import ForwardChangeValidationPolicy
    from ..models import ForwardChangeValidationRule

    problems = _problems(data)
    if (
        not problems
        and ForwardChangeValidationPolicy.objects.filter(name=data["name"]).exists()
    ):
        problems.append(f"A policy named {data['name']!r} already exists.")
    if problems:
        return None, problems
    with transaction.atomic():
        policy = ForwardChangeValidationPolicy.objects.create(
            name=data["name"],
            description=data.get("description", ""),
            enabled=data.get("enabled", True),
            min_score=data.get("min_score", 0),
            require_passed_run=data.get("require_passed_run", False),
            run_on_submission=data.get("run_on_submission", False),
        )
        for key, model, attr in _SCOPE:
            wanted = (data.get("scope") or {}).get(key) or []
            if wanted:
                getattr(policy, key).set(
                    apps.get_model(model).objects.filter(**{f"{attr}__in": wanted})
                )
        for rule in data.get("rules") or []:
            ForwardChangeValidationRule.objects.create(
                policy=policy,
                name=rule["name"],
                engine=rule["engine"],
                category=rule.get("category", ""),
                severity=rule.get("severity", Severity.ERROR),
                enabled=rule.get("enabled", True),
                parameters=rule.get("parameters") or {},
            )
    return policy, []


def install_pack(key):
    """Install a ready-made pack as a new policy."""
    pack = PACKS.get(key)
    if pack is None:
        return None, [f"There is no pack called {key!r}."]
    return import_policy(pack)
