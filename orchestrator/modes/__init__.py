"""Known matchmaking modes; this is deliberately a small explicit registry."""
import os
from . import ranked

REGISTRY = {"default": ranked, "ranked": ranked}


def enabled():
    names = [name.strip() for name in os.getenv("ORCHESTRATOR_MODES", "ranked").split(",") if name.strip()]
    result = {}
    for name in names:
        if name not in REGISTRY:
            raise ValueError("unknown orchestrator mode: %s" % name)
        result[name] = REGISTRY[name]
        if name == "ranked":
            result["default"] = ranked
    return result
