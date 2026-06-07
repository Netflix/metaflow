"""Per-entry diagnostic records for the artifact-serializer lifecycle."""

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional


class SerializerState(str, Enum):
    KNOWN = "known"
    IMPORTING = "importing"
    CLASS_LOADED = "class_loaded"
    IMPORTING_DEPS = "importing_deps"
    ACTIVE = "active"
    PENDING_ON_IMPORTS = "pending_on_imports"
    BROKEN = "broken"
    DISABLED = "disabled"


@dataclass
class SerializerRecord:
    name: str
    class_path: str
    state: SerializerState = SerializerState.KNOWN
    awaiting_modules: List[str] = field(default_factory=list)
    last_error: Optional[str] = None
    priority: Optional[int] = None
    type: Optional[str] = None
    import_trigger: Optional[str] = None
    dispatch_error_count: int = 0
    # Human-readable identifier for where this serializer came from — e.g.
    # ``"metaflow"`` for the core, the extension's ``package_name`` for one
    # shipped by an extension. Stamped into ``serializer_info["source"]`` at
    # save time so the "no deserializer claimed this artifact" load error can
    # tell the user which extension to install. Serializers that set
    # ``source`` in their own ``serializer_info`` are not overridden.
    source: Optional[str] = None

    def as_dict(self):
        return {
            "name": self.name,
            "class_path": self.class_path,
            "state": self.state.value,
            "awaiting_modules": list(self.awaiting_modules),
            "last_error": self.last_error,
            "priority": self.priority,
            "type": self.type,
            "import_trigger": self.import_trigger,
            "dispatch_error_count": self.dispatch_error_count,
            "source": self.source,
        }


def list_serializer_status():
    """Return a list of per-serializer diagnostic records as dicts.

    One entry per tuple in ``ARTIFACT_SERIALIZERS_DESC`` (post-toggle),
    including entries in ``pending_on_imports``, ``broken``, and
    ``disabled`` states. Used for debugging "why isn't my custom
    serializer active?".
    """
    from .serializer import SerializerStore

    return [rec.as_dict() for rec in SerializerStore._records.values()]
