"""WorkFlowy API client implementation - Compatibility wrapper.

This file now delegates to modular components:
- api_client_core.py: CRUD operations, validation, logging
- api_client_etch.py: ETCH and export/import
- api_client_nexus.py: NEXUS pipeline (SCRY, GLIMPSE, ANCHOR, WEAVE)
- api_client_exploration.py: Exploration state machine

For backward compatibility, this wrapper re-exports WorkFlowyClient
as an alias for the fully-featured WorkFlowyClientExploration class.
"""

# Re-export everything from exploration (top of dependency chain)
from .api_client_exploration import (
    WorkFlowyClientExploration as WorkFlowyClient,
    EXPLORATION_ACTION_2LETTER,
)

# Re-export helpers that may be imported elsewhere
from .api_client_core import (
    log_event,
    _log,
    _ClientLogger,
    API_RATE_LIMIT_DELAY,
)

from .api_client_etch import (
    _log_to_file_helper,
    _current_weave_context,
    export_nodes_impl,
    bulk_export_to_file_impl,
)

from .api_client_nexus import (
    is_pid_running,
    scan_active_weaves,
)

# Compatibility aliases (for code that imports from api_client directly)
__all__ = [
    "WorkFlowyClient",
    "EXPLORATION_ACTION_2LETTER",
    "log_event",
    "_log",
    "_ClientLogger",
    "API_RATE_LIMIT_DELAY",
    "_log_to_file_helper",
    "_current_weave_context",
    "export_nodes_impl",
    "bulk_export_to_file_impl",
    "is_pid_running",
    "scan_active_weaves",
]
