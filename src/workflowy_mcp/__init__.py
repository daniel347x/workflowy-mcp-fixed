# @beacon[
#   id=package-loading@init-reexports,
#   role=package import surface and mcp re-export,
#   slice_labels=nexus-loading-flow,
#   kind=span,
#   show_span=true,
#   comment=Import-time package surface that re-exports mcp and core types; importing workflowy_mcp pulls in server.py here,
# ]
"""WorkFlowy MCP Server - Model Context Protocol server for WorkFlowy API integration."""

__version__ = "0.1.0"

from .client import WorkFlowyClient
from .models import (
    APIConfiguration,
    NodeCreateRequest,
    NodeListRequest,
    NodeUpdateRequest,
    WorkFlowyNode,
)
from .server import mcp

__all__ = [
    "mcp",
    "WorkFlowyNode",
    "NodeCreateRequest",
    "NodeUpdateRequest",
    "NodeListRequest",
    "APIConfiguration",
    "WorkFlowyClient",
]
# @beacon-close[
#   id=package-loading@init-reexports,
# ]
