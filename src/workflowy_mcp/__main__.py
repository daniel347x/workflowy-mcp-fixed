# @beacon[
#   id=package-loading@main-module-bootstrap,
#   role=__main__.py import-and-run bootstrap,
#   slice_labels=nexus-loading-flow,
#   kind=span,
#   show_span=true,
#   comment=Package entrypoint for `python -m workflowy_mcp`: import the FastMCP app from server.py and run it,
# ]
"""Main entry point for WorkFlowy MCP Server."""

import asyncio

from workflowy_mcp.server import mcp


# @beacon[
#   id=package-loading@main-entrypoint,
#   role=main,
#   slice_labels=nexus-loading-flow,
#   kind=ast,
#   comment=Exact package entrypoint reached by `python -m workflowy_mcp`,
# ]
def main() -> None:
    """Run the MCP server."""
    asyncio.run(mcp.run())  # type: ignore[func-returns-value]


if __name__ == "__main__":
    main()
# @beacon-close[
#   id=package-loading@main-module-bootstrap,
# ]
