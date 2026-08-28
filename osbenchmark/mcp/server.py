# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
"""
MCP server entry point. Registers all OSB tools with a FastMCP app and
runs the stdio transport.

Invoked via the `opensearch-benchmark-mcp` console script. MCP clients
launch this as a subprocess and speak JSON-RPC over stdio.
"""

import argparse
import sys

try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    print(
        "The MCP server requires the optional `mcp` extra. "
        "Install with: pip install opensearch-benchmark[mcp]",
        file=sys.stderr,
    )
    raise


_APP_NAME = "opensearch-benchmark"
_VERSION = "0.1.0"


def build_app() -> FastMCP:
    """Construct the FastMCP app with all tools registered."""
    app = FastMCP(_APP_NAME)
    _register_tools(app)
    return app


def _register_tools(app: FastMCP) -> None:
    """Register every OSB tool on the app. One import per tool module."""
    from osbenchmark.mcp.tools import catalog, runs, compare

    catalog.register(app)
    runs.register(app)
    compare.register(app)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="opensearch-benchmark-mcp",
        description=(
            "MCP server exposing OpenSearch Benchmark as typed tools "
            "for AI coding assistants."
        ),
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"opensearch-benchmark-mcp {_VERSION}",
    )

    subparsers = parser.add_subparsers(dest="command")

    install_parser = subparsers.add_parser(
        "install",
        help="Register the MCP server with a local MCP client.",
    )
    install_parser.add_argument(
        "--client",
        choices=["claude-desktop", "claude-code", "cursor", "cline", "auto"],
        default="auto",
        help="Which MCP client to configure (default: auto-detect).",
    )
    install_parser.add_argument(
        "--print",
        dest="print_only",
        action="store_true",
        help="Print the JSON config snippet instead of writing to disk.",
    )

    args = parser.parse_args()

    if args.command == "install":
        from osbenchmark.mcp.install import run_install
        return run_install(client=args.client, print_only=args.print_only)

    # No subcommand: start the MCP server on stdio.
    app = build_app()
    app.run()


if __name__ == "__main__":
    main()
