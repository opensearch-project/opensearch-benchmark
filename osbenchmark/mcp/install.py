# SPDX-License-Identifier: Apache-2.0
"""
Install subcommand: register opensearch-benchmark-mcp with a local MCP
client by merging the appropriate config snippet into the client's
settings file.

Safe by default:
  - Backs up the existing config to <path>.bak-<timestamp> before writing
  - Merges the new server entry (does NOT overwrite other servers)
  - Idempotent (re-running is a no-op if already installed)

Supported clients in Phase 1:
  - claude-desktop (JSON config)
  - claude-code (delegates to `claude mcp add`)
  - cursor (JSON config; path may vary by platform)
  - cline (JSON config inside VS Code globalStorage)

For anything else, use `--print` to get the JSON snippet and paste it
into the client's config yourself.
"""

import json
import platform
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Optional

_SERVER_NAME = "opensearch-benchmark"
_SERVER_COMMAND = "opensearch-benchmark-mcp"


@dataclass
class ClientSpec:
    name: str
    config_path: Callable[[], Optional[Path]]
    handler: Callable[[Path, bool], str]


def run_install(client: str, print_only: bool) -> None:
    if client == "auto":
        installed = _detect_clients()
        if not installed:
            print(
                "No supported MCP client detected. Use --print to get the JSON "
                "snippet, or pass --client=<name> explicitly.",
                file=sys.stderr,
            )
            sys.exit(1)
        for spec in installed:
            _do_install(spec, print_only=print_only)
        return

    spec = _CLIENTS.get(client)
    if spec is None:
        print(f"Unknown client: {client}", file=sys.stderr)
        sys.exit(2)
    _do_install(spec, print_only=print_only)


def _do_install(spec: ClientSpec, print_only: bool) -> None:
    if print_only:
        print(f"\n# {spec.name}")
        print(_snippet_for_human(spec))
        return
    path = spec.config_path()
    if path is None:
        print(
            f"Could not determine config path for {spec.name} on this platform.",
            file=sys.stderr,
        )
        return
    result = spec.handler(path, print_only)
    print(result)


def _snippet_for_human(spec: ClientSpec) -> str:
    """Human-readable snippet describing what to add for a client."""
    if spec.name == "claude-code":
        return f"  claude mcp add {_SERVER_NAME} {_SERVER_COMMAND}"
    snippet = {
        "mcpServers": {
            _SERVER_NAME: {
                "command": _SERVER_COMMAND,
            }
        }
    }
    return json.dumps(snippet, indent=2)


# --- per-client handlers ---------------------------------------------------


def _claude_desktop_config_path() -> Optional[Path]:
    system = platform.system()
    if system == "Darwin":
        return Path("~/Library/Application Support/Claude/claude_desktop_config.json").expanduser()
    if system == "Linux":
        return Path("~/.config/Claude/claude_desktop_config.json").expanduser()
    if system == "Windows":
        return Path("~/AppData/Roaming/Claude/claude_desktop_config.json").expanduser()
    return None


def _claude_desktop_install(path: Path, _print_only: bool) -> str:
    return _merge_json_config(
        path,
        {"command": _SERVER_COMMAND},
        client_label="Claude Desktop",
    )


def _claude_code_config_path() -> Optional[Path]:
    # claude-code uses its own CLI for config; no static path to write.
    return Path.home()


def _claude_code_install(_path: Path, _print_only: bool) -> str:
    if shutil.which("claude") is None:
        return (
            "Claude Code's `claude` CLI is not on PATH. Install Claude Code "
            "first, or run with --print to see what to add manually."
        )
    result = subprocess.run(
        ["claude", "mcp", "add", _SERVER_NAME, _SERVER_COMMAND],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return (
            f"`claude mcp add` failed (exit {result.returncode}): "
            f"{result.stderr.strip() or result.stdout.strip()}"
        )
    return f"Registered with Claude Code: {result.stdout.strip() or 'ok'}"


def _cursor_config_path() -> Optional[Path]:
    # Cursor stores MCP config in ~/.cursor/mcp.json on most platforms.
    candidate = Path("~/.cursor/mcp.json").expanduser()
    return candidate


def _cursor_install(path: Path, _print_only: bool) -> str:
    return _merge_json_config(
        path,
        {"command": _SERVER_COMMAND},
        client_label="Cursor",
    )


def _cline_config_path() -> Optional[Path]:
    # Cline stores its MCP servers JSON inside VS Code globalStorage. The
    # exact path varies by VS Code flavor; this covers the most common
    # ones. Users on a non-standard layout should use --print.
    candidates = [
        Path("~/Library/Application Support/Code/User/globalStorage/saoudrizwan.claude-dev/settings/cline_mcp_settings.json").expanduser(),
        Path("~/.config/Code/User/globalStorage/saoudrizwan.claude-dev/settings/cline_mcp_settings.json").expanduser(),
        Path("~/AppData/Roaming/Code/User/globalStorage/saoudrizwan.claude-dev/settings/cline_mcp_settings.json").expanduser(),
    ]
    for path in candidates:
        if path.parent.exists():
            return path
    return None


def _cline_install(path: Path, _print_only: bool) -> str:
    return _merge_json_config(
        path,
        {"command": _SERVER_COMMAND},
        client_label="Cline",
    )


# --- shared JSON-merge helper ----------------------------------------------


def _merge_json_config(
    path: Path,
    server_entry: dict,
    client_label: str,
) -> str:
    if path.exists():
        backup = path.with_suffix(path.suffix + f".bak-{int(time.time())}")
        shutil.copy2(path, backup)
        try:
            with path.open() as f:
                config = json.load(f)
        except json.JSONDecodeError:
            return (
                f"Existing {client_label} config at {path} is not valid JSON. "
                f"Refusing to overwrite. Fix the file or use --print."
            )
        backup_note = f" (backed up to {backup.name})"
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        config = {}
        backup_note = ""

    servers = config.setdefault("mcpServers", {})
    if servers.get(_SERVER_NAME) == server_entry:
        return f"{client_label}: {_SERVER_NAME} already registered at {path}."
    servers[_SERVER_NAME] = server_entry

    with path.open("w") as f:
        json.dump(config, f, indent=2)
        f.write("\n")
    return (
        f"{client_label}: registered {_SERVER_NAME} in {path}{backup_note}. "
        f"Restart {client_label} to load the new tools."
    )


# --- client registry --------------------------------------------------------


_CLIENTS: Dict[str, ClientSpec] = {
    "claude-desktop": ClientSpec(
        name="claude-desktop",
        config_path=_claude_desktop_config_path,
        handler=_claude_desktop_install,
    ),
    "claude-code": ClientSpec(
        name="claude-code",
        config_path=_claude_code_config_path,
        handler=_claude_code_install,
    ),
    "cursor": ClientSpec(
        name="cursor",
        config_path=_cursor_config_path,
        handler=_cursor_install,
    ),
    "cline": ClientSpec(
        name="cline",
        config_path=_cline_config_path,
        handler=_cline_install,
    ),
}


def _detect_clients() -> list:
    """Return the list of clients we can plausibly install into."""
    found = []
    for spec in _CLIENTS.values():
        if spec.name == "claude-code":
            if shutil.which("claude") is not None:
                found.append(spec)
            continue
        path = spec.config_path()
        if path is None:
            continue
        # Heuristic: if either the file exists or the parent dir exists,
        # treat the client as installed.
        if path.exists() or (path.parent and path.parent.exists()):
            found.append(spec)
    return found
