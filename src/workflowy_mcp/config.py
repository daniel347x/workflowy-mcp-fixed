"""Configuration management for WorkFlowy MCP server."""

from __future__ import annotations

import json
import logging
import logging.handlers
import os
import sys
from pathlib import Path
from typing import Any

from .models.config import ServerConfig

# @beacon[
#   id=config-bootstrap@dotenv-gated-load,
#   role=dotenv bootstrap gated by WORKFLOWY_DEV_MODE / WORKFLOWY_LOAD_ENV,
#   slice_labels=nexus--config,nexus-loading-flow,
#   kind=span,
#   show_span=true,
#   comment=Development-only .env loading path that yields to normal environment-driven startup by default,
# ]
# Only load .env in development mode or if explicitly requested
# In production, MCP clients provide environment variables directly
if os.getenv("WORKFLOWY_DEV_MODE") or os.getenv("WORKFLOWY_LOAD_ENV"):
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        # python-dotenv is optional - only needed for development
        pass
# @beacon-close[
#   id=config-bootstrap@dotenv-gated-load,
# ]


_CONFIG_ENV_VAR = "WORKFLOWY_NEXUS_CONFIG"
_CONFIG_CACHE: ServerConfig | None = None
_CONFIG_META: dict[str, Any] = {
    "config_env_var": _CONFIG_ENV_VAR,
    "config_path": None,
    "source": "uninitialized",
    "search_paths": [],
}


def _repo_root() -> Path:
    """Return the package repo root (parent of src/)."""
    return Path(__file__).resolve().parents[2]


def _portable_state_home() -> Path:
    """Return an OS-appropriate per-user state/app-data home."""
    if os.name == "nt":
        base = os.environ.get("LOCALAPPDATA") or os.environ.get("APPDATA")
        if base:
            return Path(base).expanduser()
        return Path.home() / "AppData" / "Local"

    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support"

    xdg_state = os.environ.get("XDG_STATE_HOME")
    if xdg_state:
        return Path(xdg_state).expanduser()

    return Path.home() / ".local" / "state"


def get_runtime_base_dir() -> Path:
    """Return the portable per-user runtime base dir for Workflowy MCP artifacts."""
    base = _portable_state_home() / "workflowy-mcp"
    base.mkdir(parents=True, exist_ok=True)
    return base


def resolve_runtime_dir(configured_path: str | None, *fallback_parts: str) -> Path:
    """Return a configured directory or a portable runtime fallback."""
    raw = str(configured_path or "").strip()
    if raw:
        path = Path(os.path.expandvars(raw)).expanduser()
    else:
        path = get_runtime_base_dir().joinpath(*fallback_parts)

    path.mkdir(parents=True, exist_ok=True)
    return path


def get_runtime_subdir(*parts: str) -> Path:
    """Return a portable runtime subdirectory under the Workflowy MCP base dir."""
    return resolve_runtime_dir(None, *parts)


def get_nexus_runs_base_dir() -> Path:
    """Return the configured or portable default NEXUS runs base directory."""
    try:
        config = get_server_config()
        configured = str(config.paths.nexus_runs_base or "").strip()
    except Exception:
        configured = ""
    return resolve_runtime_dir(configured, "nexus_runs")


def get_cartographer_jobs_dir() -> Path:
    """Return the configured or portable default Cartographer jobs directory."""
    try:
        config = get_server_config()
        configured = str(config.paths.cartographer_jobs_dir or "").strip()
    except Exception:
        configured = ""
    return resolve_runtime_dir(configured, "cartographer_jobs")


def get_cartographer_file_refresh_dir() -> Path:
    """Return the configured or portable default Cartographer file-refresh directory."""
    try:
        config = get_server_config()
        configured = str(config.paths.cartographer_file_refresh_dir or "").strip()
    except Exception:
        configured = ""
    return resolve_runtime_dir(configured, "cartographer_file_refresh")


def get_windsurf_exe_candidates() -> list[str]:
    """Return configured-first, generic fallback WindSurf executable candidates."""
    configured: list[str] = []
    try:
        config = get_server_config()
        configured = [
            str(Path(os.path.expandvars(p)).expanduser())
            for p in (config.paths.windsurf_exe_candidates or [])
            if str(p or "").strip()
        ]
    except Exception:
        configured = []

    local_appdata = os.environ.get("LOCALAPPDATA")
    program_files = os.environ.get("PROGRAMFILES")
    program_files_x86 = os.environ.get("PROGRAMFILES(X86)")
    home = Path.home()

    generic_candidates = [
        str(home / "AppData" / "Local" / "Programs" / "Windsurf" / "Windsurf.exe"),
        str(Path(local_appdata) / "Programs" / "Windsurf" / "Windsurf.exe") if local_appdata else "",
        str(Path(program_files) / "Windsurf" / "Windsurf.exe") if program_files else "",
        str(Path(program_files_x86) / "Windsurf" / "Windsurf.exe") if program_files_x86 else "",
        r"C:\Program Files\Windsurf\Windsurf.exe",
    ]

    deduped: list[str] = []
    seen: set[str] = set()
    for candidate in configured + generic_candidates:
        raw = str(candidate or "").strip()
        if not raw:
            continue
        key = os.path.normcase(raw)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(raw)
    return deduped


# @beacon[
#   id=config-bootstrap@candidate-config-paths,
#   role=_candidate_config_paths,
#   slice_labels=nexus--config,nexus-loading-flow,
#   kind=ast,
#   comment=Build default search order for workflowy_nexus config files when WORKFLOWY_NEXUS_CONFIG is not set,
# ]
def _candidate_config_paths() -> list[Path]:
    """Return default config file candidates in priority order."""
    cwd = Path.cwd()
    repo_root = _repo_root()

    candidates = [
        cwd / "workflowy_nexus.config.local.json",
        cwd / "workflowy_nexus.config.json",
        repo_root / "workflowy_nexus.config.local.json",
        repo_root / "workflowy_nexus.config.json",
    ]

    deduped: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate.resolve()) if candidate.exists() else str(candidate)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)
    return deduped


# @beacon[
#   id=config-bootstrap@resolve-config-path,
#   role=resolve_server_config_path,
#   slice_labels=nexus--config,nexus-loading-flow,
#   kind=ast,
#   comment=Resolve explicit WORKFLOWY_NEXUS_CONFIG path or first discovered default config candidate,
# ]
def resolve_server_config_path() -> Path | None:
    """Resolve the runtime config file path, if any."""
    explicit = (os.getenv(_CONFIG_ENV_VAR) or "").strip()
    if explicit:
        return Path(explicit).expanduser().resolve()

    for candidate in _candidate_config_paths():
        if candidate.is_file():
            return candidate.resolve()

    return None


# @beacon[
#   id=config-bootstrap@load-json-config-file,
#   role=_load_json_config_file,
#   slice_labels=nexus--config,nexus-loading-flow,
#   kind=ast,
#   comment=Read and validate the external workflowy_nexus JSON config file before model validation,
# ]
def _load_json_config_file(path: Path) -> dict[str, Any]:
    """Load a JSON config file and require a top-level object."""
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise ValueError(f"Config file must contain a top-level JSON object: {path}")
    return payload


def _parse_env_bool(raw: str) -> bool:
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _set_nested_value(container: dict[str, Any], path: tuple[str, ...], value: Any) -> None:
    cur = container
    for key in path[:-1]:
        nxt = cur.get(key)
        if not isinstance(nxt, dict):
            nxt = {}
            cur[key] = nxt
        cur = nxt
    cur[path[-1]] = value


# @beacon[
#   id=config-bootstrap@merge-env-overrides,
#   role=_merge_env_overrides,
#   slice_labels=nexus--config,nexus-loading-flow,
#   kind=ast,
#   comment=Apply environment-variable precedence over JSON-file values when building the resolved ServerConfig,
# ]
def _merge_env_overrides(config_data: dict[str, Any]) -> dict[str, Any]:
    """Apply environment-variable overrides on top of file/default config data."""
    merged: dict[str, Any] = dict(config_data or {})

    env_map: list[tuple[str, tuple[str, ...], Any]] = [
        ("WORKFLOWY_API_KEY", ("WORKFLOWY_API_KEY",), str),
        ("WORKFLOWY_API_URL", ("WORKFLOWY_API_URL",), str),
        ("WORKFLOWY_TIMEOUT", ("WORKFLOWY_TIMEOUT",), int),
        ("WORKFLOWY_MAX_RETRIES", ("WORKFLOWY_MAX_RETRIES",), int),
        ("DEBUG", ("DEBUG",), _parse_env_bool),
        ("LOG_LEVEL", ("LOG_LEVEL",), str),
        ("NEXUS_RUNS_BASE", ("paths", "nexusRunsBase"), str),
        ("CARTOGRAPHER_JOBS_DIR", ("paths", "cartographerJobsDir"), str),
        ("CARTOGRAPHER_FILE_REFRESH_DIR", ("paths", "cartographerFileRefreshDir"), str),
        ("WORKFLOWY_WS_HOST", ("websocket", "host"), str),
        ("WORKFLOWY_WS_PORT", ("websocket", "port"), int),
        ("WORKFLOWY_WS_RECONNECT_DELAY_MS", ("timing", "websocketReconnectDelayMs"), int),
        ("WORKFLOWY_CARTO_JOBS_POLL_INTERVAL_MS", ("timing", "cartoJobsPollIntervalMs"), int),
        ("WORKFLOWY_CARTO_JOB_MAX_AGE_SECONDS", ("retention", "cartoJobMaxAgeSeconds"), int),
        ("WINDSURF_EXE", ("paths", "windsurfExeCandidates"), lambda raw: [str(raw)]),
    ]

    for env_name, field_path, parser in env_map:
        if env_name not in os.environ:
            continue
        raw_val = os.environ.get(env_name)
        if raw_val is None:
            continue
        value = parser(raw_val)
        _set_nested_value(merged, field_path, value)

    return merged


# @beacon[
#   id=config-bootstrap@load-server-config,
#   role=load_server_config,
#   slice_labels=nexus--config,nexus-loading-flow,
#   kind=ast,
#   comment=Central loader implementing config-file discovery, environment precedence, validation, and cached resolved config state,
# ]
def load_server_config(*, reload: bool = False) -> ServerConfig:
    """Load and validate the resolved server configuration."""
    global _CONFIG_CACHE, _CONFIG_META

    if _CONFIG_CACHE is not None and not reload:
        return _CONFIG_CACHE

    explicit = (os.getenv(_CONFIG_ENV_VAR) or "").strip()
    config_data: dict[str, Any] = {}
    config_path: Path | None = None
    source = "env-and-defaults-only"

    if explicit:
        config_path = Path(explicit).expanduser().resolve()
        if not config_path.is_file():
            raise FileNotFoundError(
                f"{_CONFIG_ENV_VAR} points to a missing config file: {config_path}"
            )
        config_data = _load_json_config_file(config_path)
        source = "explicit-env-path"
    else:
        candidates = _candidate_config_paths()
        for candidate in candidates:
            if candidate.is_file():
                config_path = candidate.resolve()
                config_data = _load_json_config_file(config_path)
                source = "discovered-default-path"
                break

    merged = _merge_env_overrides(config_data)
    config = ServerConfig.model_validate(merged)

    _CONFIG_CACHE = config
    _CONFIG_META = {
        "config_env_var": _CONFIG_ENV_VAR,
        "config_path": str(config_path) if config_path else None,
        "source": source,
        "search_paths": [
            str(Path(explicit).expanduser().resolve())
            if explicit
            else str(path.resolve() if path.exists() else path)
            for path in ([Path(explicit)] if explicit else _candidate_config_paths())
        ],
    }
    return config


# @beacon[
#   id=config-bootstrap@get-server-config,
#   role=get_server_config,
#   slice_labels=nexus--config,nexus-loading-flow,
#   kind=ast,
#   comment=Cached access wrapper for the resolved ServerConfig used by server and worker bootstrap flows,
# ]
def get_server_config(*, reload: bool = False) -> ServerConfig:
    """Return the cached resolved server configuration."""
    return load_server_config(reload=reload)


def get_server_config_meta() -> dict[str, Any]:
    """Return metadata about how the runtime configuration was resolved."""
    return dict(_CONFIG_META)


# @beacon[
#   id=config-bootstrap@setup-logging,
#   role=setup_logging,
#   slice_labels=nexus--config,nexus-loading-flow,
#   kind=ast,
#   comment=Startup logging helper that lazily loads the resolved config when no config is supplied,
# ]
def setup_logging(config: ServerConfig | None = None) -> None:
    """Setup logging configuration.

    Args:
        config: Optional resolved server configuration. If not provided,
            will attempt to load from the standard loader.
    """
    if config is None:
        try:
            config = get_server_config()
        except Exception:
            config = None

    if config is None:
        log_level = os.getenv("LOG_LEVEL", "INFO")
        log_file = os.getenv("LOG_FILE")
    else:
        log_level = config.log_level
        log_file = os.getenv("LOG_FILE")

    level_map = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }

    if isinstance(log_level, str):
        log_level = level_map.get(log_level.upper(), logging.INFO)  # type: ignore[assignment]

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    if log_file:
        try:
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)

            file_handler = logging.handlers.RotatingFileHandler(
                log_file, maxBytes=10 * 1024 * 1024, backupCount=5
            )
            file_handler.setLevel(log_level)
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
        except Exception as e:
            logging.warning(f"Failed to setup file logging: {str(e)}")

    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    logging.info(f"Logging configured at level: {logging.getLevelName(log_level)}")
