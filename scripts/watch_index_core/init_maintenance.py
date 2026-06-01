"""Periodic bootstrap/init maintenance for the long-lived watcher."""

from __future__ import annotations

import os
import subprocess
import sys
import threading
from pathlib import Path
from typing import Optional, Sequence

from scripts.workspace_state import _cross_process_lock, _get_global_state_dir

from . import config as watch_config
from .config import LOGGER
from .utils import get_boolean_env

logger = LOGGER

DEFAULT_INTERVAL_MINUTES = 120.0
DEFAULT_COMMAND_TIMEOUT_SECONDS = 1800.0


def _interval_seconds() -> float:
    raw = os.environ.get("WATCH_INIT_MAINTENANCE_INTERVAL_MINUTES")
    if raw is None:
        raw = os.environ.get("INIT_MAINTENANCE_INTERVAL_MINUTES")
    try:
        minutes = float(raw if raw is not None else DEFAULT_INTERVAL_MINUTES)
    except Exception:
        minutes = DEFAULT_INTERVAL_MINUTES
    return max(0.0, minutes * 60.0)


def _command_timeout_seconds() -> float:
    try:
        return max(
            1.0,
            float(
                os.environ.get(
                    "WATCH_INIT_MAINTENANCE_COMMAND_TIMEOUT_SECS",
                    str(DEFAULT_COMMAND_TIMEOUT_SECONDS),
                )
                or DEFAULT_COMMAND_TIMEOUT_SECONDS
            ),
        )
    except Exception:
        return DEFAULT_COMMAND_TIMEOUT_SECONDS


def _script_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _wait_for_qdrant_command(script_root: Path) -> list[str]:
    return [str(script_root / "wait-for-qdrant.sh")]


def _python_script_command(script_root: Path, script_name: str) -> list[str]:
    return [sys.executable or "python", str(script_root / script_name)]


def _maintenance_commands(script_root: Optional[Path] = None) -> list[list[str]]:
    scripts = script_root or _script_root()
    return [
        _wait_for_qdrant_command(scripts),
        _python_script_command(scripts, "create_indexes.py"),
        _python_script_command(scripts, "warm_all_collections.py"),
        _python_script_command(scripts, "health_check.py"),
    ]


def _env_for_subprocess() -> dict[str, str]:
    env = os.environ.copy()
    root = str(Path(__file__).resolve().parents[2])
    existing = env.get("PYTHONPATH")
    env["PYTHONPATH"] = f"{root}{os.pathsep}{existing}" if existing else root
    if str(watch_config.ROOT):
        env.setdefault("WORKSPACE_PATH", str(watch_config.ROOT))
        env.setdefault("WORKDIR", str(watch_config.ROOT))
        env.setdefault("WORK_DIR", str(watch_config.ROOT))
    return env


def _run_command(command: Sequence[str], *, timeout: float, env: dict[str, str]) -> bool:
    label = " ".join(str(part) for part in command)
    logger.info("[init_maintenance] running: %s", label)
    try:
        result = subprocess.run(
            list(command),
            cwd=str(watch_config.ROOT),
            env=env,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        logger.error("[init_maintenance] timed out after %.0fs: %s", timeout, label)
        return False
    except Exception as exc:
        logger.error("[init_maintenance] failed to start %s: %s", label, exc, exc_info=True)
        return False

    if result.returncode == 0:
        logger.info("[init_maintenance] completed: %s", label)
        if result.stdout:
            logger.debug("[init_maintenance] stdout for %s:\n%s", label, result.stdout[-4000:])
        if result.stderr:
            logger.debug("[init_maintenance] stderr for %s:\n%s", label, result.stderr[-4000:])
        return True

    logger.warning(
        "[init_maintenance] command failed rc=%s: %s\nstdout:\n%s\nstderr:\n%s",
        result.returncode,
        label,
        (result.stdout or "")[-4000:],
        (result.stderr or "")[-4000:],
    )
    return False


def run_init_maintenance_once(
    *,
    commands: Optional[Sequence[Sequence[str]]] = None,
    lock_path: Optional[Path] = None,
) -> bool:
    """Run the existing init scripts once under a cross-process lock."""

    timeout = _command_timeout_seconds()
    env = _env_for_subprocess()
    cmd_list = [list(cmd) for cmd in (commands or _maintenance_commands())]
    if not cmd_list:
        return True

    target_lock = lock_path
    if target_lock is None:
        try:
            target_lock = _get_global_state_dir(str(watch_config.ROOT)) / "init_maintenance.lock"
        except Exception:
            target_lock = Path("/tmp/context-engine-init-maintenance.lock")

    with _cross_process_lock(target_lock):
        for command in cmd_list:
            if not _run_command(command, timeout=timeout, env=env):
                return False
    return True


def start_init_maintenance_worker() -> Optional[threading.Event]:
    """Start periodic init maintenance, controlled by watcher env vars."""

    if not get_boolean_env("WATCH_INIT_MAINTENANCE_ENABLED", default=True):
        return None

    interval = _interval_seconds()
    if interval <= 0:
        return None

    run_on_start = get_boolean_env("WATCH_INIT_MAINTENANCE_RUN_ON_START", default=False)
    shutdown_event = threading.Event()

    def _worker() -> None:
        if not run_on_start:
            shutdown_event.wait(timeout=interval)
        while not shutdown_event.is_set():
            try:
                ok = run_init_maintenance_once()
                if ok:
                    logger.info("[init_maintenance] pass completed")
                else:
                    logger.warning("[init_maintenance] pass completed with failures")
            except Exception:
                logger.error("[init_maintenance] unexpected worker error", exc_info=True)
            shutdown_event.wait(timeout=interval)

    thread = threading.Thread(target=_worker, name="init-maintenance", daemon=True)
    thread.start()
    logger.info(
        "[init_maintenance] worker started interval=%.1fm run_on_start=%s",
        interval / 60.0,
        run_on_start,
    )
    return shutdown_event


__all__ = [
    "DEFAULT_INTERVAL_MINUTES",
    "run_init_maintenance_once",
    "start_init_maintenance_worker",
]
