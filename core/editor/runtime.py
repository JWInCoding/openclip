from __future__ import annotations

import contextlib
import json
import logging
import os
import shutil
import socket
import subprocess
import sys
import time
import webbrowser
from pathlib import Path
from typing import Optional

import requests

logger = logging.getLogger(__name__)

RUNTIME_DIR = Path('.omx/runtime')
RUNTIME_FILE = RUNTIME_DIR / 'editor-service.json'
LOCK_FILE = RUNTIME_DIR / 'editor-service.lock'
HEALTH_PATH = '/healthz'


def _ensure_runtime_dir() -> None:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)


@contextlib.contextmanager
def _runtime_lock():
    _ensure_runtime_dir()
    with LOCK_FILE.open('a+', encoding='utf-8') as handle:
        try:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            yield
        finally:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass


def _is_process_alive(pid: Optional[int]) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _pick_free_port(host: str = '127.0.0.1') -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


def _load_runtime_record() -> dict:
    if not RUNTIME_FILE.exists():
        return {}
    try:
        return json.loads(RUNTIME_FILE.read_text(encoding='utf-8'))
    except Exception:
        return {}


def _save_runtime_record(record: dict) -> None:
    _ensure_runtime_dir()
    RUNTIME_FILE.write_text(json.dumps(record, indent=2), encoding='utf-8')


def _health_url(host: str, port: int) -> str:
    return f'http://{host}:{port}{HEALTH_PATH}'


def _healthy(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        response = requests.get(_health_url(host, port), timeout=timeout)
        return response.ok
    except Exception:
        return False


def _normalized_path(value: str | Path) -> str:
    return str(Path(value).resolve())


def ensure_editor_service(
    project_id: str,
    *,
    projects_root: str | Path = 'processed_videos',
    jobs_dir: str | Path = 'jobs',
    host: str = '127.0.0.1',
    open_browser: bool = False,
) -> str:
    normalized_projects_root = _normalized_path(projects_root)
    normalized_jobs_dir = _normalized_path(jobs_dir)

    with _runtime_lock():
        record = _load_runtime_record()
        port = int(record.get('port') or 0)
        pid = record.get('pid')
        same_runtime = (
            record.get('projects_root') == normalized_projects_root
            and record.get('jobs_dir') == normalized_jobs_dir
        )
        if port and same_runtime and _is_process_alive(pid) and _healthy(host, port):
            url = f'http://{host}:{port}/projects/{project_id}'
            if open_browser:
                webbrowser.open_new_tab(url)
            return url

        last_error = None
        for _ in range(2):
            port = _pick_free_port(host)
            uv_binary = shutil.which('uv')
            if uv_binary:
                cmd = [
                    uv_binary,
                    'run',
                    'python',
                    '-m',
                    'editor_runtime',
                    '--host',
                    host,
                    '--port',
                    str(port),
                    '--projects-root',
                    normalized_projects_root,
                    '--jobs-dir',
                    normalized_jobs_dir,
                ]
            else:
                cmd = [
                    sys.executable,
                    '-m',
                    'editor_runtime',
                    '--host',
                    host,
                    '--port',
                    str(port),
                    '--projects-root',
                    normalized_projects_root,
                    '--jobs-dir',
                    normalized_jobs_dir,
                ]
            process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            record = {
                'pid': process.pid,
                'port': port,
                'host': host,
                'started_at': time.time(),
                'projects_root': normalized_projects_root,
                'jobs_dir': normalized_jobs_dir,
            }
            _save_runtime_record(record)
            for _ in range(50):
                if _healthy(host, port):
                    url = f'http://{host}:{port}/projects/{project_id}'
                    if open_browser:
                        webbrowser.open_new_tab(url)
                    return url
                if process.poll() is not None:
                    break
                time.sleep(0.1)
            last_error = f'editor service failed to start on port {port}'
        raise RuntimeError(last_error or 'editor service failed to start')
