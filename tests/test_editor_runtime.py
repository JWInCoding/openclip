from pathlib import Path
from types import SimpleNamespace

from core.editor import runtime


def test_ensure_editor_service_reuses_healthy_runtime(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    runtime._ensure_runtime_dir()
    runtime._save_runtime_record({'pid': 123, 'port': 8765, 'host': '127.0.0.1', 'projects_root': str((tmp_path / 'processed_videos').resolve()), 'jobs_dir': str((tmp_path / 'jobs').resolve())})
    monkeypatch.setattr(runtime, '_is_process_alive', lambda pid: True)
    monkeypatch.setattr(runtime, '_healthy', lambda host, port, timeout=0.5: True)
    url = runtime.ensure_editor_service('proj-1', projects_root=tmp_path / 'processed_videos', jobs_dir=tmp_path / 'jobs', open_browser=False)
    assert url == 'http://127.0.0.1:8765/projects/proj-1'


def test_ensure_editor_service_launches_when_missing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    runtime._ensure_runtime_dir()
    monkeypatch.setattr(runtime, '_is_process_alive', lambda pid: False)
    monkeypatch.setattr(runtime, '_pick_free_port', lambda host='127.0.0.1': 9001)
    monkeypatch.setattr(runtime.shutil, 'which', lambda name: '/usr/local/bin/uv' if name == 'uv' else None)

    calls = {'healthy': 0, 'opened': [], 'cmd': None}

    def fake_healthy(host, port, timeout=0.5):
        calls['healthy'] += 1
        return calls['healthy'] >= 2

    monkeypatch.setattr(runtime, '_healthy', fake_healthy)
    monkeypatch.setattr(runtime.webbrowser, 'open_new_tab', lambda url: calls['opened'].append(url))
    monkeypatch.setattr(
        runtime.subprocess,
        'Popen',
        lambda cmd, **kwargs: calls.update({'cmd': cmd}) or SimpleNamespace(pid=456, poll=lambda: None),
    )

    url = runtime.ensure_editor_service('proj-2', projects_root=tmp_path / 'processed_videos', jobs_dir=tmp_path / 'jobs', open_browser=True)
    assert url == 'http://127.0.0.1:9001/projects/proj-2'
    assert calls['opened'] == [url]
    record = runtime._load_runtime_record()
    assert record['pid'] == 456
    assert record['port'] == 9001
