import pytest
from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient

from core.editor.manifest import load_manifest, save_manifest, upsert_manifest
from core.editor.service import EditorService, create_app



def _create_project(tmp_path):
    projects_root = tmp_path / "processed_videos"
    project_root = projects_root / "sample-video"
    clips_dir = project_root / "clips"
    post_dir = project_root / "clips_post_processed"
    splits_dir = project_root / "splits"
    clips_dir.mkdir(parents=True)
    post_dir.mkdir(parents=True)
    splits_dir.mkdir(parents=True)

    raw_clip = clips_dir / "rank_01_test_clip.mp4"
    raw_clip.write_bytes(b"raw")
    (clips_dir / "rank_01_test_clip.srt").write_text("1\n00:00:00,000 --> 00:00:01,000\nHello\n", encoding="utf-8")
    (post_dir / raw_clip.name).write_bytes(b"composed")
    cover = project_root / "cover_rank_01_test_clip.jpg"
    cover.write_bytes(b"jpg")
    (project_root / "cover_rank_01_test_clip_vertical.jpg").write_bytes(b"jpg")

    (splits_dir / "sample-video_part01.mp4").write_bytes(b"part")
    (splits_dir / "sample-video_part01.srt").write_text("1\n00:00:10,000 --> 00:00:35,000\nHello\n", encoding="utf-8")

    result = SimpleNamespace(
        video_info={"title": "Sample Video", "duration": 120.0},
        source_video_path=str(tmp_path / "source.mp4"),
        video_path=str(tmp_path / "source.mp4"),
        video_parts=[str(splits_dir / "sample-video_part01.mp4")],
        transcript_parts=[str(splits_dir / "sample-video_part01.srt")],
        part_offsets={"part01": 60.0},
        clip_generation={
            "success": True,
            "output_dir": str(clips_dir),
            "clips_info": [
                {
                    "rank": 1,
                    "title": "Test Clip",
                    "filename": raw_clip.name,
                    "subtitle_filename": "rank_01_test_clip.srt",
                    "duration": 15.0,
                    "video_part": "part01",
                    "time_range": "00:00:10 - 00:00:25",
                    "original_time_range": "00:00:10 - 00:00:25",
                }
            ],
        },
        post_processing={"success": True, "output_dir": str(post_dir)},
        cover_generation={
            "success": True,
            "covers": [{"rank": 1, "title": "Test Clip", "filename": cover.name, "path": str(cover)}],
        },
    )
    manifest_path = upsert_manifest(
        video_root_dir=project_root,
        result=result,
        title_style="gradient_3d",
        title_font_size=40,
        subtitle_translation=None,
        subtitle_style_preset="default",
        subtitle_style_font_size="medium",
        subtitle_style_vertical_position="bottom",
        subtitle_style_bilingual_layout="auto",
        subtitle_style_background_style="none",
        cover_text_location="center",
        cover_fill_color="yellow",
        cover_outline_color="black",
    )
    manifest = load_manifest(manifest_path)
    return manifest, projects_root, tmp_path / "jobs"



def test_editor_service_load_update_and_rerender_contract(tmp_path):
    manifest, projects_root, jobs_dir = _create_project(tmp_path)
    service = EditorService(projects_root=projects_root, jobs_dir=jobs_dir)

    project = service.load_project(manifest.project_id)
    assert project["project_id"] == manifest.project_id
    assert project["active_clip_id"] == manifest.clips[0].clip_id

    clip_id = manifest.clips[0].clip_id
    updated_clip = service.update_clip_bounds(manifest.project_id, clip_id, "00:01:12", "00:01:27")
    assert updated_clip["time_range"] == "00:00:12 - 00:00:27"
    assert updated_clip["absolute_time_range"] == "00:01:12 - 00:01:27"
    assert updated_clip["recovery"]["cover_dirty"] is True

    updated_clip = service.update_clip_subtitles(manifest.project_id, clip_id, "Edited subtitle")
    assert updated_clip["subtitle_recipe"]["override_text"] == "Edited subtitle"

    updated_clip = service.update_cover_title(manifest.project_id, clip_id, "New Cover Title")
    assert updated_clip["cover_recipe"]["text"] == "New Cover Title"

    rerender = service.request_rerender(manifest.project_id, clip_id, "subtitle")
    assert rerender["status"] == "pending"

    job_status = service.get_job_status(rerender["job_id"])
    assert job_status["status"] in {"pending", "processing", "completed", "failed"}
    assert job_status["options"]["clip_id"] == clip_id

    saved_manifest = load_manifest(Path(manifest.project_root) / "editor_project.json")
    saved_clip = saved_manifest.clip_by_id(clip_id)
    assert saved_clip.recovery.pending_job_id == rerender["job_id"]
    assert saved_clip.recovery.pending_operation == "subtitles"



def test_editor_service_fastapi_routes(tmp_path):
    manifest, projects_root, jobs_dir = _create_project(tmp_path)
    app = create_app(projects_root=projects_root, jobs_dir=jobs_dir)
    client = TestClient(app)
    clip_id = manifest.clips[0].clip_id

    health = client.get("/healthz")
    assert health.status_code == 200
    assert health.json() == {"status": "ok"}

    project = client.get(f"/api/projects/{manifest.project_id}")
    assert project.status_code == 200
    assert project.json()["project_id"] == manifest.project_id

    clip = client.get(f"/api/projects/{manifest.project_id}/clips/{clip_id}")
    assert clip.status_code == 200
    assert clip.json()["clip_id"] == clip_id
    assert clip.json()["effective_subtitle_text"] == 'Hello'
    assert clip.json()["absolute_time_range"] == "00:01:10 - 00:01:25"
    assert clip.json()["part_duration_seconds"] == 35.0

    bounds = client.patch(
        f"/api/projects/{manifest.project_id}/clips/{clip_id}/bounds",
        json={"start_time": "00:01:11", "end_time": "00:01:24"},
    )
    assert bounds.status_code == 200
    assert bounds.json()["time_range"] == "00:00:11 - 00:00:24"
    assert bounds.json()["absolute_time_range"] == "00:01:11 - 00:01:24"

    rerender = client.post(f"/api/projects/{manifest.project_id}/clips/{clip_id}/rerender/boundary")
    assert rerender.status_code == 200
    assert rerender.json()["operation"] == "boundary"


def test_cover_title_update_does_not_mutate_title_overlay_recipe(tmp_path):
    manifest, projects_root, jobs_dir = _create_project(tmp_path)
    service = EditorService(projects_root=projects_root, jobs_dir=jobs_dir)
    clip_id = manifest.clips[0].clip_id

    updated = service.update_cover_title(manifest.project_id, clip_id, 'Cover Only Title')

    assert updated['cover_recipe']['text'] == 'Cover Only Title'
    assert updated['title_recipe']['text'] == 'Test Clip'
    assert updated['title'] == 'Test Clip'


def test_subtitle_worker_preserves_original_generation_behavior_without_titles(tmp_path, monkeypatch):
    manifest, projects_root, jobs_dir = _create_project(tmp_path)
    manifest_path = Path(manifest.project_root) / 'editor_project.json'
    clip = manifest.clips[0]
    clip.metadata['title_overlay_enabled'] = False
    clip.subtitle_recipe.override_text = 'Edited subtitle'
    save_manifest(manifest, manifest_path)

    service = EditorService(projects_root=projects_root, jobs_dir=jobs_dir)
    calls = {'subtitle_only': 0, 'title_overlay': 0}

    def fake_process_clip(self, mp4, srt, output, subtitle_translation=None):
        calls['subtitle_only'] += 1
        Path(output).write_bytes(b'subtitle-only')
        return True

    def fake_add_title(self, *args, **kwargs):
        calls['title_overlay'] += 1
        return True

    monkeypatch.setattr('core.subtitle_burner.SubtitleBurner._process_clip', fake_process_clip, raising=False)
    monkeypatch.setattr('core.title_adder.TitleAdder._add_artistic_title', fake_add_title, raising=False)

    result = service._subtitle_worker(manifest_path, clip.clip_id, None, lambda *_args: None)

    assert result['current_composed_clip'].endswith('.mp4')
    assert calls['subtitle_only'] == 1
    assert calls['title_overlay'] == 0


def test_boundary_worker_refreshes_post_processed_clip_when_subtitles_are_derived(tmp_path, monkeypatch):
    manifest, projects_root, jobs_dir = _create_project(tmp_path)
    manifest_path = Path(manifest.project_root) / 'editor_project.json'
    clip = manifest.clips[0]
    clip.metadata['title_overlay_enabled'] = False
    clip.subtitle_recipe.override_text = None
    clip.asset_registry.subtitle_sidecars['active'] = clip.asset_registry.subtitle_sidecars['original']
    save_manifest(manifest, manifest_path)

    service = EditorService(projects_root=projects_root, jobs_dir=jobs_dir)
    calls = {'create_clip': 0, 'extract_subtitle': 0, 'subtitle_only': 0}

    def fake_create_clip(self, source_video_path, start_time, end_time, output_path, title):
        calls['create_clip'] += 1
        Path(output_path).write_bytes(b'raw-updated')
        return True

    def fake_extract_subtitle(self, subtitle_path, start_time, end_time, output_path):
        calls['extract_subtitle'] += 1
        Path(output_path).write_text("1\n00:00:00,000 --> 00:00:01,000\nDerived\n", encoding='utf-8')
        return True

    def fake_process_clip(self, mp4, srt, output, subtitle_translation=None):
        calls['subtitle_only'] += 1
        Path(output).write_bytes(b'composed-updated')
        return True

    monkeypatch.setattr('core.clip_generator.ClipGenerator._create_clip', fake_create_clip, raising=False)
    monkeypatch.setattr('core.clip_generator.ClipGenerator._extract_subtitle_from_file', fake_extract_subtitle, raising=False)
    monkeypatch.setattr('core.subtitle_burner.SubtitleBurner._process_clip', fake_process_clip, raising=False)

    result = service._boundary_worker(manifest_path, clip.clip_id, None, lambda *_args: None)
    saved_manifest = load_manifest(manifest_path)
    saved_clip = saved_manifest.clip_by_id(clip.clip_id)

    assert calls['create_clip'] == 1
    assert calls['extract_subtitle'] >= 1
    assert calls['subtitle_only'] == 1
    assert Path(result['current_composed_clip']).parent.name == 'clips_post_processed'
    assert Path(saved_clip.asset_registry.current_composed_clip).parent.name == 'clips_post_processed'


def test_boundary_worker_keeps_manual_override_on_raw_clip_until_subtitle_rerender(tmp_path, monkeypatch):
    manifest, projects_root, jobs_dir = _create_project(tmp_path)
    manifest_path = Path(manifest.project_root) / 'editor_project.json'
    clip = manifest.clips[0]
    clip.metadata['title_overlay_enabled'] = False
    clip.subtitle_recipe.override_text = 'Manual override text'
    save_manifest(manifest, manifest_path)

    service = EditorService(projects_root=projects_root, jobs_dir=jobs_dir)
    calls = {'create_clip': 0, 'subtitle_only': 0}

    def fake_create_clip(self, source_video_path, start_time, end_time, output_path, title):
        calls['create_clip'] += 1
        Path(output_path).write_bytes(b'raw-updated')
        return True

    def fake_process_clip(self, mp4, srt, output, subtitle_translation=None):
        calls['subtitle_only'] += 1
        Path(output).write_bytes(b'composed-updated')
        return True

    monkeypatch.setattr('core.clip_generator.ClipGenerator._create_clip', fake_create_clip, raising=False)
    monkeypatch.setattr('core.subtitle_burner.SubtitleBurner._process_clip', fake_process_clip, raising=False)

    result = service._boundary_worker(manifest_path, clip.clip_id, None, lambda *_args: None)
    saved_manifest = load_manifest(manifest_path)
    saved_clip = saved_manifest.clip_by_id(clip.clip_id)

    assert calls['create_clip'] == 1
    assert calls['subtitle_only'] == 0
    assert result['current_composed_clip'] == saved_clip.asset_registry.raw_clip
    assert saved_clip.asset_registry.current_composed_clip == saved_clip.asset_registry.raw_clip


def test_request_rerender_rejects_duplicate_pending_job(tmp_path):
    manifest, projects_root, jobs_dir = _create_project(tmp_path)
    service = EditorService(projects_root=projects_root, jobs_dir=jobs_dir)
    clip_id = manifest.clips[0].clip_id

    service.request_rerender(manifest.project_id, clip_id, 'subtitle')

    with pytest.raises(ValueError, match='already has a pending rerender job'):
        service.request_rerender(manifest.project_id, clip_id, 'cover')


def test_bounds_change_marks_manual_subtitle_override_stale_and_regeneration_replaces_it(tmp_path):
    manifest, projects_root, jobs_dir = _create_project(tmp_path)
    service = EditorService(projects_root=projects_root, jobs_dir=jobs_dir)
    clip_id = manifest.clips[0].clip_id

    service.update_clip_subtitles(manifest.project_id, clip_id, 'Manual override text')
    updated_clip = service.update_clip_bounds(manifest.project_id, clip_id, "00:01:12", "00:01:27")
    assert updated_clip["metadata"]["subtitle_stale"] is True
    assert updated_clip["subtitle_recipe"]["override_text"] == 'Manual override text'

    regenerated = service.regenerate_subtitle_text(manifest.project_id, clip_id)
    assert regenerated["metadata"]["subtitle_stale"] is False
    assert regenerated["subtitle_recipe"]["override_text"] != 'Manual override text'
    assert regenerated["effective_subtitle_text"] == regenerated["subtitle_recipe"]["override_text"]


def test_update_clip_bounds_rejects_absolute_range_past_part_end(tmp_path):
    manifest, projects_root, jobs_dir = _create_project(tmp_path)
    service = EditorService(projects_root=projects_root, jobs_dir=jobs_dir)
    clip_id = manifest.clips[0].clip_id

    with pytest.raises(ValueError, match='past the end of its source part'):
        service.update_clip_bounds(manifest.project_id, clip_id, "00:01:20", "00:01:40")
