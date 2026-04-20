import asyncio
from pathlib import Path

import pytest

from video_orchestrator import VideoOrchestrator


def test_skip_transcript_uses_existing_local_subtitle_for_single_part_video(tmp_path, monkeypatch):
    source_video = tmp_path / "input.mp4"
    source_video.write_bytes(b"fake-video")
    source_subtitle = tmp_path / "input.srt"
    source_subtitle.write_text(
        "1\n00:00:00,000 --> 00:00:01,000\n你好\n",
        encoding="utf-8",
    )

    orchestrator = VideoOrchestrator(
        output_dir=str(tmp_path / "output"),
        skip_analysis=True,
        generate_clips=False,
        generate_cover=False,
    )

    async def fake_is_local_video_file(_source: str) -> bool:
        return True

    async def fake_process_local_video(_video_path: str, _progress_callback):
        return {
            "video_path": str(source_video),
            "video_info": {
                "title": "input",
                "duration": 60,
                "uploader": "Local File",
            },
            "subtitle_path": str(source_subtitle),
        }

    monkeypatch.setattr(orchestrator, "_is_local_video_file", fake_is_local_video_file)
    monkeypatch.setattr(orchestrator, "_process_local_video", fake_process_local_video)

    result = asyncio.run(
        orchestrator.process_video(
            str(source_video),
            skip_transcript=True,
            progress_callback=None,
        )
    )

    expected_subtitle = (
        Path(orchestrator.output_dir)
        / "input"
        / "splits"
        / "input_part01.srt"
    )

    assert result.success is True
    assert result.transcript_source == "existing"
    assert result.transcript_parts == [str(expected_subtitle)]
    assert expected_subtitle.exists()


def test_custom_openai_requires_model_when_analysis_is_enabled(tmp_path):
    with pytest.raises(ValueError, match="Invalid custom_openai analysis configuration"):
        VideoOrchestrator(
            output_dir=str(tmp_path / "output"),
            llm_provider="custom_openai",
            llm_base_url="http://127.0.0.1:8000/v1",
        )


def test_agentic_analysis_routes_through_coordinator(tmp_path, monkeypatch):
    source_video = tmp_path / "input.mp4"
    source_video.write_bytes(b"fake-video")
    source_subtitle = tmp_path / "input.srt"
    source_subtitle.write_text(
        "\n".join(
            [
                "1",
                "00:00:00,000 --> 00:00:45,000",
                "This clip contains enough setup to stand alone.",
            ]
        ),
        encoding="utf-8",
    )

    orchestrator = VideoOrchestrator(
        output_dir=str(tmp_path / "output"),
        api_key="test-key",
        agentic_analysis=True,
        generate_clips=False,
        generate_cover=False,
    )

    async def fake_is_local_video_file(_source: str) -> bool:
        return True

    async def fake_process_local_video(_video_path: str, _progress_callback):
        return {
            "video_path": str(source_video),
            "video_info": {
                "title": "input",
                "duration": 60,
                "uploader": "Local File",
            },
            "subtitle_path": str(source_subtitle),
        }

    async def fake_process_transcripts(_subtitle_path, _video_path, _force_whisper, _progress_callback):
        output_srt = (
            Path(orchestrator.output_dir)
            / "input"
            / "splits"
            / "input_part01.srt"
        )
        output_srt.parent.mkdir(parents=True, exist_ok=True)
        output_srt.write_text(source_subtitle.read_text(encoding="utf-8"), encoding="utf-8")
        return {
            "source": "existing",
            "transcript_parts": [str(output_srt)],
        }

    async def fake_run(transcript_parts, progress_callback=None):
        aggregated_file = Path(transcript_parts[0]).parent / "top_engaging_moments.json"
        aggregated_file.write_text(
            '{"top_engaging_moments":[],"total_moments":0}',
            encoding="utf-8",
        )
        return {
            "highlights_files": [],
            "aggregated_file": str(aggregated_file),
            "top_moments": {"top_engaging_moments": [], "total_moments": 0},
            "total_parts_analyzed": len(transcript_parts),
            "agentic_analysis": True,
        }

    monkeypatch.setattr(orchestrator, "_is_local_video_file", fake_is_local_video_file)
    monkeypatch.setattr(orchestrator, "_process_local_video", fake_process_local_video)
    monkeypatch.setattr(orchestrator.transcript_processor, "process_transcripts", fake_process_transcripts)
    monkeypatch.setattr(orchestrator.analysis_coordinator, "run", fake_run)

    result = asyncio.run(
        orchestrator.process_video(
            str(source_video),
            skip_transcript=False,
            progress_callback=None,
        )
    )

    assert result.success is True
    assert result.engaging_moments_analysis["agentic_analysis"] is True
