import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from app.services.media_storyboard import (
    _ffmpeg_extract_frames_command,
    _probe_duration_seconds,
    _probe_duration_from_json,
    _sampling_fps,
    _target_columns,
    _target_frame_count,
)


class MediaStoryboardSamplingTests(unittest.TestCase):
    def test_target_frame_count_scales_with_duration(self) -> None:
        self.assertEqual(_target_frame_count(1.0, requested_frame_count=None), 8)
        self.assertEqual(_target_frame_count(3.0, requested_frame_count=None), 12)
        self.assertEqual(_target_frame_count(5.25, requested_frame_count=None), 21)
        self.assertEqual(_target_frame_count(20.0, requested_frame_count=None), 24)

    def test_explicit_frame_count_override_is_preserved(self) -> None:
        self.assertEqual(_target_frame_count(20.0, requested_frame_count=6), 6)
        self.assertEqual(_target_frame_count(20.0, requested_frame_count=100), 24)

    def test_sampling_fps_covers_full_duration(self) -> None:
        self.assertAlmostEqual(_sampling_fps(6.0, 24), 4.0)
        self.assertAlmostEqual(_sampling_fps(4.0, 16), 4.0)
        self.assertAlmostEqual(_sampling_fps(None, 16), 3.0)

    def test_columns_expand_for_larger_storyboards(self) -> None:
        self.assertEqual(_target_columns(12, requested_columns=None), 3)
        self.assertEqual(_target_columns(16, requested_columns=None), 4)
        self.assertEqual(_target_columns(24, requested_columns=5), 5)

    def test_ffmpeg_command_uses_duration_aware_fps_and_dynamic_frame_cap(self) -> None:
        command = _ffmpeg_extract_frames_command(
            input_path=Path("input.webm"),
            frame_pattern="frame_%03d.png",
            duration_seconds=6.0,
            frame_count=24,
            frame_width=320,
        )

        self.assertIn("fps=4.000000,scale=320:-1:force_original_aspect_ratio=decrease", command)
        self.assertEqual(command[command.index("-frames:v") + 1], "24")

    @patch("app.services.media_storyboard.shutil.which", return_value="ffprobe")
    @patch("app.services.media_storyboard.subprocess.run")
    def test_probe_duration_seconds_parses_ffprobe_json(self, run, _which) -> None:
        run.return_value = SimpleNamespace(
            returncode=0,
            stdout='{"streams":[{"duration":"4.25"}],"format":{"duration":"4.25"}}',
        )

        duration = _probe_duration_seconds(Path("input.webm"), timeout_seconds=5.0)

        self.assertEqual(duration, 4.25)
        self.assertIn("ffprobe", run.call_args.args[0][0])
        self.assertIn("-count_frames", run.call_args.args[0])

    def test_probe_duration_falls_back_to_frame_count_when_metadata_is_bogus(self) -> None:
        duration = _probe_duration_from_json(
            {
                "streams": [
                    {
                        "nb_read_frames": "323",
                        "avg_frame_rate": "60/1",
                    }
                ],
                "format": {"duration": "0.001000"},
            }
        )

        self.assertAlmostEqual(duration, 323 / 60)
        self.assertEqual(_target_frame_count(duration, requested_frame_count=None), 22)
