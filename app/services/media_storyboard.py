from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from fractions import Fraction
from math import ceil
from io import BytesIO
from pathlib import Path

_DEFAULT_MIN_FRAME_COUNT = 8
_DEFAULT_MAX_FRAME_COUNT = 24
_DEFAULT_FRAMES_PER_SECOND = 4.0
_DEFAULT_FALLBACK_FPS = 3.0


def _clamp(value: int, *, minimum: int, maximum: int) -> int:
    return max(minimum, min(maximum, value))


def _target_frame_count(
    duration_seconds: float | None,
    *,
    requested_frame_count: int | None,
    min_frame_count: int = _DEFAULT_MIN_FRAME_COUNT,
    max_frame_count: int = _DEFAULT_MAX_FRAME_COUNT,
    frames_per_second: float = _DEFAULT_FRAMES_PER_SECOND,
) -> int:
    minimum = max(1, min_frame_count)
    maximum = max(minimum, max_frame_count)
    if requested_frame_count is not None:
        return _clamp(int(requested_frame_count), minimum=1, maximum=maximum)
    if duration_seconds is None or duration_seconds <= 0:
        return minimum
    derived_count = ceil(duration_seconds * max(0.1, frames_per_second))
    return _clamp(derived_count, minimum=minimum, maximum=maximum)


def _target_columns(frame_count: int, *, requested_columns: int | None) -> int:
    if requested_columns is not None:
        return max(1, requested_columns)
    if frame_count >= 16:
        return 4
    return 3


def _sampling_fps(duration_seconds: float | None, frame_count: int) -> float:
    if duration_seconds is None or duration_seconds <= 0:
        return _DEFAULT_FALLBACK_FPS
    return max(0.1, frame_count / duration_seconds)


def _parse_positive_float(value: object) -> float | None:
    try:
        parsed = float(str(value or "").strip())
    except (TypeError, ValueError):
        return None
    if parsed <= 0:
        return None
    return parsed


def _parse_frame_rate(value: object) -> float | None:
    text = str(value or "").strip()
    if not text or text == "0/0":
        return None
    try:
        rate = float(Fraction(text))
    except (ValueError, ZeroDivisionError):
        return _parse_positive_float(text)
    if rate <= 0:
        return None
    return rate


def _probe_duration_from_json(payload: dict[str, object]) -> float | None:
    candidates: list[float] = []
    format_payload = payload.get("format")
    if isinstance(format_payload, dict):
        duration = _parse_positive_float(format_payload.get("duration"))
        if duration is not None:
            candidates.append(duration)

    streams = payload.get("streams")
    if isinstance(streams, list):
        for stream in streams:
            if not isinstance(stream, dict):
                continue
            duration = _parse_positive_float(stream.get("duration"))
            if duration is not None:
                candidates.append(duration)
            frame_count = _parse_positive_float(
                stream.get("nb_read_frames") or stream.get("nb_frames")
            )
            frame_rate = _parse_frame_rate(stream.get("avg_frame_rate") or stream.get("r_frame_rate"))
            if frame_count is not None and frame_rate is not None:
                candidates.append(frame_count / frame_rate)

    if not candidates:
        return None
    return max(candidates)


def _probe_duration_seconds(input_path: Path, *, timeout_seconds: float) -> float | None:
    if shutil.which("ffprobe") is None:
        return None
    command = [
        "ffprobe",
        "-v",
        "error",
        "-count_frames",
        "-show_entries",
        "format=duration:stream=duration,nb_frames,nb_read_frames,r_frame_rate,avg_frame_rate",
        "-of",
        "json",
        str(input_path),
    ]
    try:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    try:
        payload = json.loads(result.stdout or "{}")
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    return _probe_duration_from_json(payload)


def _ffmpeg_extract_frames_command(
    *,
    input_path: Path,
    frame_pattern: str,
    duration_seconds: float | None,
    frame_count: int,
    frame_width: int,
) -> list[str]:
    fps = _sampling_fps(duration_seconds, frame_count)
    return [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-i",
        str(input_path),
        "-vf",
        f"fps={fps:.6f},scale={frame_width}:-1:force_original_aspect_ratio=decrease",
        "-frames:v",
        str(max(1, frame_count)),
        frame_pattern,
    ]


def build_animation_storyboard(
    media_bytes: bytes,
    *,
    frame_count: int | None = None,
    columns: int | None = None,
    frame_width: int = 320,
    min_frame_count: int = _DEFAULT_MIN_FRAME_COUNT,
    max_frame_count: int = _DEFAULT_MAX_FRAME_COUNT,
    frames_per_second: float = _DEFAULT_FRAMES_PER_SECOND,
    timeout_seconds: float = 20.0,
) -> bytes | None:
    """Return a PNG contact sheet for ffmpeg-readable animated media."""
    if not media_bytes or shutil.which("ffmpeg") is None:
        return None

    try:
        from PIL import Image, ImageDraw
    except Exception:
        return None

    with tempfile.TemporaryDirectory(prefix="media-storyboard-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        input_path = tmp_path / "input.media"
        input_path.write_bytes(media_bytes)
        frame_pattern = str(tmp_path / "frame_%03d.png")
        duration_seconds = _probe_duration_seconds(input_path, timeout_seconds=timeout_seconds)
        selected_frame_count = _target_frame_count(
            duration_seconds,
            requested_frame_count=frame_count,
            min_frame_count=min_frame_count,
            max_frame_count=max_frame_count,
            frames_per_second=frames_per_second,
        )
        command = _ffmpeg_extract_frames_command(
            input_path=input_path,
            frame_pattern=frame_pattern,
            duration_seconds=duration_seconds,
            frame_count=selected_frame_count,
            frame_width=frame_width,
        )
        try:
            subprocess.run(
                command,
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=timeout_seconds,
            )
        except (OSError, subprocess.SubprocessError):
            return None

        frame_paths = sorted(tmp_path.glob("frame_*.png"))
        if not frame_paths:
            return None

        frames = []
        for frame_path in frame_paths[:selected_frame_count]:
            try:
                frames.append(Image.open(frame_path).convert("RGB"))
            except Exception:
                continue
        if not frames:
            return None

        cell_width = max(frame.width for frame in frames)
        cell_height = max(frame.height for frame in frames)
        column_count = max(1, min(_target_columns(selected_frame_count, requested_columns=columns), len(frames)))
        row_count = (len(frames) + column_count - 1) // column_count
        sheet = Image.new("RGB", (cell_width * column_count, cell_height * row_count), "white")
        draw = ImageDraw.Draw(sheet)

        for index, frame in enumerate(frames):
            row = index // column_count
            column = index % column_count
            x = column * cell_width + (cell_width - frame.width) // 2
            y = row * cell_height + (cell_height - frame.height) // 2
            sheet.paste(frame, (x, y))
            label = str(index + 1)
            draw.rectangle((x + 6, y + 6, x + 34, y + 30), fill="black")
            draw.text((x + 15, y + 10), label, fill="white")

        output = BytesIO()
        sheet.save(output, format="PNG", optimize=True)
        return output.getvalue()
