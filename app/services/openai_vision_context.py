"""OpenAI vision context extraction for lightweight NotebookLM media events."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any

import httpx

from app.core.config import get_notebooklm_proxy_url, get_settings


def _extract_output_text(payload: dict[str, Any]) -> str:
    output_text = payload.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text.strip()

    fragments: list[str] = []
    for item in payload.get("output", []):
        if not isinstance(item, dict):
            continue
        for content in item.get("content", []):
            if not isinstance(content, dict):
                continue
            if content.get("type") == "output_text" and isinstance(content.get("text"), str):
                fragments.append(content["text"])
    return "\n".join(fragment.strip() for fragment in fragments if fragment.strip())


def _parse_json_object(text: str) -> dict[str, Any]:
    stripped = text.strip()
    if not stripped:
        raise ValueError("OpenAI vision response did not contain any text.")
    try:
        payload = json.loads(stripped)
    except json.JSONDecodeError:
        start = stripped.find("{")
        end = stripped.rfind("}")
        if start < 0 or end < start:
            raise ValueError("OpenAI vision response did not contain valid JSON.") from None
        payload = json.loads(stripped[start : end + 1])
    if not isinstance(payload, dict):
        raise ValueError("OpenAI vision response JSON must be an object.")
    return payload


def _coerce_text_field(payload: dict[str, Any], field_name: str) -> str:
    value = payload.get(field_name, "")
    if value is None:
        return ""
    return str(value).strip()


@dataclass(slots=True)
class VisionContextResult:
    summary: str
    ocr_text: str
    visual_intent: str

    def to_timeline_text(self) -> str:
        lines = [
            f"Summary: {self.summary or '(empty)'}",
            "OCR text:",
            self.ocr_text or "(empty)",
            "",
            f"Visual intent: {self.visual_intent or '(empty)'}",
        ]
        return "\n".join(lines).rstrip()


class OpenAIVisionContextService:
    def __init__(self, settings=None) -> None:
        self._settings = settings or get_settings()

    def _responses_url(self) -> str:
        return f"{str(self._settings.openai_base_url).rstrip('/')}/responses"

    def _timeout(self) -> httpx.Timeout:
        timeout_seconds = float(getattr(self._settings, "media_context_timeout_seconds", 60.0) or 60.0)
        return httpx.Timeout(
            connect=timeout_seconds,
            read=timeout_seconds,
            write=timeout_seconds,
            pool=timeout_seconds,
        )

    def _client(self) -> httpx.AsyncClient:
        client_kwargs: dict[str, Any] = {
            "headers": {
                "Authorization": f"Bearer {self._settings.openai_api_key}",
                "Content-Type": "application/json",
            },
            "timeout": self._timeout(),
            "trust_env": False,
        }
        proxy_url = get_notebooklm_proxy_url(self._settings)
        if proxy_url:
            client_kwargs["proxy"] = proxy_url
        return httpx.AsyncClient(**client_kwargs)

    @staticmethod
    def _prompt(media_kind: str, *, is_storyboard: bool = False) -> str:
        prompt = (
            "Analyze the provided image and respond with valid JSON only, without markdown and without any "
            'explanations. Return exactly one object with the keys "summary", "ocr_text", and "visual_intent".\n\n'
            "Goal: convey not only what is literally shown in the image, but also the main point of the picture, "
            "its mood, possible subtext, and subtle humor, if these are genuinely inferable from the visual "
            "details.\n\n"
            "Field requirements:\n\n"
            '"summary": a short summary in Russian (1-3 sentences) that conveys the essence of the image rather than '
            "simply listing objects. State what is happening and, if visually justified, reflect the mood, irony, "
            "absurdity, tension, awkwardness, or emotional tone of the scene.\n"
            '"ocr_text": preserve all readable text as literally as possible, including case, punctuation, line '
            "breaks, emoji, and noticeable spaces. Do not translate, normalize, or infer anything. If only part of "
            'the text is readable, return only the readable fragments. If there is no text, return "".\n'
            '"visual_intent": in Russian, explain what the image is most likely trying to communicate, suggest, '
            "mock, contrast, or evoke in the viewer. Indicate the likely function of the image: meme, joke, satire, "
            "reaction image, advertisement, poster, warning, social commentary, emotional message, or simply an "
            "illustration. If there is a probable hidden meaning, visual irony, sarcasm, contrast-based humor, meme "
            'logic, or subtle joke, briefly explain it. If this is already interpretation rather than direct fact, '
            'use cautious phrasing such as "possibly", "probably", or "it seems".\n\n'
            "Analysis rules:\n\n"
            "First rely on observable details, then move to careful interpretation.\n"
            "Pay special attention to facial expressions, poses, composition, framing, symbols, unusual "
            "juxtapositions, exaggeration, awkwardness, and conflict between text and image - this is often where "
            "the main point, subtext, or humor is hidden.\n"
            "If the comedic effect is based on contrast, implication, absurdity, visual metaphor, irony, or sarcasm, "
            "briefly name that mechanism.\n"
            "If the image has multiple possible interpretations, choose the one best supported by the visuals; "
            "mention ambiguity only if it is truly important.\n"
            "Do not invent unreadable text and do not attribute external context to the image unless it is confirmed "
            "by the picture itself.\n"
            "If the image is straightforward and has no noticeable subtext or humor, say so directly.\n"
            "The response should be compact but meaningful."
        )
        if is_storyboard:
            prompt += (
                "\n\nAdditional context: the image is a storyboard/contact sheet made from one animated Telegram "
                "media item. Read the numbered frames in chronological order from left to right and top to bottom. "
                "Focus on changes between frames, motion, timing, and the overall animated reaction or joke; do not "
                "treat the frames as unrelated separate images."
            )
        return prompt

    async def analyze_media(
        self,
        *,
        media_bytes: bytes,
        media_kind: str,
        mime_type: str,
        is_storyboard: bool = False,
    ) -> VisionContextResult:
        data_url = f"data:{mime_type};base64,{base64.b64encode(media_bytes).decode('ascii')}"
        payload = {
            "model": getattr(self._settings, "openai_vision_model", "gpt-5.4-nano"),
            "reasoning": {
                "effort": "medium",
            },
            "input": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": self._prompt(media_kind, is_storyboard=is_storyboard),
                        },
                        {
                            "type": "input_image",
                            "image_url": data_url,
                            "detail": "high",
                        },
                    ],
                }
            ],
        }
        retries = max(0, int(getattr(self._settings, "media_context_max_retries", 2) or 0))
        last_error: Exception | None = None
        async with self._client() as client:
            for _attempt in range(retries + 1):
                try:
                    response = await client.post(self._responses_url(), json=payload)
                    response.raise_for_status()
                    body = response.json()
                    text = _extract_output_text(body)
                    parsed = _parse_json_object(text)
                    return VisionContextResult(
                        summary=_coerce_text_field(parsed, "summary"),
                        ocr_text=_coerce_text_field(parsed, "ocr_text"),
                        visual_intent=_coerce_text_field(parsed, "visual_intent"),
                    )
                except (httpx.HTTPError, ValueError) as exc:
                    last_error = exc
        raise RuntimeError("OpenAI vision analysis failed after retries.") from last_error
