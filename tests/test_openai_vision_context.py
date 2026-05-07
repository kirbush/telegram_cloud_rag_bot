import unittest
from types import SimpleNamespace

import httpx

from app.services.openai_vision_context import OpenAIVisionContextService


class _FakeResponse:
    def __init__(self, payload, *, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code
        self.request = httpx.Request("POST", "https://api.openai.com/v1/responses")

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise httpx.HTTPStatusError("boom", request=self.request, response=httpx.Response(self.status_code))

    def json(self):
        return self._payload


class _FakeClient:
    def __init__(self, responses) -> None:
        self._responses = list(responses)
        self.calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False

    async def post(self, url, *, json):
        self.calls.append((url, json))
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class OpenAIVisionContextServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_analyze_media_posts_to_responses_and_parses_json(self) -> None:
        settings = SimpleNamespace(
            openai_api_key="sk-test",
            openai_base_url="https://api.openai.com/v1",
            openai_vision_model="gpt-5.4-nano",
            media_context_timeout_seconds=5.0,
            media_context_max_retries=1,
            notebooklm_proxy_enabled=False,
        )
        client = _FakeClient(
            [
                _FakeResponse(
                    {
                        "output_text": '{"summary":"Коротко","ocr_text":"HELLO","visual_intent":"Мем"}',
                    }
                )
            ]
        )
        service = OpenAIVisionContextService(settings=settings)
        service._client = lambda: client

        result = await service.analyze_media(
            media_bytes=b"image-bytes",
            media_kind="photo",
            mime_type="image/jpeg",
        )

        self.assertEqual(result.summary, "Коротко")
        self.assertEqual(result.ocr_text, "HELLO")
        self.assertEqual(result.visual_intent, "Мем")
        self.assertEqual(client.calls[0][0], "https://api.openai.com/v1/responses")
        self.assertEqual(client.calls[0][1]["model"], "gpt-5.4-nano")
        self.assertEqual(client.calls[0][1]["reasoning"]["effort"], "medium")
        prompt = client.calls[0][1]["input"][0]["content"][0]["text"]
        image_url = client.calls[0][1]["input"][0]["content"][1]["image_url"]
        self.assertNotIn("storyboard/contact sheet", prompt)
        self.assertTrue(image_url.startswith("data:image/jpeg;base64,"))

    async def test_analyze_storyboard_adds_motion_prompt_and_png_mime(self) -> None:
        settings = SimpleNamespace(
            openai_api_key="sk-test",
            openai_base_url="https://api.openai.com/v1",
            openai_vision_model="gpt-5.4-nano",
            media_context_timeout_seconds=5.0,
            media_context_max_retries=1,
            notebooklm_proxy_enabled=False,
        )
        client = _FakeClient(
            [_FakeResponse({"output_text": '{"summary":"ok","ocr_text":"","visual_intent":"motion"}'})]
        )
        service = OpenAIVisionContextService(settings=settings)
        service._client = lambda: client

        await service.analyze_media(
            media_bytes=b"png-bytes",
            media_kind="sticker",
            mime_type="image/png",
            is_storyboard=True,
        )

        prompt = client.calls[0][1]["input"][0]["content"][0]["text"]
        image_url = client.calls[0][1]["input"][0]["content"][1]["image_url"]
        self.assertIn("storyboard/contact sheet", prompt)
        self.assertIn("chronological order", prompt)
        self.assertTrue(image_url.startswith("data:image/png;base64,"))

    async def test_analyze_media_retries_invalid_json_then_succeeds(self) -> None:
        settings = SimpleNamespace(
            openai_api_key="sk-test",
            openai_base_url="https://api.openai.com/v1",
            openai_vision_model="gpt-5.4-nano",
            media_context_timeout_seconds=5.0,
            media_context_max_retries=2,
            notebooklm_proxy_enabled=False,
        )
        client = _FakeClient(
            [
                _FakeResponse({"output_text": "not json"}),
                _FakeResponse({"output_text": '{"summary":"ok","ocr_text":"","visual_intent":"intent"}'}),
            ]
        )
        service = OpenAIVisionContextService(settings=settings)
        service._client = lambda: client

        result = await service.analyze_media(
            media_bytes=b"image-bytes",
            media_kind="sticker",
            mime_type="image/webp",
        )

        self.assertEqual(result.summary, "ok")
        self.assertEqual(len(client.calls), 2)
