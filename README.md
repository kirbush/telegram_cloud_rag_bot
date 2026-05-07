# telegram_cloud_rag_bot

Lightweight Telegram bot for asking questions over a Telegram chat knowledge base through Google NotebookLM.

The project runs as a small Python/FastAPI/aiogram service. It keeps runtime state locally and can be deployed as a single VPS container pair: an API/admin service and a polling bot service.

## Features

- Telegram bot commands for asking questions in private chats and groups.
- Group mention support: mention the bot with a question and it replies in context.
- Google NotebookLM integration for search/summarization.
- Optional rolling source sync from Telegram history into NotebookLM markdown sources.
- Optional media context using OpenAI Vision.
- Owner-only bot admin panel in a private Telegram chat.
- Optional Telegram Stars credit/limit system for paid question credits.
- FastAPI admin UI for runtime status, auth refresh, and access configuration.
- SQLite local stores for lightweight history, access/credits, and conversation logs.

## Safety model

Do not commit runtime state or secrets. The repository is designed to keep these local-only:

- `.env`, `.env.vps*`
- `.state*/`
- `.tmp/`
- Telegram chat exports
- Google/NotebookLM storage state and cookies
- SQLite runtime databases
- private deployment notes

Use `.env.example` or `.env.vps.example` as templates and keep real values outside Git.

## Quick start

```bash
cp .env.example .env
# Fill BOT_TOKEN, NOTEBOOKLM_ADMIN_PASSWORD, and NotebookLM storage/runtime settings.
python -m app.main
```

## Core commands

| Command | Description |
| --- | --- |
| `/start`, `/help` | Show available commands |
| `/ask <question>` | Ask a question |
| `/nlm <question>` | Ask a question, compatibility alias |
| `/askboth <question>` | Ask a question with the current lightweight behavior |
| `/chats` | Select a chat/context for private messages |
| `/balance`, `/limits`, `/buy` | Access limits and Telegram Stars credits |
| `/admin` | Owner-only in-bot admin panel in a private chat |
| `/update` | Owner-only manual source sync |
| `/auth_nlm` | Owner-only NotebookLM auth refresh session |

## VPS deployment

The VPS compose file is `docker-compose.vps.yml`.

```bash
cp .env.vps.example .env.vps
# Fill real secrets and deployment-specific values.
docker compose -f docker-compose.vps.yml up -d api
docker compose -f docker-compose.vps.yml --profile cutover up -d bot
```

For multiple bot instances, use separate env files and state directories:

```bash
VPS_ENV_FILE=.env.vps.secondary \
VPS_STATE_DIR=./.state-secondary \
VPS_ADMIN_PORT=8011 \
VPS_BOT_UI_PORT=8001 \
docker compose -f docker-compose.vps.yml -p tgctxbot-secondary up -d api

VPS_ENV_FILE=.env.vps.secondary \
VPS_STATE_DIR=./.state-secondary \
VPS_ADMIN_PORT=8011 \
VPS_BOT_UI_PORT=8001 \
docker compose -f docker-compose.vps.yml -p tgctxbot-secondary --profile cutover up -d bot
```

Each instance should have its own:

- `BOT_INSTANCE_NAME`
- `BOT_ACCESS_STATE_PATH`
- `BOT_CONVERSATION_STATE_PATH`
- `NOTEBOOKLM_RUNTIME_STATE_PATH`
- `NOTEBOOKLM_RUNTIME_STORAGE_STATE`
- `BOT_ADMIN_USER_IDS`

`BOT_ADMIN_USER_IDS` is a comma-separated list of Telegram user IDs allowed to use owner/admin commands in a private chat.

## Admin UI

The API exposes an admin UI at `/admin/notebooklm`. Bind it to loopback or protect it behind your own secure reverse proxy.

Important admin-related env variables:

- `NOTEBOOKLM_ADMIN_USERNAME`
- `NOTEBOOKLM_ADMIN_PASSWORD`
- `NOTEBOOKLM_ADMIN_BIND_HOST`
- `NOTEBOOKLM_REMOTE_AUTH_BASE_URL`

## Configuration

See `.env.example` for a local template and `.env.vps.example` for a VPS template. The most important values are:

| Variable | Description |
| --- | --- |
| `BOT_TOKEN` | Telegram bot token |
| `BOT_ADMIN_USER_IDS` | Comma-separated Telegram user IDs allowed to use owner-only commands |
| `BOT_INSTANCE_NAME` | Human-readable deployment label |
| `NOTEBOOKLM_ENABLED` | Enable NotebookLM integration |
| `NOTEBOOKLM_STORAGE_STATE` | Path to NotebookLM/Google auth storage state |
| `NOTEBOOKLM_DEFAULT_NOTEBOOK` | Default NotebookLM notebook id or URL |
| `NOTEBOOKLM_NOTEBOOK_MAP` | JSON mapping of chat IDs to notebook IDs/URLs |
| `NOTEBOOKLM_ADMIN_USERNAME` / `NOTEBOOKLM_ADMIN_PASSWORD` | Basic Auth credentials for admin UI |
| `BOT_ACCESS_STATE_PATH` | SQLite path for access/credits state |
| `BOT_CONVERSATION_STATE_PATH` | SQLite path for owner conversation log |
| `NOTEBOOKLM_LIGHTWEIGHT_HISTORY_PATH` | SQLite path for Telegram history export |
| `OPENAI_API_KEY` | Optional, only needed for media context |

## Tech stack

- Python 3.11+
- aiogram 3
- FastAPI
- httpx / socksio / aiohttp-socks
- notebooklm-py
- SQLite
- PowerShell/Python Windows helper for cookie sync

