from pathlib import Path


def test_env_example_documents_bot_access_state_path() -> None:
    env_example = Path(".env.example").read_text(encoding="utf-8")

    assert "BOT_INSTANCE_NAME=" in env_example
    assert "BOT_ADMIN_USER_IDS=123456789" in env_example
    assert "BOT_ACCESS_STATE_PATH=.state/bot/access.sqlite3" in env_example
    assert "RUB" not in env_example
