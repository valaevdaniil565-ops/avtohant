from app import config


def test_resolve_env_path_prefers_explicit_env_file(tmp_path, monkeypatch):
    explicit_env = tmp_path / "explicit.env"
    explicit_env.write_text("DATABASE_URL=postgresql://example\nBOT_TOKEN=\n", encoding="utf-8")

    local_env = tmp_path / "local.env"
    local_env.write_text("DATABASE_URL=postgresql://local\nBOT_TOKEN=\n", encoding="utf-8")

    monkeypatch.setattr(config, "LOCAL_ENV_PATH", local_env)
    monkeypatch.setattr(config, "DEFAULT_SHARED_ENV_PATH", tmp_path / "missing.env")
    monkeypatch.setenv("HUNTING_MVP_ENV_FILE", str(explicit_env))
    monkeypatch.delenv("AUTOHUNT_ENV_FILE", raising=False)
    monkeypatch.delenv("HUNTING_MVP_EXTERNAL_DIR", raising=False)

    assert config.resolve_env_path() == explicit_env


def test_resolve_env_path_falls_back_to_default_shared_env(tmp_path, monkeypatch):
    shared_env = tmp_path / ".env"
    shared_env.write_text("DATABASE_URL=postgresql://shared\nBOT_TOKEN=\n", encoding="utf-8")

    monkeypatch.setattr(config, "LOCAL_ENV_PATH", tmp_path / "missing.env")
    monkeypatch.setattr(config, "DEFAULT_SHARED_ENV_PATH", shared_env)
    monkeypatch.delenv("HUNTING_MVP_ENV_FILE", raising=False)
    monkeypatch.delenv("AUTOHUNT_ENV_FILE", raising=False)
    monkeypatch.delenv("HUNTING_MVP_EXTERNAL_DIR", raising=False)

    assert config.resolve_env_path() == shared_env
