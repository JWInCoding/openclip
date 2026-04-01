from core.llm.custom_openai_api_client import CustomOpenAIAPIClient
from core.subtitle_burner import SubtitleBurner


def test_custom_openai_client_uses_environment_overrides_without_api_key(monkeypatch):
    with monkeypatch.context() as env:
        env.delenv("CUSTOM_OPENAI_API_KEY", raising=False)
        env.setenv("CUSTOM_OPENAI_BASE_URL", "https://gateway.example/v1/")
        env.setenv("CUSTOM_OPENAI_MODEL", "gateway-model")

        client = CustomOpenAIAPIClient()

        assert client.api_key is None
        assert client.base_url == "https://gateway.example/v1/chat/completions"
        assert client.default_model == "gateway-model"


def test_custom_openai_client_preserves_explicit_full_endpoint():
    client = CustomOpenAIAPIClient(
        base_url="https://gateway.example/custom/chat/completions",
    )

    assert client.base_url == "https://gateway.example/custom/chat/completions"


def test_subtitle_burner_allows_custom_openai_without_api_key():
    burner = SubtitleBurner(
        provider="custom_openai",
        model="local-model",
        base_url="https://gateway.example/v1",
        enable_llm=True,
    )

    assert burner.client is not None
    assert burner.model == "local-model"
