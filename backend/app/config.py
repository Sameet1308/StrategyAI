"""Backend configuration.

All values come from the environment (.env at the repo root). Nothing is
hardcoded; mock mode is the default so the app runs with an empty .env.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

_REPO_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_REPO_ROOT / ".env")

_VAR_DIR = Path(__file__).resolve().parents[1] / "var"


def _bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


class Settings:
    def __init__(self) -> None:
        # MSTR adapter: mock unless explicitly turned off.
        self.mock_mstr = _bool("STRATEGYAI_MOCK_MSTR", True)

        # LLM provider: mock | anthropic | bedrock. Back-compat: the old
        # STRATEGYAI_MOCK_LLM flag still works when no provider is set.
        provider = os.getenv("STRATEGYAI_LLM_PROVIDER", "").strip().lower()
        if provider:
            self.llm_provider = provider
        elif _bool("STRATEGYAI_MOCK_LLM", True):
            self.llm_provider = "mock"
        else:
            self.llm_provider = "bedrock"
        self.mock_llm = self.llm_provider == "mock"

        _VAR_DIR.mkdir(parents=True, exist_ok=True)
        default_db = f"sqlite:///{(_VAR_DIR / 'strategyai.db').as_posix()}"
        self.database_url = os.getenv("STRATEGYAI_DATABASE_URL", default_db)

        # MSTR connection (single admin service account per client decision)
        self.mstr_base_url = (os.getenv("MSTR_BASE_URL")
                              or os.getenv("MSTR_DEV_BASE_URL") or "").rstrip("/")
        self.mstr_username = os.getenv("MSTR_USERNAME", "")
        self.mstr_password = os.getenv("MSTR_PASSWORD", "")
        self.mstr_verify_ssl = _bool("MSTR_VERIFY_SSL", False)

        # Bedrock (Claude via the Anthropic Bedrock Mantle client) — client AWS
        self.aws_region = os.getenv("AWS_REGION", "us-east-1")
        self.bedrock_model_id = os.getenv(
            "BEDROCK_MODEL_ID", "anthropic.claude-sonnet-5")

        # Direct Anthropic API (Claude with an API key) — easiest local live
        self.anthropic_api_key = os.getenv("ANTHROPIC_API_KEY", "")
        self.anthropic_model_id = os.getenv(
            "ANTHROPIC_MODEL_ID", "claude-sonnet-5")

        # Identity: in prod the internal ALB does Okta OIDC and forwards
        # x-amzn-oidc-data; locally we fall back to a dev user.
        self.require_alb_auth = _bool("STRATEGYAI_REQUIRE_ALB_AUTH", False)
        self.dev_user = os.getenv("STRATEGYAI_DEV_USER", "dev.admin@local")

        self.pending_action_ttl_seconds = int(
            os.getenv("STRATEGYAI_ACTION_TTL_SECONDS", "900"))
        self.max_tool_turns = int(os.getenv("STRATEGYAI_MAX_TOOL_TURNS", "6"))


settings = Settings()
