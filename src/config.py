"""
Configuration management.
Loads credentials from environment variables.
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from loguru import logger


load_dotenv()


class Config(BaseSettings):
    """App config from environment."""
    
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")
    
    groundx_api_key: str | None = Field(default=None, alias="GROUNDX_API_KEY")
    
    # LLM Config
    openai_api_key: str | None = Field(default=None, alias="OPENAI_API_KEY")
    openai_api_base: str | None = Field(default=None, alias="OPENAI_API_BASE")
    openai_model_name: str = Field(default="gpt-3.5-turbo", alias="OPENAI_MODEL_NAME")
    log_level: str = Field(default="INFO")
    scrape_target_url: str = Field(default="https://www.itnb.ch/en")
    data_dir: Path = Field(default=Path("data"))
    logs_dir: Path = Field(default=Path("logs"))
    
    def validate_required(self) -> bool:
        """Check required config is present."""
        if not self.groundx_api_key:
            logger.error("GROUNDX_API_KEY not set")
            return False
        return True


def get_config() -> Config:
    """Get config singleton."""
    return Config()
