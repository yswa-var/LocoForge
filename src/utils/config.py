"""Configuration management for the Query Orchestrator."""

import os
from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, field_validator


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # OpenAI Configuration
    openai_api_key: Optional[str] = Field(
        default=None,
        env="OPENAI_API_KEY",
        description="OpenAI API key for LLM access"
    )
    
    # Database Paths
    sql_db_path: Optional[Path] = Field(
        default=None,
        env="SQL_DB_PATH",
        description="Path to SQLite database file"
    )
    google_credentials_path: Optional[Path] = Field(
        default=None,
        env="GOOGLE_APPLICATION_CREDENTIALS",
        description="Path to Google credentials JSON file"
    )
    
    # MongoDB Configuration (optional)
    mongodb_uri: Optional[str] = Field(
        default="mongodb://localhost:27017/",
        env="MONGODB_URI",
        description="MongoDB connection URI"
    )
    mongodb_database: str = Field(
        default="query_orchestrator",
        env="MONGODB_DATABASE",
        description="MongoDB database name"
    )
    
    # System Configuration
    log_level: str = Field(
        default="INFO",
        env="LOG_LEVEL",
        description="Logging level"
    )
    max_retries: int = Field(
        default=3,
        env="MAX_RETRIES",
        description="Maximum number of retry attempts"
    )
    enable_parallel_execution: bool = Field(
        default=True,
        env="ENABLE_PARALLEL_EXECUTION",
        description="Enable parallel task execution"
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",  # Ignore extra environment variables
    )

    @field_validator("sql_db_path", "google_credentials_path", mode="before")
    @classmethod
    def validate_paths(cls, v: Optional[str | Path]) -> Optional[Path]:
        """Validate and convert string paths to Path objects."""
        if v is None:
            return None
        if isinstance(v, str):
            v = Path(v)
        if not v.parent.exists():
            v.parent.mkdir(parents=True, exist_ok=True)
        return v

    @field_validator("openai_api_key", mode="before")
    @classmethod
    def validate_openai_key(cls, v: Optional[str]) -> Optional[str]:
        """Validate OpenAI API key if provided."""
        if v is None:
            return None
        if not v.startswith("sk-"):
            raise ValueError("Invalid OpenAI API key format")
        return v


# Create a global settings instance with better error handling
try:
    settings = Settings()
except Exception as e:
    print(f"Warning: Error loading settings: {e}")
    print("Some features may not work correctly until configuration is fixed.")
    print("Please ensure your .env file exists and contains the required variables:")
    print("  - OPENAI_API_KEY (optional)")
    print("  - SQL_DB_PATH (optional)")
    print("  - GOOGLE_APPLICATION_CREDENTIALS (optional)")
    print("  - MONGODB_URI (optional, defaults to mongodb://localhost:27017/)")
    print("  - MONGODB_DATABASE (optional, defaults to query_orchestrator)")
    print("  - LOG_LEVEL (optional, defaults to INFO)")
    print("  - MAX_RETRIES (optional, defaults to 3)")
    print("  - ENABLE_PARALLEL_EXECUTION (optional, defaults to true)")
    # Create a minimal settings instance to allow the application to start
    settings = Settings(
        openai_api_key=None,
        sql_db_path=None,
        google_credentials_path=None
    )


def validate_paths() -> None:
    """Validate that critical paths exist."""
    if settings.sql_db_path and not settings.sql_db_path.parent.exists():
        settings.sql_db_path.parent.mkdir(parents=True, exist_ok=True)
    
    if settings.google_credentials_path and not settings.google_credentials_path.exists():
        print(f"Warning: Google credentials file not found at {settings.google_credentials_path}")
        print("Google Drive features will not be available.")


# Validate paths on module import
try:
    validate_paths()
except Exception as e:
    print(f"Warning: Path validation failed: {e}")
    print("Some features may not work correctly until paths are properly configured.") 