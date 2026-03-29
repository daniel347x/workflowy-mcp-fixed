"""Configuration models for WorkFlowy MCP server."""

from __future__ import annotations

from pydantic import BaseModel, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings


# @beacon[
#   id=config-models@api-configuration,
#   role=APIConfiguration,
#   slice_labels=nexus--config,nexus-loading-flow,
#   kind=ast,
#   comment=Validated API client configuration model used after server settings are resolved,
# ]
class APIConfiguration(BaseModel):
    """Server configuration for WorkFlowy API access."""

    api_key: SecretStr = Field(..., description="WorkFlowy API authentication key")
    base_url: str = Field("https://workflowy.com/api/v1", description="API base URL")
    timeout: int = Field(30, gt=0, description="Request timeout in seconds")
    max_retries: int = Field(3, ge=0, description="Maximum retry attempts")

    @field_validator("api_key")
    @classmethod
    def validate_api_key(cls, v: SecretStr) -> SecretStr:
        """Ensure API key is not empty."""
        if not v or not v.get_secret_value().strip():
            raise ValueError("API key must be non-empty")
        return v

    @field_validator("base_url")
    @classmethod
    def validate_base_url(cls, v: str) -> str:
        """Ensure base URL is HTTPS."""
        if not v.startswith("https://"):
            raise ValueError("API base URL must use HTTPS")
        return v.rstrip("/")

    @field_validator("timeout")
    @classmethod
    def validate_timeout(cls, v: int) -> int:
        """Ensure timeout is positive."""
        if v <= 0:
            raise ValueError("Timeout must be positive")
        return v

    @field_validator("max_retries")
    @classmethod
    def validate_max_retries(cls, v: int) -> int:
        """Ensure max_retries is non-negative."""
        if v < 0:
            raise ValueError("Max retries must be non-negative")
        return v


# @beacon[
#   id=config-models@paths-configuration,
#   role=PathsConfiguration,
#   slice_labels=nexus--config,nexus-loading-flow,
#   kind=ast,
#   comment=Optional runtime filesystem paths loaded from workflowy_nexus config JSON,
# ]
class PathsConfiguration(BaseModel):
    """Optional runtime filesystem paths and local tool locations."""

    nexus_runs_base: str | None = Field(None, alias="nexusRunsBase")
    cartographer_jobs_dir: str | None = Field(None, alias="cartographerJobsDir")
    cartographer_file_refresh_dir: str | None = Field(None, alias="cartographerFileRefreshDir")
    windsurf_exe_candidates: list[str] = Field(default_factory=list, alias="windsurfExeCandidates")

    model_config = {"populate_by_name": True, "validate_by_name": True, "validate_by_alias": True, "extra": "forbid"}


# @beacon[
#   id=config-models@websocket-configuration,
#   role=WebSocketConfiguration,
#   slice_labels=nexus--config,nexus-loading-flow,
#   kind=ast,
#   comment=WebSocket host and port settings for server-extension communication,
# ]
class WebSocketConfiguration(BaseModel):
    """WebSocket settings for the Workflowy GLIMPSE bridge."""

    host: str = Field("localhost")
    port: int = Field(8765, gt=0, le=65535)

    model_config = {"populate_by_name": True, "validate_by_name": True, "validate_by_alias": True, "extra": "forbid"}


# @beacon[
#   id=config-models@timing-configuration,
#   role=TimingConfiguration,
#   slice_labels=nexus--config,nexus-loading-flow,
#   kind=ast,
#   comment=Timing and polling defaults loaded from workflowy_nexus config JSON,
# ]
class TimingConfiguration(BaseModel):
    """Timing defaults for extension reconnect and polling behavior."""

    websocket_reconnect_delay_ms: int = Field(3000, alias="websocketReconnectDelayMs", ge=0)
    carto_jobs_poll_interval_ms: int = Field(1000, alias="cartoJobsPollIntervalMs", ge=0)

    model_config = {"populate_by_name": True, "validate_by_name": True, "validate_by_alias": True, "extra": "forbid"}


# @beacon[
#   id=config-models@retention-configuration,
#   role=RetentionConfiguration,
#   slice_labels=nexus--config,nexus-loading-flow,
#   kind=ast,
#   comment=Retention and garbage-collection settings loaded from workflowy_nexus config JSON,
# ]
class RetentionConfiguration(BaseModel):
    """Retention settings for CARTO/F12 artifacts."""

    carto_job_max_age_seconds: int = Field(3600, alias="cartoJobMaxAgeSeconds", ge=0)

    model_config = {"populate_by_name": True, "validate_by_name": True, "validate_by_alias": True, "extra": "forbid"}


# @beacon[
#   id=config-models@server-config,
#   role=ServerConfig,
#   slice_labels=nexus--config,nexus-loading-flow,
#   kind=ast,
#   comment=Merged runtime configuration model populated from config file plus environment overrides,
# ]
class ServerConfig(BaseSettings):
    """Resolved runtime configuration for the Workflowy MCP server."""

    schema_version: int = Field(1, alias="schemaVersion")

    # WorkFlowy API settings
    workflowy_api_key: SecretStr = Field(
        ..., description="WorkFlowy API key", alias="WORKFLOWY_API_KEY"
    )
    workflowy_api_url: str = Field(
        "https://workflowy.com/api/v1",
        description="WorkFlowy API base URL",
        alias="WORKFLOWY_API_URL",
    )
    workflowy_timeout: int = Field(
        30, description="API request timeout in seconds", alias="WORKFLOWY_TIMEOUT"
    )
    workflowy_max_retries: int = Field(
        3, description="Maximum retry attempts", alias="WORKFLOWY_MAX_RETRIES"
    )

    # Server settings
    debug: bool = Field(False, description="Enable debug mode", alias="DEBUG")
    log_level: str = Field("INFO", description="Logging level", alias="LOG_LEVEL")

    # Optional external JSON-config sections
    paths: PathsConfiguration = Field(default_factory=PathsConfiguration)
    websocket: WebSocketConfiguration = Field(default_factory=WebSocketConfiguration)
    timing: TimingConfiguration = Field(default_factory=TimingConfiguration)
    retention: RetentionConfiguration = Field(default_factory=RetentionConfiguration)

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": False,
        "populate_by_name": True,
        "validate_by_name": True,
        "validate_by_alias": True,
        "extra": "forbid",
    }

    @field_validator("schema_version")
    @classmethod
    def validate_schema_version(cls, v: int) -> int:
        """Only schema version 1 is currently supported."""
        if v != 1:
            raise ValueError("Unsupported config schemaVersion; expected 1")
        return v

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Normalize logging level names."""
        norm = str(v).strip().upper()
        if norm not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError("LOG_LEVEL must be one of DEBUG, INFO, WARNING, ERROR, CRITICAL")
        return norm

    # @beacon[
    #   id=config-models@server-config-to-api-config,
    #   role=ServerConfig.get_api_config,
    #   slice_labels=nexus--config,nexus-loading-flow,
    #   kind=ast,
    #   comment=Convert loaded server settings into the APIConfiguration consumed by WorkFlowyClient,
    # ]
    def get_api_config(self) -> APIConfiguration:
        """Convert to APIConfiguration for the API client."""
        return APIConfiguration(
            api_key=self.workflowy_api_key,
            base_url=self.workflowy_api_url,
            timeout=self.workflowy_timeout,
            max_retries=self.workflowy_max_retries,
        )
