from typing import Optional

from meilisearch_python_sdk.models.settings import MeilisearchSettings as MsSettings
from pydantic import SecretStr, model_validator

from ormy.base.abc import ConfigABC
from ormy.base.pydantic import Base

# ----------------------- #


class MeilisearchSettings(MsSettings):
    default_sort: Optional[str] = None

    # ....................... #

    @model_validator(mode="after")
    def validate_default_sort(self):
        if self.default_sort and self.sortable_attributes:
            if self.default_sort not in self.sortable_attributes:
                raise ValueError(f"Invalid Default Sort Field: {self.default_sort}")

        return self


# ....................... #


class MeilisearchCredentials(Base):
    host: str = "localhost"
    port: str = "7700"
    master_key: Optional[SecretStr] = None


# ....................... #


class MeilisearchConfig(ConfigABC):
    # Local configuration
    index: str = "_default_"
    primary_key: str = "id"
    settings: MeilisearchSettings = MeilisearchSettings(searchable_attributes=["*"])
    include_to_registry: bool = True

    # Global configuration
    credentials: MeilisearchCredentials = MeilisearchCredentials()

    # ....................... #

    def url(self) -> str:
        """
        Returns the Meilisearch URL
        """

        return f"http://{self.credentials.host}:{self.credentials.port}"

    # ....................... #

    def is_default(self) -> bool:
        """
        Validate if the config is default
        """

        return self._default_helper("index")
