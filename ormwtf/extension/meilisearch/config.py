from typing import Optional

from meilisearch_python_sdk.models.settings import MeilisearchSettings
from pydantic import SecretStr

from ormwtf.base.abc import ConfigABC
from ormwtf.base.pydantic import Base

# ----------------------- #


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
