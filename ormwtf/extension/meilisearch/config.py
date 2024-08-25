from typing import Optional

from meilisearch_python_sdk.models.settings import MeilisearchSettings
from pydantic import SecretStr

from ormwtf.base.pydantic import Base

# ----------------------- #


class MeilisearchCredentials(Base):
    host: str = "localhost"
    port: str = "7700"
    master_key: Optional[SecretStr] = None


# ....................... #


class MeilisearchConfig(Base):
    # Local configuration
    index: str = "default"
    primary_key: str = "id"
    settings: MeilisearchSettings = MeilisearchSettings()
    include_to_registry: bool = True

    # Global configuration
    credentials: MeilisearchCredentials = MeilisearchCredentials()

    # ....................... #

    def url(self) -> str:
        """
        Returns the Meilisearch URL
        """

        return f"http://{self.credentials.host}:{self.credentials.port}"
