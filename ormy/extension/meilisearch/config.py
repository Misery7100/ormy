from typing import Dict, List, Optional

from meilisearch_python_sdk.models.settings import (
    Faceting,
    HuggingFaceEmbedder,
    LocalizedAttributes,
    OllamaEmbedder,
    OpenAiEmbedder,
    Pagination,
    ProximityPrecision,
    RestEmbedder,
    TypoTolerance,
    UserProvidedEmbedder,
)
from meilisearch_python_sdk.models.settings import MeilisearchSettings as MsSettings
from meilisearch_python_sdk.types import JsonDict
from pydantic import SecretStr, model_validator

from ormy.base.abc import ConfigABC
from ormy.base.error import InternalError
from ormy.base.pydantic import Base

# ----------------------- #


# TODO: merge settings option ?
class MeilisearchSettings(MsSettings):
    """
    Meilisearch extension settings

    Attributes:
        default_sort (str, optional): Default sort field
        synonyms (JsonDict, optional): Synonyms
        stop_words (List[str], optional): Stop words
        ranking_rules (List[str], optional): Ranking rules
        filterable_attributes (List[str], optional): Filterable attributes
        distinct_attribute (str, optional): Distinct attribute
        searchable_attributes (List[str], optional): Searchable attributes
        displayed_attributes (List[str], optional): Displayed attributes
        sortable_attributes (List[str], optional): Sortable attributes
        typo_tolerance (TypoTolerance, optional): Typo tolerance
        faceting (Faceting, optional): Faceting
        pagination (Pagination, optional): Pagination
        proximity_precision (ProximityPrecision, optional): Proximity precision
        separator_tokens (List[str], optional): Separator tokens
        non_separator_tokens (List[str], optional): Non separator tokens
        search_cutoff_ms (int, optional): Search cutoff ms
        dictionary (List[str], optional): Dictionary
        embedders (Dict[str, OpenAiEmbedder | HuggingFaceEmbedder | OllamaEmbedder | RestEmbedder | UserProvidedEmbedder], optional): Embedders
        localized_attributes (List[LocalizedAttributes], optional): Localized attributes
    """

    default_sort: Optional[str] = None
    exclude_mask: Optional[Dict[str, str | List[str]]] = None

    # ....................... #

    @model_validator(mode="after")
    def validate_default_sort(self):
        if self.default_sort and self.sortable_attributes:
            if self.default_sort not in self.sortable_attributes:
                raise InternalError(f"Invalid Default Sort Field: {self.default_sort}")

        return self


# ....................... #


class MeilisearchCredentials(Base):
    """
    Meilisearch connect credentials

    Attributes:
        master_key (SecretStr, optional): Meilisearch master key
        host (str): Meilisearch host
        port (int, optional): Meilisearch port
        https (bool): Whether to use HTTPS
    """

    master_key: Optional[SecretStr] = None
    host: str = "localhost"
    port: Optional[int] = 7700
    https: bool = False

    # ....................... #

    def url(self) -> str:
        """
        Returns the Meilisearch URL
        """

        if self.https:
            return f"https://{self.host}"

        return f"http://{self.host}:{self.port}"


# ....................... #


class MeilisearchConfig(ConfigABC):
    """
    Configuration for Meilisearch extension

    Attributes:
        index (str): Meilisearch index name
        primary_key (str): Meilisearch primary key
        settings (MeilisearchSettings): Meilisearch settings
        include_to_registry (bool): Whether to include to registry
        log_level (ormy.utils.logging.LogLevel): Log level
        credentials (MeilisearchCredentials): Meilisearch connect credentials
        context_client (bool): Whether to use context manager for Meilisearch client
    """

    # Local configuration
    index: str = "_default_"
    primary_key: str = "id"
    settings: MeilisearchSettings = MeilisearchSettings(searchable_attributes=["*"])

    # Global configuration
    credentials: MeilisearchCredentials = MeilisearchCredentials()
    context_client: bool = True

    # ....................... #

    def url(self) -> str:
        """
        Returns the Meilisearch URL
        """

        return self.credentials.url()

    # ....................... #

    def is_default(self) -> bool:
        """
        Validate if the config is default
        """

        return self._default_helper("index")


# ----------------------- #

__all__ = [
    "MeilisearchConfig",
    "MeilisearchCredentials",
    "MeilisearchSettings",
    "JsonDict",
    "TypoTolerance",
    "Faceting",
    "Pagination",
    "ProximityPrecision",
    "LocalizedAttributes",
    "OpenAiEmbedder",
    "OllamaEmbedder",
    "RestEmbedder",
    "UserProvidedEmbedder",
    "HuggingFaceEmbedder",
]
