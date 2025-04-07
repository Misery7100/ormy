from typing import Optional

from pydantic import SecretStr, model_validator

from ormy._abc import ConfigABC, Mergeable
from ormy.exceptions import InternalError, ModuleNotFound

try:
    from meilisearch_python_sdk.models.settings import MeilisearchSettings as MsSettings
except ImportError as e:
    raise ModuleNotFound(
        extra="meilisearch", packages=["meilisearch-python-sdk"]
    ) from e

# ----------------------- #


# TODO: use Mergeable ?
class MeilisearchSettings(MsSettings, Mergeable):  # type: ignore[misc]
    """
    Meilisearch extension settings

    Attributes:
        default_sort (str, optional): Default sort field
        synonyms (JsonDict, optional): Synonyms
        stop_words (list[str], optional): Stop words
        ranking_rules (list[str], optional): Ranking rules
        filterable_attributes (list[str], optional): Filterable attributes
        distinct_attribute (str, optional): Distinct attribute
        searchable_attributes (list[str], optional): Searchable attributes
        displayed_attributes (list[str], optional): Displayed attributes
        sortable_attributes (list[str], optional): Sortable attributes
        typo_tolerance (TypoTolerance, optional): Typo tolerance
        faceting (Faceting, optional): Faceting
        pagination (Pagination, optional): Pagination
        proximity_precision (ProximityPrecision, optional): Proximity precision
        separator_tokens (list[str], optional): Separator tokens
        non_separator_tokens (list[str], optional): Non separator tokens
        search_cutoff_ms (int, optional): Search cutoff ms
        dictionary (list[str], optional): Dictionary
        embedders (dict[str, OpenAiEmbedder | HuggingFaceEmbedder | OllamaEmbedder | RestEmbedder | UserProvidedEmbedder], optional): Embedders
        localized_attributes (list[LocalizedAttributes], optional): Localized attributes
    """

    default_sort: Optional[str] = None
    exclude_mask: Optional[dict[str, str | list[str]]] = None

    # ....................... #

    @model_validator(mode="after")
    def validate_default_sort(self):
        if self.default_sort and self.sortable_attributes:
            if self.default_sort not in self.sortable_attributes:
                raise InternalError(f"Invalid Default Sort Field: {self.default_sort}")

        return self


# ....................... #


class MeilisearchCredentials(Mergeable):
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
