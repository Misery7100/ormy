from typing import Optional

from ormy.exceptions import ModuleNotFound

try:
    from google.cloud.bigquery import Client
except ImportError as e:
    raise ModuleNotFound(extra="bigquery", packages=["google-cloud-bigquery"]) from e

from pydantic import ConfigDict

from ormy._abc import ConfigABC, Mergeable

# ----------------------- #


class BigQueryCredentials(Mergeable):

    model_config = ConfigDict(arbitrary_types_allowed=True)

    # ....................... #

    project_id: Optional[str] = None
    client: Optional[Client] = None


# ....................... #


class BigQueryConfig(ConfigABC):
    """
    BigQuery configuration

    Attributes:
        dataset (str): BigQuery dataset name
        table (str): BigQuery table name
        credentials (BigQueryCredentials): BigQuery credentials
        timeout (int, optional): Timeout for the query
        max_batch_size (int, optional): Maximum batch size for the query
    """

    dataset: str = "_default_"
    table: str = "_default_"

    credentials: BigQueryCredentials = BigQueryCredentials()
    timeout: int = 300
    max_batch_size: int = 10000

    # ....................... #

    def is_default(self) -> bool:
        """
        Validate if the config is default
        """

        return self._default_helper("dataset", "table")

    # ....................... #

    def client(self):
        return self.credentials.client

    # ....................... #

    @property
    def full_dataset_path(self) -> str:
        return f"{self.credentials.project_id}.{self.dataset}"

    # ....................... #

    @property
    def full_table_path(self) -> str:
        return f"{self.full_dataset_path}.{self.table}"
