from typing import Optional

from google.cloud import bigquery
from pydantic import ConfigDict

from ormy._abc import ConfigABC
from ormy.base.pydantic import Base

# ----------------------- #


class BigQueryCredentials(Base):

    model_config = ConfigDict(arbitrary_types_allowed=True)

    # ....................... #

    project_id: Optional[str] = None
    client: Optional[bigquery.Client] = None


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

    dataset: str = "default"
    table: str = "default"

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
