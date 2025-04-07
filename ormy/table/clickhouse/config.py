from typing import Optional

from pydantic import SecretStr

from ormy._abc import ConfigABC, Mergeable

# ----------------------- #


class ClickHouseCredentials(Mergeable):
    """
    ClickHouse connect credentials

    Attributes:
        host (str): ClickHouse host
        port (int, optional): ClickHouse port
        username (SecretStr, optional): ClickHouse username
        password (SecretStr, optional): ClickHouse password
    """

    host: str = "localhost"
    port: Optional[int] = None
    username: Optional[SecretStr] = None
    password: Optional[SecretStr] = None

    # ....................... #

    def url(self) -> str:
        """
        Returns the ClickHouse database URL
        """

        if self.port:
            return f"http://{self.host}:{self.port}/"

        return f"http://{self.host}/"


# ....................... #


class ClickHouseConfig(ConfigABC):
    """
    ClickHouse extension config

    Attributes:
        database (str): ClickHouse database
        table (str): ClickHouse table
        log_level (ormy.utils.logging.LogLevel): Log level
        include_to_registry (bool): Whether to include to registry
        credentials (ClickHouseCredentials): ClickHouse connection credentials
    """

    database: str = "_default_"
    table: str = "_default_"

    credentials: ClickHouseCredentials = ClickHouseCredentials()

    # ....................... #

    def is_default(self) -> bool:
        """
        Validate if the config is default
        """

        return self._default_helper("database", "table")

    # ....................... #

    def url(self) -> str:
        """
        Returns the ClickHouse database URL
        """

        return self.credentials.url()
