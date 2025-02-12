from typing import Optional
from urllib.parse import quote_plus

from pydantic import SecretStr

from ormy.base.abc import ConfigABC
from ormy.base.pydantic import Base

# ----------------------- #


class MongoCredentials(Base):
    """
    MongoDB connect credentials

    Attributes:
        host (str): MongoDB host
        port (int): MongoDB port
        username (SecretStr): MongoDB username
        password (SecretStr): MongoDB password
        replicaset (str, optional): MongoDB replicaset
        directConnection (bool): Whether to connect to replica directly
    """

    host: str = "localhost"
    port: Optional[int] = None
    username: Optional[SecretStr] = None
    password: Optional[SecretStr] = None
    replicaset: Optional[str] = None
    directConnection: bool = True

    # ....................... #

    def url(self) -> str:
        """Get the MongoDB connection URL"""

        username = self.username.get_secret_value() if self.username else None
        password = self.password.get_secret_value() if self.password else None

        if username and password:
            return f"mongodb://{quote_plus(username)}:{quote_plus(password)}@{self.host}:{self.port}"

        return f"mongodb://{self.host}:{self.port}"


# ....................... #


class MongoConfig(ConfigABC):
    """
    Configuration for Mongo Base Model

    Attributes:
        database (str): Database name to assign
        collection (str): Collection name to assign
        streaming (bool): Whether to enable watch on collection
        log_level (ormy.utils.logging.LogLevel): Log level
        include_to_registry (bool): Whether to include the config to registry
        credentials (MongoCredentials): Connection credentials
        ping_database (str): Database to ping for health check
    """

    # Local configuration
    database: str = "default"
    collection: str = "default"
    streaming: bool = False

    # Global configuration
    credentials: MongoCredentials = MongoCredentials()
    ping_database: str = "admin"  # TODO: remove

    # ....................... #

    def is_default(self) -> bool:
        """Validate if the config is default"""

        return self._default_helper("collection")

    # ....................... #

    def url(self) -> str:
        """Get the MongoDB connection URL"""

        return self.credentials.url()
