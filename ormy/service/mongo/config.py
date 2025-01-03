from typing import Optional

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
    ping_database: str = "admin"

    # ....................... #

    def is_default(self) -> bool:
        """
        Validate if the config is default
        """

        return self._default_helper("collection")
