from typing import Optional

from pydantic import SecretStr

from ormwtf.base.pydantic import Base

# ----------------------- #


class MongoCredentials(Base):
    """
    MongoDB connect credentials

    Attributes:
        host (str): MongoDB host
        port (int): MongoDB port
        username (SecretStr): MongoDB username
        password (SecretStr): MongoDB password
        replicaset (str): MongoDB replicaset
        directConnection (bool): Whether to connect to replica directly
    """

    host: str = "localhost"
    port: Optional[int] = None
    username: Optional[SecretStr] = None
    password: Optional[SecretStr] = None
    replicaset: str = "rs0"
    directConnection: bool = False


# ....................... #


class MongoConfig(Base):
    """
    Mongo Configuration for ORM WTF Base Model

    Attributes:
        database (str): Database name to assign
        collection (str): Collection name to assign
        streaming (bool): Whether to enable watch on collection
        credentials (MongoCredentials): Connection credentials
    """

    # Local configuration
    database: str = "default"
    collection: str = "default"
    streaming: bool = True

    # Global configuration
    credentials: MongoCredentials = MongoCredentials()
