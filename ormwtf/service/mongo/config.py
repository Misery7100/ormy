from typing import Optional

from ormwtf.base.abc import TypedDictWithDefaults

# ----------------------- #


class MongoCredentials(TypedDictWithDefaults):
    """
    MongoDB connect credentials

    Attributes:
        host (str): MongoDB host
        port (int): MongoDB port
        username (str): MongoDB username
        password (str): MongoDB password
        replicaset (str): MongoDB replicaset
        directConnection (bool): Whether to connect to replica directly
    """

    host: str
    port: Optional[int]
    username: str
    password: str
    replicaset: str
    directConnection: bool

    # ....................... #

    @classmethod
    def with_defaults(
        cls,
        host: str = "localhost",
        port: Optional[int] = None,
        username: str = "root",
        password: str = "password",
        replicaset: str = "rs0",
        directConnection: bool = False,
    ) -> "MongoCredentials":
        """
        Returns a new instance of MongoCredentials with overridable defaults
        """

        return cls(
            host=host,
            port=port,
            username=username,
            password=password,
            replicaset=replicaset,
            directConnection=directConnection,
        )


# ....................... #


class MongoConfig(TypedDictWithDefaults):
    """
    Mongo Configuration for ORM WTF Base Model

    Attributes:
        database (str): Database name to assign
        collection (str): Collection name to assign
        streaming (bool): Whether to enable watch on collection
        credentials (MongoCredentials): Connection credentials
    """

    # Local configuration
    database: str
    collection: str
    streaming: bool

    # Global configuration
    credentials: MongoCredentials

    # ....................... #

    @classmethod
    def with_defaults(
        cls,
        database: str = "default",
        collection: str = "default",
        streaming: bool = True,
        credentials: MongoCredentials = MongoCredentials.with_defaults(),
    ):
        """
        Returns a new instance of MongoConfig with overridable defaults
        """

        return cls(
            database=database,
            collection=collection,
            streaming=streaming,
            credentials=credentials,
        )
