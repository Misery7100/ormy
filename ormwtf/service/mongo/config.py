from typing import Optional

from typing_extensions import TypedDict

# ----------------------- #


class MongoCredentials(TypedDict):
    host: str
    port: Optional[int]
    username: str
    password: str
    replicaset: str

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
    ):
        return cls(
            host=host,
            port=port,
            username=username,
            password=password,
            replicaset=replicaset,
            directConnection=directConnection,
        )


# ....................... #


class MongoConfig(TypedDict):

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
        return cls(
            database=database,
            collection=collection,
            streaming=streaming,
            credentials=credentials,
        )
