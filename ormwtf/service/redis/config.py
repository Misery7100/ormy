from typing import Optional

from ormwtf.base.abc import TypedDictWithDefaults

# ----------------------- #


class RedisCredentials(TypedDictWithDefaults):
    """
    Redis connect credentials

    Attributes:
        host (str): Redis host
        port (int): Redis port
        username (str): Redis username (applicable only for Redis with ACL)
        password (str): Redis password
    """

    host: str
    port: Optional[int]
    username: Optional[str]
    password: Optional[str]

    # ....................... #

    @classmethod
    def with_defaults(
        cls,
        host: str = "localhost",
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> "RedisCredentials":
        """
        Returns a new instance of RedisCredentials with overridable defaults
        """

        return cls(
            host=host,
            port=port,
            username=username,
            password=password,
        )


# ....................... #


#! TODO: check if possible to specify a particular database ?


class RedisConfig(TypedDictWithDefaults):
    """
    Redis Configuration for ORM WTF Base Model

    Attributes:
        database (int): Database number to assign
        collection (str): Collection name to assign
        credentials (RedisCredentials): Connection credentials
    """

    # Local configuration
    database: int
    collection: str

    # Global configuration
    credentials: RedisCredentials

    # ....................... #

    @classmethod
    def with_defaults(
        cls,
        database: int = 0,
        collection: str = "default",
        credentials: RedisCredentials = RedisCredentials.with_defaults(),
    ) -> "RedisConfig":
        """
        Returns a new instance of RedisConfig with overridable defaults
        """

        return cls(
            database=database,
            collection=collection,
            credentials=credentials,
        )

    # ....................... #

    def url(self) -> str:
        """
        Returns the Redis URL
        """

        creds = self.credentials
        password = creds.get("password", None)
        user = creds.get("username", None)
        host = creds.get("host", None)
        port = creds.get("port", None)

        if password:
            auth = f"{user or ''}:{password}@"

        else:
            auth = ""

        if port:
            conn = f"{host}:{port}"

        else:
            conn = host

        return f"redis://{auth}{conn}/{self.database}"
