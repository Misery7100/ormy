from typing import Optional

from pydantic import SecretStr

from ormy._abc import ConfigABC, Mergeable

# ----------------------- #


class RedlockCredentials(Mergeable):
    """
    Redis connect credentials for Redlock

    Attributes:
        host (str): Redis host
        port (int): Redis port
        username (SecretStr): Redis username (applicable only for Redis with ACL)
        password (SecretStr): Redis password
    """

    host: str = "localhost"
    port: Optional[int] = None
    username: Optional[SecretStr] = None
    password: Optional[SecretStr] = None

    # ....................... #

    def url(self) -> str:
        """
        Returns the Redis URL for Redlock
        """

        creds = self.model_dump_with_secrets()
        password = creds.get("password", None)
        user = creds.get("username", None)
        host = creds.get("host", None)
        port = creds.get("port", None)
        auth = ""
        conn = host

        if password:
            auth = f"{user or ''}:{password}@"

        if port:
            conn = f"{host}:{port}"

        return f"redis://{auth}{conn}"


# ....................... #


class RedlockConfig(ConfigABC):
    """
    Configuration for Redlock extension

    Attributes:
        database (int): Database number to assign
        collection (str): Collection name to assign
        log_level (ormy.utils.logging.LogLevel): Log level
        credentials (RedlockCredentials): Connection credentials
        context_client (bool): Whether to use context manager for Redis client
    """

    # Local configuration
    database: int = 0
    collection: str = "_default_"

    # Global configuration
    credentials: RedlockCredentials = RedlockCredentials()
    context_client: bool = True

    # ....................... #

    def url(self) -> str:
        """
        Returns the Redis URL
        """

        url = self.credentials.url()

        return f"{url}/{self.database}"

    # ....................... #

    def is_default(self) -> bool:
        """
        Validate if the config is default
        """

        return self._default_helper("collection")
