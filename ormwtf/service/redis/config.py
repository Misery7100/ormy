from typing import Optional

from pydantic import SecretStr

from ormwtf.base.pydantic import Base

# ----------------------- #


class RedisCredentials(Base):
    """
    Redis connect credentials

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


class RedisConfig(Base):
    """
    Redis Configuration for ORM WTF Base Model

    Attributes:
        database (int): Database number to assign
        collection (str): Collection name to assign
        credentials (RedisCredentials): Connection credentials
    """

    # Local configuration
    database: int = 0
    collection: str = "default"
    include_to_registry: bool = True

    # Global configuration
    credentials: RedisCredentials = RedisCredentials()

    # ....................... #

    def url(self) -> str:
        """
        Returns the Redis URL
        """

        creds = self.credentials.model_dump_with_secrets()
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
