from typing import Optional

from pydantic import SecretStr

from ormy._abc import ConfigABC
from ormy.base.pydantic import Base

# ----------------------- #


class ArangoCredentials(Base):
    """
    ArangoDB connect credentials

    Attributes:
        host (str): ArangoDB host
        port (int, optional): ArangoDB port
        username (SecretStr, optional): ArangoDB username
        password (SecretStr, optional): ArangoDB password
    """

    host: str = "localhost"
    port: Optional[int] = None
    username: SecretStr = SecretStr("")
    password: SecretStr = SecretStr("")

    # ....................... #

    def url(self) -> str:
        """Get the ArangoDB connection URL"""

        if self.port is None:
            _url = self.host

        else:
            _url = f"{self.host}:{self.port}"

        return f"http://{_url}"


# ....................... #


class ArangoConfig(ConfigABC):
    """ArangoDB configuration"""

    database: str = "default"
    collection: str = "default"
    credentials: ArangoCredentials = ArangoCredentials()

    # ....................... #

    def is_default(self) -> bool:
        """Validate if the config is default"""

        return self._default_helper("database", "collection")

    # ....................... #

    def url(self) -> str:
        """Get the ArangoDB connection URL"""

        return self.credentials.url()
