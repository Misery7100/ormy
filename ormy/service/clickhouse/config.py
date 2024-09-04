from typing import Optional

from pydantic import SecretStr

from ormy.base.abc import ConfigABC
from ormy.base.pydantic import Base

# ----------------------- #


class ClickHouseCredentials(Base):
    host: str = "localhost"
    port: Optional[int] = None
    username: Optional[SecretStr] = None
    password: Optional[SecretStr] = None


# ....................... #


class ClickHouseConfig(ConfigABC):
    database: str = "default"
    table: str = "default"
    include_to_registry: bool = True

    credentials: ClickHouseCredentials = ClickHouseCredentials()

    # ....................... #

    def is_default(self) -> bool:
        """
        Validate if the config is default
        """

        return self._default_helper("database", "table")

    # ....................... #

    def db_url(self) -> str:
        host = self.credentials.host
        port = self.credentials.port

        if port:
            return f"http://{host}:{port}/"

        return f"http://{host}/"
