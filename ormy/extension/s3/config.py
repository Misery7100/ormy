from typing import Optional

from pydantic import SecretStr

from ormy.base.abc import ConfigABC
from ormy.base.pydantic import Base

# ----------------------- #


class S3Credentials(Base):
    """
    S3 connect credentials

    Attributes:
        username (SecretStr): S3 username
        password (SecretStr): S3 password
        host (str): S3 host
        port (int, optional): S3 port
        https (bool): Whether to use HTTPS
    """

    username: Optional[SecretStr] = None
    password: Optional[SecretStr] = None
    host: str = "localhost"
    port: Optional[int] = None
    https: bool = False

    # ....................... #

    def url(self) -> str:
        """
        Returns the S3 endpoint URL
        """

        if self.https:
            return f"https://{self.host}"

        return f"http://{self.host}:{self.port}"


# ....................... #


class S3Config(ConfigABC):
    """
    Configuration for S3 extension

    Attributes:
        bucket (str): S3 bucket name
        include_to_registry (bool): Whether to include to registry
        credentials (S3Credentials): S3 connect credentials
    """

    # Local configuration
    bucket: str = "_default_"
    include_to_registry: bool = True

    # Global configuration
    credentials: S3Credentials = S3Credentials()

    # ....................... #

    def is_default(self) -> bool:
        """
        Validate if the config is default
        """

        return self._default_helper("bucket")

    # ....................... #

    def url(self) -> str:
        """
        Returns the S3 endpoint URL
        """

        return self.credentials.url()
