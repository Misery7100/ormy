import re
from typing import Optional

from pydantic import SecretStr, field_validator

from ormy._abc import ConfigABC, Mergeable

# ----------------------- #


class S3Credentials(Mergeable):
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
        log_level (ormy.utils.logging.LogLevel): Log level
        include_to_registry (bool): Whether to include to registry
        credentials (S3Credentials): S3 connect credentials
    """

    # Local configuration
    bucket: str = "default-bucket"

    # Global configuration
    credentials: S3Credentials = S3Credentials()

    # ....................... #

    @field_validator("bucket", mode="before")
    @classmethod
    def validate_and_transform_bucket(cls, v: str) -> str:
        """
        Validate and transform bucket name
        """

        bucket = v.lower()
        bucket = re.sub(r"[^a-z0-9.-]", "-", bucket)
        bucket = re.sub(r"\.\.+|-+", "-", bucket)
        bucket = bucket.strip("-")

        return bucket

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
