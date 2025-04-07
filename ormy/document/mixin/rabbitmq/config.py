from typing import Optional

from pydantic import SecretStr

from ormy._abc import ConfigABC, Mergeable

# ----------------------- #


class RabbitMQCredentials(Mergeable):
    """
    RabbitMQ connect credentials

    Attributes:
        host (str): RabbitMQ host
        port (int): RabbitMQ port
        username (SecretStr): RabbitMQ username
        password (SecretStr): RabbitMQ password
    """

    host: str = "localhost"
    port: Optional[int] = None
    username: Optional[SecretStr] = None
    password: Optional[SecretStr] = None

    # ....................... #

    def url(self) -> str:
        """
        Returns the RabbitMQ URL
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

        return f"amqp://{auth}{conn}"


# ....................... #


class RabbitMQConfig(ConfigABC):
    """
    Configuration for RabbitMQ extension

    Attributes:
        queue (str): RabbitMQ queue name
        credentials (RabbitMQCredentials): RabbitMQ connect credentials
    """

    queue: str = "_default_"
    credentials: RabbitMQCredentials = RabbitMQCredentials()

    # ....................... #

    def url(self) -> str:
        """
        Returns the RabbitMQ URL
        """

        return self.credentials.url()

    # ....................... #

    def is_default(self) -> bool:
        """
        Validate if the config is default
        """

        return self._default_helper("queue")
