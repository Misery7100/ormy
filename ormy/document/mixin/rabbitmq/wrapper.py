import json
from contextlib import asynccontextmanager, contextmanager
from typing import Any, ClassVar, Optional

from ormy.document._abc import DocumentMixinABC
from ormy.exceptions import ModuleNotFound

try:
    import aio_pika  # noqa: F401
    import pika  # type: ignore[import-untyped]
except ImportError as e:
    raise ModuleNotFound(extra="rabbitmq", packages=["pika", "aio-pika"]) from e

from .config import RabbitMQConfig

# ----------------------- #


class RabbitMQMixin(DocumentMixinABC):
    """RabbitMQ mixin"""

    mixin_configs: ClassVar[list[Any]] = [RabbitMQConfig()]

    # ....................... #

    def __init_subclass__(cls, **kwargs):
        """Initialize subclass"""

        super().__init_subclass__(**kwargs)

        cls.defer_mixin_registration(
            config=RabbitMQConfig,
            discriminator="queue",
        )

    # ....................... #

    @classmethod
    def _get_rmq_queue(cls):
        """Get queue"""

        cfg = cls.get_mixin_config(type_=RabbitMQConfig)
        queue = cfg.queue

        return queue

    # ....................... #

    @classmethod
    @contextmanager
    def _rmq_connection(cls):
        """
        Get RabbitMQ connection

        Returns:
            connection (pika.BlockingConnection): RabbitMQ connection
        """

        cfg = cls.get_mixin_config(type_=RabbitMQConfig)
        url = cfg.url()
        conn = pika.BlockingConnection(pika.URLParameters(url))

        try:
            yield conn

        finally:
            if conn.is_open:
                conn.close()

    # ....................... #

    @classmethod
    @asynccontextmanager
    async def _armq_connection(cls):
        """
        Get async RabbitMQ connection

        Returns:
            connection (aio_pika.abc.AbstractRobustConnection): async RabbitMQ connection
        """

        cfg = cls.get_mixin_config(type_=RabbitMQConfig)
        url = cfg.url()
        conn = await aio_pika.connect_robust(url)

        try:
            yield conn

        finally:
            if not conn.is_closed:
                await conn.close()

    # ....................... #

    @classmethod
    @contextmanager
    def _rmq_channel(cls):
        """
        Get syncronous RabbitMQ channel

        Yields:
            channel (pika.BlockingConnection): RabbitMQ channel
        """

        with cls._rmq_connection() as connection:
            channel = connection.channel()

            try:
                yield channel

            finally:
                if channel.is_open:
                    channel.close()

    # ....................... #

    @classmethod
    @asynccontextmanager
    async def _armq_channel(cls):
        """
        Get asyncronous RabbitMQ channel

        Yields:
            channel (aio_pika.abc.AbstractRobustConnection): async RabbitMQ channel
        """

        async with cls._armq_connection() as connection:
            channel = await connection.channel()

            try:
                yield channel

            finally:
                if not channel.is_closed:
                    await channel.close()

    # ....................... #

    @classmethod
    def _rmq_publish(
        cls,
        queue: str,
        message: Any,
        headers: Optional[dict[str, Any]] = None,
        delivery_mode: int = 2,
    ):
        """
        Publish message to RabbitMQ

        Args:
            queue (str): Queue to publish to
            message (Any): Message to publish (JSON serializable)
            headers (dict[str, Any]): Headers to publish
            delivery_mode (int): Delivery mode (2 for persistent)
        """

        with cls._rmq_channel() as channel:
            channel.basic_publish(
                exchange="",
                routing_key=queue,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    headers=headers,
                    content_type="application/json",
                    delivery_mode=delivery_mode,
                ),
            )

    # ....................... #

    @classmethod
    async def _armq_publish(
        cls,
        queue: str,
        message: Any,
        headers: Optional[dict[str, Any]] = None,
        delivery_mode: int = 2,
    ):
        """
        Publish message to RabbitMQ

        Args:
            queue (str): Queue to publish to
            message (Any): Message to publish (JSON serializable)
            headers (dict[str, Any]): Headers to publish
            delivery_mode (int): Delivery mode (2 for persistent)
        """

        async with cls._armq_channel() as channel:
            await channel.default_exchange.publish(
                message=aio_pika.Message(
                    body=json.dumps(message).encode(),
                    headers=headers,
                    content_type="application/json",
                    delivery_mode=delivery_mode,
                ),
                routing_key=queue,
            )

    # ....................... #

    @classmethod
    def rmq_publish(
        cls,
        message: Any,
        headers: Optional[dict[str, Any]] = None,
        delivery_mode: int = 2,
    ):
        """
        Publish message to RabbitMQ

        Args:
            message (Any): Message to publish (JSON serializable)
            headers (dict[str, Any]): Headers to publish
            delivery_mode (int): Delivery mode (2 for persistent)
        """

        queue = cls._get_rmq_queue()
        return cls._rmq_publish(
            queue=queue,
            message=message,
            headers=headers,
            delivery_mode=delivery_mode,
        )

    # ....................... #

    @classmethod
    async def armq_publish(
        cls,
        message: Any,
        headers: Optional[dict[str, Any]] = None,
        delivery_mode: int = 2,
    ):
        """
        Publish message to RabbitMQ

        Args:
            message (Any): Message to publish (JSON serializable)
            headers (dict[str, Any]): Headers to publish
            delivery_mode (int): Delivery mode (2 for persistent)
        """

        queue = cls._get_rmq_queue()
        return await cls._armq_publish(
            queue=queue,
            message=message,
            headers=headers,
            delivery_mode=delivery_mode,
        )
