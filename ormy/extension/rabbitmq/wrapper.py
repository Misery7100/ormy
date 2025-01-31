from contextlib import asynccontextmanager, contextmanager
from typing import Any, ClassVar, List, Optional, Type, TypeVar

import aio_pika
import pika  # type: ignore[import-untyped]

from ormy.base.abc import ExtensionABC

from .config import RabbitMQConfig

# ----------------------- #

R = TypeVar("R", bound="RabbitMQExtension")
T = TypeVar("T")

# ----------------------- #


class RabbitMQExtension(ExtensionABC):
    """RabbitMQ extension"""

    extension_configs: ClassVar[List[Any]] = [RabbitMQConfig()]

    __rmq_static: ClassVar[Optional[pika.BlockingConnection]] = None
    __armq_static: ClassVar[Optional[aio_pika.abc.AbstractRobustConnection]] = None

    # ....................... #

    def __init_subclass__(cls: Type[R], **kwargs):
        """Initialize subclass"""

        super().__init_subclass__(**kwargs)

        cls._register_extension_subclass_helper(
            config=RabbitMQConfig,
            discriminator=["queue"],
        )

    # ....................... #

    @classmethod
    def _get_rmq_queue(cls: Type[R]):
        """Get queue"""

        cfg = cls.get_extension_config(type_=RabbitMQConfig)
        queue = cfg.queue

        return queue

    # ....................... #

    @classmethod
    def __is_static_rmq(cls: Type[R]):
        """Check if static RabbitMQ client is used"""

        cfg = cls.get_extension_config(type_=RabbitMQConfig)
        use_static = not cfg.context_client

        return use_static

    # ....................... #

    @classmethod
    def __rmq_static_connection(cls):
        """
        Get RabbitMQ connection

        Returns:
            connection (pika.BlockingConnection): RabbitMQ connection
        """

        if cls.__rmq_static is None:
            cfg = cls.get_extension_config(type_=RabbitMQConfig)
            url = cfg.url()
            cls.__rmq_static = pika.BlockingConnection(pika.URLParameters(url))

        return cls.__rmq_static

    # ....................... #

    @classmethod
    async def __armq_static_connection(cls):
        """
        Get async RabbitMQ connection

        Returns:
            connection (aio_pika.abc.AbstractRobustConnection): async RabbitMQ connection
        """

        if cls.__armq_static is None:
            cfg = cls.get_extension_config(type_=RabbitMQConfig)
            url = cfg.url()
            cls.__armq_static = await aio_pika.connect_robust(url)

        return cls.__armq_static

    # ....................... #

    @classmethod
    @contextmanager
    def __rmq_connection(cls):
        """
        Get RabbitMQ connection

        Yields:
            connection (pika.BlockingConnection): RabbitMQ connection
        """

        cfg = cls.get_extension_config(type_=RabbitMQConfig)
        url = cfg.url()
        rmq = pika.BlockingConnection(pika.URLParameters(url))

        try:
            yield rmq

        finally:
            rmq.close()

    # ....................... #

    @classmethod
    @asynccontextmanager
    async def __armq_connection(cls):
        """
        Get async RabbitMQ connection

        Yields:
            connection (aio_pika.abc.AbstractRobustConnection): async RabbitMQ connection
        """

        cfg = cls.get_extension_config(type_=RabbitMQConfig)
        url = cfg.url()
        rmq = await aio_pika.connect_robust(url)

        try:
            yield rmq

        finally:
            await rmq.close()

    # ....................... #

    @classmethod
    @contextmanager
    def __rmq_channel(cls):
        """
        Get syncronous RabbitMQ channel

        Yields:
            channel (pika.BlockingConnection): RabbitMQ channel
        """

        if cls.__is_static_rmq():
            connection = cls.__rmq_static_connection()
            channel = connection.channel()

        else:
            with cls.__rmq_connection() as connection:
                channel = connection.channel()

        try:
            yield channel

        finally:
            channel.close()

    # ....................... #

    @classmethod
    @asynccontextmanager
    async def __armq_channel(cls):
        """
        Get asyncronous RabbitMQ channel

        Yields:
            channel (aio_pika.abc.AbstractRobustConnection): async RabbitMQ channel
        """

        if cls.__is_static_rmq():
            connection = await cls.__armq_static_connection()
            channel = await connection.channel()

        else:
            async with cls.__armq_connection() as connection:
                channel = await connection.channel()

        try:
            yield channel

        finally:
            await channel.close()

    # ....................... #

    @classmethod
    def rmq_publish(cls, message: str):
        """
        Publish message to RabbitMQ

        Args:
            message (str): Message to publish
        """

        queue = cls._get_rmq_queue()

        with cls.__rmq_channel() as channel:
            channel.basic_publish(
                exchange=queue,
                routing_key=queue,
                body=message.encode(),
            )

    # ....................... #

    @classmethod
    async def armq_publish(cls, message: str):
        """
        Publish message to RabbitMQ

        Args:
            message (str): Message to publish
        """

        queue = cls._get_rmq_queue()

        async with cls.__armq_channel() as channel:
            exchange = await channel.declare_exchange(
                name=queue,
                auto_delete=True,
            )
            q = await channel.declare_queue(
                name=queue,
                auto_delete=True,
            )
            await q.bind(exchange, queue)

            await exchange.publish(
                message=aio_pika.Message(body=message.encode()),
                routing_key=queue,
            )
