import json
from contextlib import asynccontextmanager, contextmanager
from typing import Any, ClassVar, Dict, List, Optional, Type, TypeVar

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

    __rmq: ClassVar[Optional[pika.BlockingConnection]] = None
    __armq: ClassVar[Optional[aio_pika.abc.AbstractRobustConnection]] = None

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
    def __rmq_connection(cls):
        """
        Get RabbitMQ connection

        Returns:
            connection (pika.BlockingConnection): RabbitMQ connection
        """

        if cls.__rmq is None:
            cfg = cls.get_extension_config(type_=RabbitMQConfig)
            url = cfg.url()
            cls.__rmq = pika.BlockingConnection(pika.URLParameters(url))

        return cls.__rmq

    # ....................... #

    @classmethod
    async def __armq_connection(cls):
        """
        Get async RabbitMQ connection

        Returns:
            connection (aio_pika.abc.AbstractRobustConnection): async RabbitMQ connection
        """

        if cls.__armq is None:
            cfg = cls.get_extension_config(type_=RabbitMQConfig)
            url = cfg.url()
            cls.__armq = await aio_pika.connect_robust(url)

        return cls.__armq

    # ....................... #

    @classmethod
    @contextmanager
    def __rmq_channel(cls):
        """
        Get syncronous RabbitMQ channel

        Yields:
            channel (pika.BlockingConnection): RabbitMQ channel
        """

        connection = cls.__rmq_connection()
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

        connection = await cls.__armq_connection()
        channel = await connection.channel()

        try:
            yield channel

        finally:
            await channel.close()

    # ....................... #

    @classmethod
    def _rmq_publish(
        cls,
        queue: str,
        message: Any,
        headers: Optional[Dict[str, Any]] = None,
        delivery_mode: int = 2,
    ):
        """
        Publish message to RabbitMQ

        Args:
            queue (str): Queue to publish to
            message (Any): Message to publish (JSON serializable)
            headers (Dict[str, Any]): Headers to publish
            delivery_mode (int): Delivery mode (2 for persistent)
        """

        with cls.__rmq_channel() as channel:
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
        headers: Optional[Dict[str, Any]] = None,
        delivery_mode: int = 2,
    ):
        """
        Publish message to RabbitMQ

        Args:
            queue (str): Queue to publish to
            message (Any): Message to publish (JSON serializable)
            headers (Dict[str, Any]): Headers to publish
            delivery_mode (int): Delivery mode (2 for persistent)
        """

        async with cls.__armq_channel() as channel:
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
        headers: Optional[Dict[str, Any]] = None,
        delivery_mode: int = 2,
    ):
        """
        Publish message to RabbitMQ

        Args:
            message (Any): Message to publish (JSON serializable)
            headers (Dict[str, Any]): Headers to publish
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
        headers: Optional[Dict[str, Any]] = None,
        delivery_mode: int = 2,
    ):
        """
        Publish message to RabbitMQ

        Args:
            message (Any): Message to publish (JSON serializable)
            headers (Dict[str, Any]): Headers to publish
            delivery_mode (int): Delivery mode (2 for persistent)
        """

        queue = cls._get_rmq_queue()
        return await cls._armq_publish(
            queue=queue,
            message=message,
            headers=headers,
            delivery_mode=delivery_mode,
        )
