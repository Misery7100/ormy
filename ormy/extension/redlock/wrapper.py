import uuid
from contextlib import asynccontextmanager, contextmanager
from typing import Any, ClassVar, List, Optional, Tuple, Type, TypeVar

from redis import Redis
from redis import asyncio as aioredis

from ormy.base.abc import ExtensionABC
from ormy.base.error import Conflict
from ormy.utils.logging import LogLevel, console_logger

from .config import RedlockConfig

# ----------------------- #

R = TypeVar("R", bound="RedlockExtension")
logger = console_logger(__name__, level=LogLevel.INFO)

# ----------------------- #


class RedlockExtension(ExtensionABC):
    """
    Redlock extension
    """

    extension_configs: ClassVar[List[Any]] = [RedlockConfig()]
    _registry = {RedlockConfig: {}}

    # ....................... #

    def __init_subclass__(cls: Type[R], **kwargs):
        super().__init_subclass__(**kwargs)

        cls._redlock_register_subclass()
        cls._merge_registry()

        RedlockExtension._registry = cls._merge_registry_helper(
            RedlockExtension._registry,
            cls._registry,
        )

    # ....................... #

    @classmethod
    def _redlock_register_subclass(cls: Type[R]):
        """Register subclass in the registry"""

        return cls._register_subclass_helper(
            config=RedlockConfig,
            discriminator="collection",
        )

    # ....................... #

    @classmethod
    def _get_redlock_collection(cls: Type[R]):
        """Get collection"""

        cfg = cls.get_extension_config(type_=RedlockConfig)
        col = cfg.collection

        return col

    # ....................... #

    @classmethod
    @contextmanager
    def _redlock_client(cls):
        """Get syncronous Redis client for lock purposes"""

        cfg = cls.get_extension_config(type_=RedlockConfig)
        url = cfg.url()
        r = Redis.from_url(url, decode_responses=True)

        try:
            yield r

        finally:
            r.close()

    # ....................... #

    @classmethod
    @asynccontextmanager
    async def _aredlock_client(cls):
        """Get asyncronous Redis client for lock purposes"""

        cfg = cls.get_extension_config(type_=RedlockConfig)
        url = cfg.url()
        r = aioredis.from_url(url, decode_responses=True)

        try:
            yield r

        finally:
            await r.close()

    # ....................... #

    @classmethod
    def _acquire_lock(
        cls,
        key: str,
        unique_id: Optional[str] = None,
        timeout: int = 10,
    ) -> Tuple[Optional[bool], Optional[str]]:
        """
        Acquire a lock with a unique identifier.

        Args:
            key (str): The Redis key for the lock.
            unique_id (str, optional): A unique identifier for this lock holder.
            timeout (int): The timeout for the lock in seconds.

        Returns:
            result (bool): True if the lock was acquired, False otherwise.
            unique_id (str): The unique identifier for this lock holder.
        """

        unique_id = unique_id or str(uuid.uuid4())

        with cls._redlock_client() as r:
            result = r.set(
                key,
                unique_id,
                nx=True,
                ex=timeout,
            )

        return result, unique_id if result else None

    # ....................... #

    @classmethod
    async def _aacquire_lock(
        cls,
        key: str,
        unique_id: Optional[str] = None,
        timeout: int = 10,
    ) -> Tuple[Optional[bool], Optional[str]]:
        """
        Acquire a lock with a unique identifier.

        Args:
            key (str): The Redis key for the lock.
            unique_id (str, optional): A unique identifier for this lock holder.
            timeout (int): The timeout for the lock in seconds.

        Returns:
            result (bool): True if the lock was acquired, False otherwise.
            unique_id (str): The unique identifier for this lock holder.
        """

        unique_id = unique_id or str(uuid.uuid4())

        async with cls._aredlock_client() as r:
            result = await r.set(
                key,
                unique_id,
                nx=True,
                ex=timeout,
            )

        return result, unique_id if result else None

    # ....................... #

    @classmethod
    def _release_lock(cls, key: str, unique_id: str) -> bool:
        """
        Release the lock if the unique identifier matches.

        Args:
            key (str): The Redis key for the lock.
            unique_id (str): The unique identifier of the lock holder.

        Returns:
            result (bool): True if the lock was released, False otherwise.
        """

        script = """
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("DEL", KEYS[1])
        else
            return 0
        end
        """

        with cls._redlock_client() as r:
            result = r.eval(
                script,
                1,
                key,
                unique_id,
            )

        return result

    # ....................... #

    @classmethod
    async def _arelease_lock(cls, key: str, unique_id: str) -> bool:
        """
        Release the lock if the unique identifier matches.

        Args:
            key (str): The Redis key for the lock.
            unique_id (str): The unique identifier of the lock holder.

        Returns:
            result (bool): True if the lock was released, False otherwise.
        """

        script = """
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("DEL", KEYS[1])
        else
            return 0
        end
        """

        async with cls._aredlock_client() as r:
            result = await r.eval(
                script,
                1,
                key,
                unique_id,
            )

        return result

    # ....................... #

    @classmethod
    def _extend_lock(
        cls,
        key: str,
        unique_id: str,
        additional_time: int,
    ) -> bool:
        """
        Extend the lock expiration if the unique identifier matches.

        Args:
            key (str): The Redis key for the lock.
            unique_id (str): The unique identifier of the lock holder.
            additional_time (int): The additional time to extend the lock in seconds.

        Returns:
            result (bool): True if the lock was extended, False otherwise.
        """

        script = """
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("EXPIRE", KEYS[1], ARGV[2])
        else
            return 0
        end
        """

        with cls._redlock_client() as r:
            result = r.eval(
                script,
                1,
                key,
                unique_id,
                additional_time,
            )

        return result == 1

    # ....................... #

    @classmethod
    async def _aextend_lock(
        cls,
        key: str,
        unique_id: str,
        additional_time: int,
    ) -> bool:
        """
        Extend the lock expiration if the unique identifier matches.

        Args:
            key (str): The Redis key for the lock.
            unique_id (str): The unique identifier of the lock holder.
            additional_time (int): The additional time to extend the lock in seconds.

        Returns:
            result (bool): True if the lock was extended, False otherwise.
        """

        script = """
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("EXPIRE", KEYS[1], ARGV[2])
        else
            return 0
        end
        """

        async with cls._aredlock_client() as r:
            result = await r.eval(
                script,
                1,
                key,
                unique_id,
                additional_time,
            )

        return result == 1

    # ....................... #

    @classmethod
    @contextmanager
    def redlock_cls(
        cls,
        id_: str,
        timeout: int = 10,
    ):
        """Get Redlock"""

        col = cls._get_redlock_collection()
        resource = f"{col}.{id_}"
        result, unique_id = cls._acquire_lock(
            key=resource,
            timeout=timeout,
        )

        if not result:
            raise Conflict(
                f"{resource} already locked",
            )

        try:
            yield result

        finally:
            if result and unique_id:
                cls._release_lock(
                    key=resource,
                    unique_id=unique_id,
                )

    # ....................... #

    @classmethod
    @asynccontextmanager
    async def aredlock_cls(
        cls,
        id_: str,
        timeout: int = 10,
    ):
        """Get Redlock"""

        col = cls._get_redlock_collection()
        resource = f"{col}.{id_}"
        result, unique_id = await cls._aacquire_lock(
            key=resource,
            timeout=timeout,
        )

        if not result:
            raise Conflict(
                f"{resource} already locked",
            )

        try:
            yield result

        finally:
            if result and unique_id:
                await cls._arelease_lock(
                    key=resource,
                    unique_id=unique_id,
                )
