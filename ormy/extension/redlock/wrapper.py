import asyncio
import threading
import time
import uuid
from contextlib import asynccontextmanager, contextmanager
from typing import Any, ClassVar, List, Optional, Tuple, Type, TypeVar

from redis import Redis
from redis import asyncio as aioredis

from ormy.base.abc import ExtensionABC
from ormy.base.error import BadInput, Conflict, InternalError
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
    def redlock_cls(  # TODO: exponential backoff, retry logic
        cls,
        id_: str,
        timeout: int = 10,
        extend_interval: int = 5,
    ):
        """
        Lock entity instance with automatic extension

        Args:
            id_ (str): The unique identifier of the entity.
            timeout (int): The timeout for the lock in seconds.
            extend_interval (int): The interval to extend the lock in seconds.

        Yields:
            result (bool): True if the lock was acquired, False otherwise.

        Raises:
            Conflict: If the lock already exists.
            BadInput: If the timeout or extend_interval is not greater than 0 or extend_interval is not less than timeout.
            InternalError: If the lock aquisition or extension fails.
        """

        if timeout <= 0:
            raise BadInput("timeout must be greater than 0")

        if extend_interval <= 0:
            raise BadInput("extend_interval must be greater than 0")

        if extend_interval >= timeout:
            raise BadInput("extend_interval must be less than timeout")

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

        extend_task = None
        stop_extend = threading.Event()

        def extend_lock_periodically(resource: str, unique_id: str):
            try:
                while not stop_extend.is_set():
                    time.sleep(extend_interval)
                    success = cls._extend_lock(
                        key=resource,
                        unique_id=unique_id,
                        additional_time=timeout,
                    )
                    if not success:
                        raise InternalError(f"Failed to extend lock for {resource}")
            except Exception as e:
                raise InternalError(f"Error in lock extension: {e}")

        try:
            extend_task = threading.Thread(
                target=extend_lock_periodically,
                kwargs={
                    "resource": resource,
                    "unique_id": unique_id,
                },
                daemon=True,
            )
            extend_task.start()

            yield result

        finally:
            stop_extend.set()

            if extend_task:
                extend_task.join()

            if result and unique_id:
                cls._release_lock(
                    key=resource,
                    unique_id=unique_id,
                )

    # ....................... #

    @classmethod
    @asynccontextmanager
    async def aredlock_cls(  # TODO: exponential backoff, retry logic
        cls,
        id_: str,
        timeout: int = 10,
        extend_interval: int = 5,
    ):
        """
        Lock entity instance with automatic extension

        Args:
            id_ (str): The unique identifier of the entity.
            timeout (int): The timeout for the lock in seconds.
            extend_interval (int): The interval to extend the lock in seconds.

        Yields:
            result (bool): True if the lock was acquired, False otherwise.

        Raises:
            Conflict: If the lock already exists.
            BadInput: If the timeout or extend_interval is not greater than 0 or extend_interval is not less than timeout.
            InternalError: If the lock aquisition or extension fails.
        """

        if timeout <= 0:
            raise BadInput("timeout must be greater than 0")

        if extend_interval <= 0:
            raise BadInput("extend_interval must be greater than 0")

        if extend_interval >= timeout:
            raise BadInput("extend_interval must be less than timeout")

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

        if not unique_id:
            raise InternalError(f"Failed to acquire lock for {resource}")

        extend_task = None
        stop_extend = asyncio.Event()

        async def extend_lock_periodically(resource: str, unique_id: str):
            try:
                while not stop_extend.is_set():
                    await asyncio.sleep(extend_interval)
                    success = await cls._aextend_lock(
                        key=resource,
                        unique_id=unique_id,
                        additional_time=timeout,
                    )
                    if not success:
                        raise InternalError(f"Failed to extend lock for {resource}")

            except asyncio.CancelledError:
                pass

        try:
            extend_task = asyncio.create_task(
                extend_lock_periodically(
                    resource=resource,
                    unique_id=unique_id,
                )
            )

            yield result

        finally:
            stop_extend.set()

            if extend_task:
                extend_task.cancel()
                try:
                    await extend_task
                except asyncio.CancelledError:
                    pass

            if result and unique_id:
                await cls._arelease_lock(
                    key=resource,
                    unique_id=unique_id,
                )
