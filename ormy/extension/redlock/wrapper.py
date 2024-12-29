from contextlib import asynccontextmanager, contextmanager
from typing import Any, ClassVar, List, Type, TypeVar, cast

from aioredlock import Aioredlock  # type: ignore[import-untyped]
from redlock import RedLockFactory  # type: ignore[import-untyped]

from ormy.base.abc import ExtensionABC
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
            discriminator="index",
        )

    # ....................... #

    @classmethod
    @contextmanager
    def _redlock_manager(cls):
        """Get syncronous Redlock manager"""

        cfg = cls.get_extension_config(type_=RedlockConfig)
        url = cfg.url()
        m = None

        try:
            m = cast(RedLockFactory, RedLockFactory([url]))
            yield m

        finally:
            pass

    # ....................... #

    @classmethod
    @asynccontextmanager
    async def _aredlock_manager(cls):
        """Get asyncronous Redlock manager"""

        cfg = cls.get_extension_config(type_=RedlockConfig)
        url = cfg.url()
        m = None

        try:
            m = cast(Aioredlock, Aioredlock([url]))  # type: ignore
            yield m

        finally:
            if m:
                await m.destroy()

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
    def redlock_cls(cls, id_: str, **kwargs):
        """Get Redlock"""

        col = cls._get_redlock_collection()
        resource = f"{col}.{id_}"

        with cls._redlock_manager() as m:
            lock = m.create_lock(resource, **kwargs)

            try:
                yield lock

            finally:
                if lock:
                    lock.release()

    # ....................... #

    @classmethod
    @asynccontextmanager
    async def aredlock_cls(cls, id_: str, **kwargs):
        """Get asyncronous Redlock"""

        col = cls._get_redlock_collection()
        resource = f"{col}.{id_}"

        async with cls._aredlock_manager() as m:
            lock = await m.lock(resource, **kwargs)

            try:
                yield lock

            finally:
                await m.unlock(lock)
