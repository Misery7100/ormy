from contextlib import asynccontextmanager, contextmanager
from typing import Any, ClassVar, List

from ormy.base.error import InternalError
from ormy.extension.meilisearch import MeilisearchConfig, MeilisearchExtensionV2
from ormy.extension.redlock import RedlockConfig, RedlockExtension
from ormy.service.redis import RedisConfig, RedisSingleBase

# ----------------------- #


class MeilisearchBoilerplate(RedisSingleBase, MeilisearchExtensionV2):
    config: ClassVar[RedisConfig] = RedisConfig()
    extension_configs: ClassVar[List[Any]] = [MeilisearchConfig()]

    # ....................... #

    def __init_subclass__(cls, **kwargs):
        """Initialize subclass"""

        try:
            cfg_meili = cls.get_extension_config(type_=MeilisearchConfig)

        except InternalError:
            cfg_meili = MeilisearchConfig()

        other_ext_configs = [x for x in cls.extension_configs if x not in [cfg_meili]]

        # Prevent overriding if mongo config is default
        if not cls.config.is_default():
            cfg_meili.index = f"{cls.config.database}-{cls.config.collection}"

        cls.extension_configs = [cfg_meili] + other_ext_configs

        super().__init_subclass__(**kwargs)


# ....................... #


class RedlockBoilerplate(RedisSingleBase, RedlockExtension):
    config: ClassVar[RedisConfig] = RedisConfig()
    extension_configs: ClassVar[List[Any]] = [RedlockConfig()]

    # ....................... #

    def __init_subclass__(cls, **kwargs):
        """Initialize subclass"""

        try:
            cfg_redlock = cls.get_extension_config(type_=RedlockConfig)

        except InternalError:
            cfg_redlock = RedlockConfig()

        other_ext_configs = [x for x in cls.extension_configs if x not in [cfg_redlock]]

        # Prevent overriding if mongo config is default
        if not cls.config.is_default():
            cfg_redlock.collection = f"{cls.config.database}-{cls.config.collection}"

        cls.extension_configs = [cfg_redlock] + other_ext_configs

        super().__init_subclass__(**kwargs)

    # ....................... #

    @contextmanager
    def lock(
        self,
        timeout: int = 10,
        extend_interval: int = 5,
        auto_extend: bool = True,
    ):
        """
        Lock entity instance with automatic extension

        Args:
            timeout (int): The timeout for the lock in seconds.
            extend_interval (int): The interval to extend the lock in seconds.
            auto_extend (bool): Whether to automatically extend the lock.

        Yields:
            result (bool): True if the lock was acquired, False otherwise.

        Raises:
            Conflict: If the lock already exists.
            BadRequest: If the timeout or extend_interval is not greater than 0 or extend_interval is not less than timeout.
            InternalError: If the lock aquisition or extension fails.
        """

        with self.redlock_cls(
            id_=str(self.id),
            timeout=timeout,
            extend_interval=extend_interval,
            auto_extend=auto_extend,
        ) as res:
            yield res

    # ....................... #

    @asynccontextmanager
    async def alock(
        self,
        timeout: int = 10,
        extend_interval: int = 5,
        auto_extend: bool = True,
    ):
        """
        Lock entity instance with automatic extension

        Args:
            timeout (int): The timeout for the lock in seconds.
            extend_interval (int): The interval to extend the lock in seconds.
            auto_extend (bool): Whether to automatically extend the lock.

        Yields:
            result (bool): True if the lock was acquired, False otherwise.

        Raises:
            Conflict: If the lock already exists.
            BadRequest: If the timeout or extend_interval is not greater than 0 or extend_interval is not less than timeout.
            InternalError: If the lock aquisition or extension fails.
        """

        async with self.aredlock_cls(
            id_=str(self.id),
            timeout=timeout,
            extend_interval=extend_interval,
            auto_extend=auto_extend,
        ) as res:
            yield res
