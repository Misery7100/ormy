from contextlib import asynccontextmanager, contextmanager
from typing import Any, ClassVar, Dict, List, Optional, Self

from ormy.base.error import InternalError
from ormy.extension.meilisearch import MeilisearchConfig, MeilisearchExtensionV2
from ormy.extension.rabbitmq import RabbitMQConfig, RabbitMQExtension
from ormy.extension.redlock import RedlockConfig, RedlockExtension
from ormy.extension.s3 import S3Config, S3Extension
from ormy.service.arango import ArangoBase, ArangoConfig

# ----------------------- #


class ArangoMeilisearchBoilerplate(ArangoBase, MeilisearchExtensionV2):
    config: ClassVar[ArangoConfig] = ArangoConfig()
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


class ArangoS3Boilerplate(ArangoBase, S3Extension):
    config: ClassVar[ArangoConfig] = ArangoConfig()
    extension_configs: ClassVar[List[Any]] = [S3Config()]

    # ....................... #

    def __init_subclass__(cls, **kwargs):
        """Initialize subclass"""

        try:
            cfg_s3 = cls.get_extension_config(type_=S3Config)

        except InternalError:
            cfg_s3 = S3Config()

        other_ext_configs = [x for x in cls.extension_configs if x not in [cfg_s3]]

        # Prevent overriding if mongo config is default
        if not cls.config.is_default():
            cfg_s3.bucket = f"{cls.config.database}-{cls.config.collection}"

        cls.extension_configs = [cfg_s3] + other_ext_configs

        super().__init_subclass__(**kwargs)

    # ....................... #

    def add_file(
        self: Self,
        file: bytes,
        name: str,
        avoid_duplicates: bool = True,
        tags: Dict[str, str] = {},
    ):
        """
        Add an attachment to the entity

        Args:
            file (bytes): The file to add
            name (str): The name of the file
            avoid_duplicates (bool): Whether to avoid duplicates
            tags (Dict[str, str]): The tags to add to the file

        Returns:
            result (str): The key of the attachment
        """

        key = f"{self.id}/{name}"

        f = self.s3_upload_file(
            key=key,
            file=file,
            avoid_duplicates=avoid_duplicates,
        )

        if tags:
            self.s3_add_file_tags(
                key=key,
                tags=tags,
            )

        return f

    # ....................... #

    def download_file(self: Self, name: str):
        """
        Download an attachment from the entity

        Args:
            name (str): The name of the attachment to download

        Returns:
            result (bytes): The attachment
        """

        key = f"{self.id}/{name}"
        data = self.s3_download_file(key=key)

        return data["Body"]

    # ....................... #

    def remove_file(self: Self, name: str):
        """
        Remove a file from the entity

        Args:
            name (str): The name of the attachment to remove
        """

        key = f"{self.id}/{name}"

        return self.s3_delete_file(key=key)

    # ....................... #

    def list_files(
        self: Self,
        page: int = 1,
        size: int = 20,
    ):
        """
        List files from the entity

        Args:
            page (int): The page number
            size (int): The page size

        Returns:
            result (ormy.base.pydantic.TableResponse): The attachments
        """

        return self.s3_list_files(
            blob=str(self.id),
            page=page,
            size=size,
        )

    # ....................... #

    def file_exists(self: Self, name: Optional[str] = None) -> bool:
        """
        Check if a file exists

        Args:
            name (str): The name of the file to check

        Returns:
            result (bool): Whether the file exists
        """

        if not name:
            return False

        key: str = f"{self.id}/{name}"

        return self.s3_file_exists(key=key)


# ....................... #


class ArangoRedlockBoilerplate(ArangoBase, RedlockExtension):
    config: ClassVar[ArangoConfig] = ArangoConfig()
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
            InternalError: If the timeout or extend_interval is not greater than 0 or extend_interval is not less than timeout or the lock aquisition or extension fails.
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
            InternalError: If the timeout or extend_interval is not greater than 0 or extend_interval is not less than timeout or the lock aquisition or extension fails.
        """

        async with self.aredlock_cls(
            id_=str(self.id),
            timeout=timeout,
            extend_interval=extend_interval,
            auto_extend=auto_extend,
        ) as res:
            yield res


# ....................... #


class ArangoRabbitMQBoilerplate(ArangoBase, RabbitMQExtension):
    config: ClassVar[ArangoConfig] = ArangoConfig()
    extension_configs: ClassVar[List[Any]] = [RabbitMQConfig()]

    # ....................... #

    def __init_subclass__(cls, **kwargs):
        """Initialize subclass"""

        try:
            cfg_rmq = cls.get_extension_config(type_=RabbitMQConfig)

        except InternalError:
            cfg_rmq = RabbitMQConfig()

        other_ext_configs = [x for x in cls.extension_configs if x not in [cfg_rmq]]

        # Prevent overriding if mongo config is default
        if not cls.config.is_default():
            cfg_rmq.queue = f"{cls.config.database}-{cls.config.collection}"

        cls.extension_configs = [cfg_rmq] + other_ext_configs

        super().__init_subclass__(**kwargs)
