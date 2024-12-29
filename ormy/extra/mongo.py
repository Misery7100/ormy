import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager, contextmanager
from typing import Any, ClassVar, Dict, List, Optional, Self, Type, TypeVar

from ormy.extension.meilisearch import (
    MeilisearchConfig,
    MeilisearchExtension,
    MeilisearchExtensionV2,
)
from ormy.extension.redlock import RedlockConfig, RedlockCustomExtension
from ormy.extension.s3 import S3Config, S3Extension
from ormy.service.mongo import MongoBase, MongoConfig, MongoSingleBase

# ----------------------- #

MwMb = TypeVar("MwMb", bound="MongoWithMeilisearchBackground")
MwM = TypeVar("MwM", bound="MongoWithMeilisearch")
M = TypeVar("M", bound="MongoWithMeilisearchBackgroundV2")
S = TypeVar("S", bound="MongoMeilisearchS3")
R = TypeVar("R", bound="MongoMeilisearchS3Redlock")

# ----------------------- #


class MongoWithMeilisearchBackground(MongoBase, MeilisearchExtension):
    configs = [MongoConfig(), MeilisearchConfig()]

    # ....................... #

    def save(self: MwMb) -> MwMb:
        super().save()

        # Run in background
        with ThreadPoolExecutor() as executor:
            executor.submit(self.meili_update_documents, self)

        return self

    # ....................... #

    async def asave(self: MwMb) -> MwMb:
        await super().asave()

        # Run in background
        asyncio.create_task(self.ameili_update_documents(self))

        return self

    # ....................... #

    @classmethod
    def create(cls: Type[MwMb], data: MwMb) -> MwMb:
        res = super().create(data)  # type: ignore

        # Run in background
        with ThreadPoolExecutor() as executor:
            executor.submit(cls.meili_update_documents, res)

        return res

    # ....................... #

    @classmethod
    async def acreate(cls: Type[MwMb], data: MwMb) -> MwMb:
        res = await super().acreate(data)  # type: ignore

        # Run in background
        asyncio.create_task(cls.ameili_update_documents(res))

        return res

    # ....................... #

    @classmethod
    def create_many(
        cls: Type[MwMb],
        data: List[MwMb],
        ordered: bool = False,
    ):
        super().create_many(data, ordered=ordered)  # type: ignore

        # Run in background
        with ThreadPoolExecutor() as executor:
            executor.submit(cls.meili_update_documents, data)

    # ....................... #

    @classmethod
    async def acreate_many(
        cls: Type[MwMb],
        data: List[MwMb],
        ordered: bool = False,
    ):
        await super().acreate_many(data, ordered=ordered)  # type: ignore

        # Run in background
        asyncio.create_task(cls.ameili_update_documents(data))


# ----------------------- #


class MongoWithMeilisearch(MongoBase, MeilisearchExtension):
    configs = [MongoConfig(), MeilisearchConfig()]

    # ....................... #

    def save(self: MwM) -> MwM:
        super().save()
        self.meili_update_documents(self)

        return self

    # ....................... #

    async def asave(self: MwM) -> MwM:
        await super().asave()
        await self.ameili_update_documents(self)

        return self

    # ....................... #

    @classmethod
    def create(cls: Type[MwM], data: MwM) -> MwM:
        res = super().create(data)  # type: ignore
        cls.meili_update_documents(res)

        return res

    # ....................... #

    @classmethod
    async def acreate(cls: Type[MwM], data: MwM) -> MwM:
        res = await super().acreate(data)  # type: ignore
        await cls.ameili_update_documents(res)

        return res

    # ....................... #

    @classmethod
    def create_many(
        cls: Type[MwM],
        data: List[MwM],
        ordered: bool = False,
    ):
        super().create_many(data, ordered=ordered)  # type: ignore
        cls.meili_update_documents(data)

    # ....................... #

    @classmethod
    async def acreate_many(
        cls: Type[MwM],
        data: List[MwM],
        ordered: bool = False,
    ):
        await super().acreate_many(data, ordered=ordered)  # type: ignore
        await cls.ameili_update_documents(data)


# ----------------------- #


class MongoWithMeilisearchBackgroundV2(MongoSingleBase, MeilisearchExtensionV2):
    config: ClassVar[MongoConfig] = MongoConfig()
    extension_configs: ClassVar[List[Any]] = [MeilisearchConfig()]

    # ....................... #

    def __init_subclass__(cls: Type[M], **kwargs):
        try:
            cfg_meili = cls.get_extension_config(type_=MeilisearchConfig)

        except ValueError:
            cfg_meili = MeilisearchConfig()

        other_ext_configs = [x for x in cls.extension_configs if x not in [cfg_meili]]

        # Prevent overriding if mongo config is default
        if not cls.config.is_default():
            cfg_meili.index = f"{cls.config.database}-{cls.config.collection}"

        cls.extension_configs = [cfg_meili] + other_ext_configs

        super().__init_subclass__(**kwargs)

    # ....................... #

    def save(self: M) -> M:
        res = super().save()

        # Run in background
        with ThreadPoolExecutor() as executor:
            executor.submit(self.meili_update_documents, res)

        return res

    # ....................... #

    async def asave(self: M) -> M:
        res = await super().asave()

        # Run in background
        asyncio.create_task(self.ameili_update_documents(res))

        return res

    # ....................... #

    @classmethod
    def create(cls: Type[M], data: M) -> M:
        res = super().create(data)  # type: ignore

        # Run in background
        with ThreadPoolExecutor() as executor:
            executor.submit(cls.meili_update_documents, res)

        return res

    # ....................... #

    @classmethod
    async def acreate(cls: Type[M], data: M) -> M:
        res = await super().acreate(data)  # type: ignore

        # Run in background
        asyncio.create_task(cls.ameili_update_documents(res))

        return res

    # ....................... #

    @classmethod
    def create_many(
        cls: Type[M],
        data: List[M],
        ordered: bool = False,
    ):
        super().create_many(data, ordered=ordered)  # type: ignore

        # Run in background
        with ThreadPoolExecutor() as executor:
            executor.submit(cls.meili_update_documents, data)

    # ....................... #

    @classmethod
    async def acreate_many(
        cls: Type[M],
        data: List[M],
        ordered: bool = False,
    ):
        await super().acreate_many(data, ordered=ordered)  # type: ignore

        # Run in background
        asyncio.create_task(cls.ameili_update_documents(data))


# ----------------------- #


class MongoMeilisearchS3(MongoWithMeilisearchBackgroundV2, S3Extension):
    config: ClassVar[MongoConfig] = MongoConfig()
    extension_configs: ClassVar[List[Any]] = [MeilisearchConfig(), S3Config()]

    # ....................... #

    def __init_subclass__(cls: Type[S], **kwargs):
        try:
            cfg_s3 = cls.get_extension_config(type_=S3Config)

        except ValueError:
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


# ----------------------- #


class MongoMeilisearchS3Redlock(MongoMeilisearchS3, RedlockCustomExtension):
    config: ClassVar[MongoConfig] = MongoConfig()
    extension_configs: ClassVar[List[Any]] = [
        MeilisearchConfig(),
        S3Config(),
        RedlockConfig(),
    ]

    # ....................... #

    def __init_subclass__(cls: Type[R], **kwargs):
        try:
            cfg_redlock = cls.get_extension_config(type_=RedlockConfig)

        except ValueError:
            cfg_redlock = RedlockConfig()

        other_ext_configs = [x for x in cls.extension_configs if x not in [cfg_redlock]]

        # Prevent overriding if mongo config is default
        if not cls.config.is_default():
            cfg_redlock.collection = f"{cls.config.database}_{cls.config.collection}"

        cls.extension_configs = [cfg_redlock] + other_ext_configs

        super().__init_subclass__(**kwargs)

    # ....................... #

    @contextmanager
    def redlock(self, **kwargs):
        """Get Redlock"""

        with self.redlock_cls(id_=str(self.id), **kwargs) as res:
            yield res

    # ....................... #

    @asynccontextmanager
    async def aredlock(self, **kwargs):
        """Get asyncronous Redlock"""
        async with self.aredlock_cls(id_=str(self.id), **kwargs) as res:
            yield res
