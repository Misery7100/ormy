import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any, ClassVar, List, Type, TypeVar

from ormy.extension.meilisearch import (
    MeilisearchConfig,
    MeilisearchExtension,
    MeilisearchExtensionV2,
)
from ormy.extension.s3 import S3Config, S3Extension
from ormy.service.mongo import MongoBase, MongoConfig

# ----------------------- #

MwMb = TypeVar("MwMb", bound="MongoWithMeilisearchBackground")
MwM = TypeVar("MwM", bound="MongoWithMeilisearch")
M = TypeVar("M", bound="MongoWithMeilisearchBackgroundV2")
S = TypeVar("S", bound="MongoMeilisearchS3")

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


class MongoWithMeilisearchBackgroundV2(MongoBase, MeilisearchExtensionV2):
    config: ClassVar[MongoConfig] = MongoConfig()
    extension_configs: ClassVar[List[Any]] = [MeilisearchConfig()]

    # ....................... #

    def __init_subclass__(cls: Type[M], **kwargs):
        try:
            cfg_meili = cls.get_extension_config(type_=MeilisearchConfig)

        except ValueError:
            cfg_meili = MeilisearchConfig()

        other_ext_configs = [x for x in cls.extension_configs if x not in [cfg_meili]]

        # Prevent overriding default meilisearch index if mongo config is default
        if not cls.config.is_default():
            cfg_meili.index = f"{cls.config.database}__{cls.config.collection}"

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

        # Prevent overriding default s3 bucket if mongo config is default
        if not cls.config.is_default():
            cfg_s3.bucket = f"{cls.config.database}__{cls.config.collection}"

        cls.extension_configs = [cfg_s3] + other_ext_configs

        super().__init_subclass__(**kwargs)
