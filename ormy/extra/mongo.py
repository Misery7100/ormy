import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import List, Type, TypeVar

from ormy.extension.meilisearch import MeilisearchConfig, MeilisearchExtension
from ormy.service.mongo import MongoBase, MongoConfig

# ----------------------- #

MwMb = TypeVar("MwMb", bound="MongoWithMeilisearchBackground")

# ....................... #


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
        res = super().create(data)

        # Run in background
        with ThreadPoolExecutor() as executor:
            executor.submit(cls.meili_update_documents, res)

        return res

    # ....................... #

    @classmethod
    async def acreate(cls: Type[MwMb], data: MwMb) -> MwMb:
        res = await super().acreate(data)

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
        super().create_many(data, ordered=ordered)

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
        await super().acreate_many(data, ordered=ordered)

        # Run in background
        asyncio.create_task(cls.ameili_update_documents(data))


# ----------------------- #

MwM = TypeVar("MwM", bound="MongoWithMeilisearch")

# ....................... #


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
        res = super().create(data)
        cls.meili_update_documents(res)

        return res

    # ....................... #

    @classmethod
    async def acreate(cls: Type[MwM], data: MwM) -> MwM:
        res = await super().acreate(data)
        await cls.ameili_update_documents(res)

        return res

    # ....................... #

    @classmethod
    def create_many(
        cls: Type[MwM],
        data: List[MwM],
        ordered: bool = False,
    ):
        super().create_many(data, ordered=ordered)
        cls.meili_update_documents(data)

    # ....................... #

    @classmethod
    async def acreate_many(
        cls: Type[MwM],
        data: List[MwM],
        ordered: bool = False,
    ):
        await super().acreate_many(data, ordered=ordered)
        await cls.ameili_update_documents(data)
