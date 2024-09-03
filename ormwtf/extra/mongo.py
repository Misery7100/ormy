from typing import List, Type, TypeVar

from ormwtf.extension.meilisearch import MeilisearchConfig, MeilisearchExtension
from ormwtf.service.mongo import MongoBase, MongoConfig

# ----------------------- #

M = TypeVar("M", bound="MongoWithMeilisearch")

# ....................... #


class MongoWithMeilisearch(MongoBase, MeilisearchExtension):
    configs = [MongoConfig(), MeilisearchConfig()]

    # ....................... #

    def save(self: M) -> M:
        super().save()
        self.meili_update_documents(self)

        return self

    # ....................... #

    async def asave(self: M) -> M:
        await super().asave()
        await self.ameili_update_documents(self)

        return self

    # ....................... #

    @classmethod
    def create(cls: Type[M], data: M) -> M:
        res = super().create(data)
        cls.meili_update_documents(res)

        return res

    # ....................... #

    @classmethod
    async def acreate(cls: Type[M], data: M) -> M:
        res = await super().acreate(data)
        await cls.ameili_update_documents(data)

        return res

    # ....................... #

    @classmethod
    def create_many(cls: Type[M], data: List[M], ordered: bool = False):
        super().create_many(data, ordered=ordered)
        cls.meili_update_documents(data)

    # ....................... #

    @classmethod
    async def acreate_many(cls: Type[M], data: List[M], ordered: bool = False):
        await super().acreate_many(data, ordered=ordered)
        await cls.ameili_update_documents(data)
