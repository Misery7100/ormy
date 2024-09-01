import unittest

from ormwtf.extension.meilisearch import (
    MeilisearchConfig,
    MeilisearchCredentials,
    MeilisearchExtension,
    SearchRequest,
    SearchResponse,
)
from ormwtf.service.mongo import MongoBase, MongoConfig, MongoCredentials

# ----------------------- #

mongo_creds = MongoCredentials(
    host="localhost",
    port=27117,
    username="user",
    password="password",
    directConnection=True,
)

# ....................... #

meili_creds = MeilisearchCredentials(
    master_key="master_key",
    port="7711",
)

# ----------------------- #


class TestMelisearchMongoMixed(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        class BaseMixed(MongoBase, MeilisearchExtension):
            configs = [
                MongoConfig(
                    credentials=mongo_creds,
                    database="base_mixed",
                    collection="base_mixed",
                    streaming=False,
                ),
                MeilisearchConfig(
                    credentials=meili_creds,
                    index="default_base_mixed",
                ),
            ]

            a: int = 10
            b: str = "test"

        cls.base = BaseMixed

    # ....................... #

    @classmethod
    def tearDownClass(cls):
        with cls.base._client() as client:
            cfg = cls.base.get_config(type_=MongoConfig)
            client.drop_database(cfg.database)

        with cls.base._meili_client() as meili_client:
            cfg = cls.base.get_config(type_=MeilisearchConfig)
            meili_client.delete_index_if_exists(cfg.index)

    # ....................... #

    def test_subclass(self):
        self.assertTrue(
            issubclass(self.base, MongoBase),
            "BaseMixed should be a subclass of MongoBase",
        )

        self.assertTrue(
            issubclass(self.base, MeilisearchExtension),
            "BaseMixed should be a subclass of MeilisearchExtension",
        )

    # ....................... #

    def test_registry(self):
        reg_mongo = MongoBase._registry[MongoConfig]
        reg_meili = MeilisearchExtension._registry[MeilisearchConfig]

        cfg_mongo: MongoConfig = self.base.get_config(type_=MongoConfig)
        cfg_meili: MeilisearchConfig = self.base.get_config(type_=MeilisearchConfig)

        reg1 = reg_mongo[cfg_mongo.database][cfg_mongo.collection]
        reg2 = reg_meili[cfg_meili.index]

        self.assertTrue(
            reg1 is reg2,
            "Registry items should match",
        )
        self.assertTrue(
            reg1 is self.base,
            "Registry item should be BaseMixed",
        )

    # ....................... #

    def test_search(self):
        test = self.base()
        self.base.meili_update_documents(test)
        res = self.base.meili_search(SearchRequest())

        self.assertIsNotNone(
            res,
            "Search result should not be None",
        )
        self.assertIsInstance(
            res,
            SearchResponse,
            "Search result should be SearchResponse",
        )


# ----------------------- #


class TestMelisearchMongoMixedAsync(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        class BaseMixed(MongoBase, MeilisearchExtension):
            configs = [
                MongoConfig(
                    credentials=mongo_creds,
                    database="base_mixed_async",
                    collection="base_mixed_async",
                    streaming=False,
                ),
                MeilisearchConfig(
                    credentials=meili_creds,
                    index="default_base_mixed_async",
                ),
            ]

            a: int = 10
            b: str = "test"

        cls.base = BaseMixed

    # ....................... #

    @classmethod
    def tearDownClass(cls):
        with cls.base._client() as client:
            cfg = cls.base.get_config(type_=MongoConfig)
            client.drop_database(cfg.database)

        with cls.base._meili_client() as meili_client:
            cfg = cls.base.get_config(type_=MeilisearchConfig)
            meili_client.delete_index_if_exists(cfg.index)

    # ....................... #

    async def test_asearch(self):
        test = self.base()
        await self.base.ameili_update_documents(test)
        res = await self.base.ameili_search(SearchRequest())

        self.assertIsNotNone(
            res,
            "Search result should not be None",
        )
        self.assertIsInstance(
            res,
            SearchResponse,
            "Search result should be SearchResponse",
        )


# ----------------------- #

if __name__ == "__main__":
    unittest.main()
