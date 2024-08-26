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

meili_creds = MeilisearchCredentials(master_key="master_key")

# ....................... #


class BaseMixed(MongoBase, MeilisearchExtension):
    config = MongoConfig(
        credentials=mongo_creds,
        database="default",
        collection="base_mixed",
        streaming=False,
    )
    meili_config = MeilisearchConfig(
        credentials=meili_creds,
        index="default_base_mixed",
    )

    a: int = 10
    b: str = "test"


# ----------------------- #


class TestMelisearchMongoMixed(unittest.TestCase):
    def setUp(self):
        self.test_base = BaseMixed

    # ....................... #

    @classmethod
    def tearDownClass(cls):
        with BaseMixed._client() as client:
            client.drop_database(BaseMixed.config.database)

        with BaseMixed._meili_client() as meili_client:
            meili_client.delete_index_if_exists(BaseMixed.meili_config.index)

    # ....................... #

    def test_subclass(self):
        self.assertTrue(
            issubclass(self.test_base, MongoBase),
            "BaseMixed should be a subclass of MongoBase",
        )

        self.assertTrue(
            issubclass(self.test_base, MeilisearchExtension),
            "BaseMixed should be a subclass of MeilisearchExtension",
        )

    # ....................... #

    def test_registry(self):
        reg_mongo = MongoBase._registry.get(self.test_base.config.database).get(
            self.test_base.config.collection
        )
        reg_meli = MeilisearchExtension._meili_registry.get(
            self.test_base.meili_config.index
        )
        self.assertTrue(
            reg_mongo is reg_meli,
            "Registry items should match",
        )
        self.assertTrue(
            reg_meli is self.test_base,
            "Registry item should be BaseMixed",
        )

    # ....................... #

    def test_search(self):
        test = self.test_base()
        self.test_base.meili_update_documents([test.model_dump()])
        res = self.test_base.meili_search(SearchRequest())

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
