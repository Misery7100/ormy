import unittest

from ormwtf.extension.meilisearch import (
    MeilisearchConfig,
    MeilisearchCredentials,
    MeilisearchExtension,
)
from ormwtf.service.mongo import MongoBase, MongoConfig, MongoCredentials

# ----------------------- #


class TestMongoBase(unittest.TestCase):
    def setUp(self):
        mongo_creds = MongoCredentials(
            host="localhost",
            port=27117,
            username="user",
            password="password",
            directConnection=True,
        )
        meili_creds = MeilisearchCredentials(master_key="master_key")

        class TestBase(MongoBase, MeilisearchExtension):
            config = MongoConfig(
                credentials=mongo_creds,
                collection="test_base",
                streaming=False,
            )
            meili_config = MeilisearchConfig(
                credentials=meili_creds,
                index="test_base",
            )

            a: int = 10
            b: str = "test"

        self.test_base = TestBase

    # ....................... #

    def tearDown(self):
        with self.test_base._client() as client:
            client.drop_database(self.test_base.config.database)

        with self.test_base._meili_client() as meili_client:
            meili_client.delete_index_if_exists(self.test_base.meili_config.index)

        del self.test_base

    # ....................... #

    def test_subclass(self):
        self.assertTrue(
            issubclass(self.test_base, MongoBase),
            "TestBase should be a subclass of MongoBase",
        )

        self.assertTrue(
            issubclass(self.test_base, MeilisearchExtension),
            "TestBase should be a subclass of MeilisearchExtension",
        )

    # ....................... #
