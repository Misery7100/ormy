import unittest
from unittest.mock import MagicMock, patch  # noqa: F401

from ormwtf.service.mongo import MongoBase, MongoConfig, MongoCredentials

# ----------------------- #

credentials = MongoCredentials(
    host="localhost",
    port=27117,
    username="user",
    password="password",
    directConnection=True,
)

# ----------------------- #


class TestMongoBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        class Base1(MongoBase):
            config = MongoConfig(
                credentials=credentials,
                collection="base1_mongo",
                database="base1",
                streaming=False,
            )

        class Base2(MongoBase):
            config = MongoConfig(
                credentials=credentials,
                collection="base2_mongo",
                database="base2",
                streaming=False,
            )

        cls.base1 = Base1
        cls.base2 = Base2

    # ....................... #

    @classmethod
    def tearDownClass(cls):
        with cls.base1._client() as client:
            client.drop_database(cls.base1.config.database)

        with cls.base2._client() as client:
            client.drop_database(cls.base1.config.database)

    # ....................... #

    def test_subclass(self):
        self.assertTrue(
            issubclass(self.base1, MongoBase),
            "Base1 should be a subclass of MongoBase",
        )

        self.assertTrue(
            issubclass(self.base2, MongoBase),
            "Base2 should be a subclass of MongoBase",
        )

    # ....................... #

    def test_registry(self):
        reg1 = MongoBase._registry.get(self.base1.config.database).get(
            self.base1.config.collection
        )
        reg2 = MongoBase._registry.get(self.base2.config.database).get(
            self.base2.config.collection
        )

        self.assertTrue(
            reg1 is self.base1,
            "Registry item should be subclass Base1",
        )
        self.assertTrue(
            reg2 is self.base2,
            "Registry item should be subclass Base2",
        )

    # ....................... #

    def test_find_save(self):
        case1 = self.base1()
        case2 = self.base2(id=case1.id)

        self.assertNotEqual(case1, case2, "Instances should be different")

        self.assertIsNone(
            self.base1.find(case1.id, bypass=True),
            "Should return None",
        )

        case1.save()

        self.assertIsNotNone(
            self.base1.find(case1.id, bypass=True),
            "Should return an instance",
        )

        self.assertIsNone(
            self.base2.find(case2.id, bypass=True),
            "Should return None",
        )

        case2.save()

        self.assertIsNotNone(
            self.base2.find(case2.id, bypass=True),
            "Should return an instance",
        )


# ----------------------- #


class TestMongoBaseAsync(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        class Base1(MongoBase):
            config = MongoConfig(
                credentials=credentials,
                collection="base1_mongo_async",
                database="base1_async",
                streaming=False,
            )

        class Base2(MongoBase):
            config = MongoConfig(
                credentials=credentials,
                collection="base2_mongo_async",
                database="base2_async",
                streaming=False,
            )

        cls.base1 = Base1
        cls.base2 = Base2

    # ....................... #

    @classmethod
    def tearDownClass(cls):
        with cls.base1._client() as client:
            client.drop_database(cls.base1.config.database)

        with cls.base2._client() as client:
            client.drop_database(cls.base1.config.database)

    # ....................... #

    async def test_afind_asave(self):
        case1 = self.base1()
        case2 = self.base2(id=case1.id)

        self.assertNotEqual(case1, case2, "Instances should be different")

        self.assertIsNone(
            await self.base1.afind(case1.id, bypass=True),
            "Should return None",
        )

        await case1.asave()

        self.assertIsNotNone(
            await self.base1.afind(case1.id, bypass=True),
            "Should return an instance",
        )

        self.assertIsNone(
            await self.base2.afind(case2.id, bypass=True),
            "Should return None",
        )

        case2.save()

        self.assertIsNotNone(
            await self.base2.afind(case2.id, bypass=True),
            "Should return an instance",
        )


# ----------------------- #

if __name__ == "__main__":
    unittest.main()
