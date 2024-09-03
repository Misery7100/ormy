import unittest
from unittest.mock import MagicMock, patch  # noqa: F401

from ormwtf.service.mongo import MongoBase, MongoConfig, MongoCredentials

# ----------------------- #

credentials = MongoCredentials(
    host="localhost",
    port=27117,
    username="user",
    password="password",
    replicaset="rs0",
    directConnection=True,
)

# ----------------------- #


class TestMongoBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        class Base1(MongoBase):
            configs = [
                MongoConfig(
                    credentials=credentials,
                    collection="base1_mongo",
                    database="base1",
                    streaming=False,
                ),
            ]

        class Base2(Base1):
            configs = [
                MongoConfig(
                    collection="base2_mongo",
                    database="base2",
                ),
            ]

        cls.base1 = Base1
        cls.base2 = Base2

    # ....................... #

    @classmethod
    def tearDownClass(cls):
        with cls.base1._client() as client:
            cfg = cls.base1.get_config(type_=MongoConfig)
            client.drop_database(cfg.database)

        with cls.base2._client() as client:
            cfg = cls.base2.get_config(type_=MongoConfig)
            client.drop_database(cfg.database)

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
        reg = MongoBase._registry[MongoConfig]
        cfg1: MongoConfig = self.base1.get_config(type_=MongoConfig)
        cfg2: MongoConfig = self.base2.get_config(type_=MongoConfig)

        reg1 = reg[cfg1.database][cfg1.collection]
        reg2 = reg[cfg2.database][cfg2.collection]

        self.assertTrue(
            reg1 is self.base1,
            "Registry item should be Base1",
        )
        self.assertTrue(
            reg2 is self.base2,
            "Registry item should be Base2",
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
            configs = [
                MongoConfig(
                    credentials=credentials,
                    collection="base1_mongo_async",
                    database="base1_async",
                    streaming=False,
                ),
            ]

        class Base2(Base1):
            configs = [
                MongoConfig(
                    collection="base2_mongo_async",
                    database="base2_async",
                ),
            ]

        cls.base1 = Base1
        cls.base2 = Base2

    # ....................... #

    @classmethod
    def tearDownClass(cls):
        with cls.base1._client() as client:
            cfg = cls.base1.get_config(type_=MongoConfig)
            client.drop_database(cfg.database)

        with cls.base2._client() as client:
            cfg = cls.base2.get_config(type_=MongoConfig)
            client.drop_database(cfg.database)

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
