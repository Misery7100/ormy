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


class Base1(MongoBase):
    config = MongoConfig(
        credentials=credentials,
        collection="base1_mongo",
        streaming=False,
    )


class Base2(MongoBase):
    config = MongoConfig(
        credentials=credentials,
        collection="base2_mongo",
        database="second",
        streaming=False,
    )


class BaseInherit(Base1):
    config = MongoConfig(
        collection="base_inherit",
    )


# ----------------------- #


class TestMongoBase(unittest.TestCase):
    def setUp(self):
        self.test_base1 = Base1
        self.test_base2 = Base2

    # ....................... #

    def test_subclass(self):
        self.assertTrue(
            issubclass(self.test_base1, MongoBase),
            "Base1 should be a subclass of MongoBase",
        )

        self.assertTrue(
            issubclass(self.test_base2, MongoBase),
            "Base2 should be a subclass of MongoBase",
        )

    # ....................... #

    def test_registry(self):
        reg1 = MongoBase._registry.get(self.test_base1.config.database).get(
            self.test_base1.config.collection
        )
        reg2 = MongoBase._registry.get(self.test_base2.config.database).get(
            self.test_base2.config.collection
        )

        self.assertTrue(
            reg1 is self.test_base1,
            "Registry item should be subclass Base1",
        )
        self.assertTrue(
            reg2 is self.test_base2,
            "Registry item should be subclass Base2",
        )

    # ....................... #

    def test_find(self):
        case1 = self.test_base1()
        case2 = self.test_base2(id=case1.id)

        self.assertNotEqual(case1, case2, "Instances should be different")

        self.assertIsNone(
            self.test_base1.find(case1.id, bypass=True),
            "Should return None",
        )

        case1.save()

        self.assertIsNotNone(
            self.test_base1.find(case1.id, bypass=True),
            "Should return an instance",
        )

        self.assertIsNone(
            self.test_base2.find(case2.id, bypass=True),
            "Should return None",
        )

        case2.save()

        self.assertIsNotNone(
            self.test_base2.find(case2.id, bypass=True),
            "Should return an instance",
        )


# ----------------------- #


# class TestMongoBaseAsync(unittest.IsolatedAsyncioTestCase):
#     pass


# ----------------------- #

if __name__ == "__main__":
    unittest.main()
