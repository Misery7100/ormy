import unittest

from ormwtf.service.mongo import MongoBase, MongoConfig, MongoCredentials

# ----------------------- #


class TestMongoBase(unittest.TestCase):
    def setUp(self):
        credentials = MongoCredentials(
            host="localhost",
            port=27117,
            username="user",
            password="password",
            directConnection=True,
        )

        class TestBase(MongoBase):
            config = MongoConfig(
                credentials=credentials,
                collection="test_base",
                streaming=False,
            )

        class TestBase2(MongoBase):
            config = MongoConfig(
                credentials=credentials,
                collection="test_base",
                database="second",
                streaming=False,
            )

        self.test_base = TestBase
        self.test_base2 = TestBase2

    # ....................... #

    def tearDown(self):
        with self.test_base._client() as client:
            client.drop_database(self.test_base.config.database)
            client.drop_database(self.test_base2.config.database)

        del self.test_base
        del self.test_base2

    # ....................... #

    def test_subclass(self):
        self.assertTrue(
            issubclass(self.test_base, MongoBase),
            "TestBase should be a subclass of MongoBase",
        )

        self.assertTrue(
            issubclass(self.test_base2, MongoBase),
            "TestBase2 should be a subclass of MongoBase",
        )

    # ....................... #

    def test_find(self):
        case1 = self.test_base()
        case2 = self.test_base2(id=case1.id)

        self.assertNotEqual(case1, case2, "Instances should be different")

        self.assertIsNone(
            self.test_base.find(case1.id, bypass=True),
            "Should return None",
        )

        case1.save()

        self.assertIsNotNone(
            self.test_base.find(case1.id, bypass=True),
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

    # ....................... #
