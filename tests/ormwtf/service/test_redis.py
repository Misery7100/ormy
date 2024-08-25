import unittest

from ormwtf.service.redis import RedisBase, RedisConfig, RedisCredentials

# ----------------------- #


class TestRedisBase(unittest.TestCase):
    def setUp(self):
        credentials = RedisCredentials(
            host="localhost",
            port=6479,
        )

        class TestBase(RedisBase):
            config = RedisConfig(
                credentials=credentials,
                collection="test_base",
            )

        class TestBase2(RedisBase):
            config = RedisConfig(
                credentials=credentials,
                collection="test_base",
                database=2,
            )

        self.test_base = TestBase
        self.test_base2 = TestBase2

    # ....................... #

    def tearDown(self):
        with self.test_base._client() as client:
            client.flushdb()

        with self.test_base2._client() as client:
            client.flushdb()

        del self.test_base
        del self.test_base2

    # ....................... #

    def test_subclass(self):
        self.assertTrue(
            issubclass(self.test_base, RedisBase),
            "TestBase should be a subclass of RedisBase",
        )

        self.assertTrue(
            issubclass(self.test_base2, RedisBase),
            "TestBase2 should be a subclass of RedisBase",
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
