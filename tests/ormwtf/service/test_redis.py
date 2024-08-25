import unittest

from ormwtf.service.redis import RedisBase, RedisConfig, RedisCredentials

# ----------------------- #

credentials = RedisCredentials(
    host="localhost",
    port=6479,
)


class Base1(RedisBase):
    config = RedisConfig(
        credentials=credentials,
        collection="base1_redis",
    )


class Base2(RedisBase):
    config = RedisConfig(
        credentials=credentials,
        collection="base2_redis",
        database=2,
    )


# ----------------------- #


class TestRedisBase(unittest.TestCase):
    def setUp(self):

        self.test_base1 = Base1
        self.test_base2 = Base2

    # ....................... #

    def tearDown(self):
        with self.test_base1._client() as client:
            client.flushdb()

        with self.test_base2._client() as client:
            client.flushdb()

        del self.test_base1
        del self.test_base2

    # ....................... #

    def test_subclass(self):
        self.assertTrue(
            issubclass(self.test_base1, RedisBase),
            "Base1 should be a subclass of RedisBase",
        )

        self.assertTrue(
            issubclass(self.test_base2, RedisBase),
            "Base2 should be a subclass of RedisBase",
        )

    # ....................... #

    def test_registry(self):
        reg1 = RedisBase._registry.get(self.test_base1.config.database).get(
            self.test_base1.config.collection
        )
        reg2 = RedisBase._registry.get(self.test_base2.config.database).get(
            self.test_base2.config.collection
        )

        self.assertTrue(
            reg1 is self.test_base1,
            "Registry item should be Base1",
        )
        self.assertTrue(
            reg2 is self.test_base2,
            "Registry item should be Base2",
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

    # ....................... #
