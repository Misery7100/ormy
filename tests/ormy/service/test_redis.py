import unittest

from ormy.service.redis import RedisBase, RedisConfig, RedisCredentials

# ----------------------- #

credentials = RedisCredentials(
    host="localhost",
    port=6479,
)

# ----------------------- #


class TestRedisBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        class Base1(RedisBase):
            configs = [
                RedisConfig(
                    credentials=credentials,
                    collection="base1_redis",
                ),
            ]

        class Base2(Base1):
            configs = [
                RedisConfig(
                    collection="base2_redis",
                    database=2,
                ),
            ]

        cls.base1 = Base1
        cls.base2 = Base2

    # ....................... #

    @classmethod
    def tearDownClass(cls):
        with cls.base1._client() as client:
            client.flushdb()

        with cls.base2._client() as client:
            client.flushdb()

    # ....................... #

    def test_subclass(self):
        self.assertTrue(
            issubclass(self.base1, RedisBase),
            "Base1 should be a subclass of RedisBase",
        )

        self.assertTrue(
            issubclass(self.base2, RedisBase),
            "Base2 should be a subclass of RedisBase",
        )

    # ....................... #

    def test_registry(self):
        reg = RedisBase._registry[RedisConfig]
        cfg1: RedisConfig = self.base1.get_config(type_=RedisConfig)
        cfg2: RedisConfig = self.base2.get_config(type_=RedisConfig)

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

    # ....................... #

    def test_pipe(self):
        case1 = self.base1()

        with case1.pipe() as pipe:
            case1.watch(pipe)
            case1.save(pipe)


# ----------------------- #


class TestRedisBaseAsync(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        class Base1(RedisBase):
            configs = [
                RedisConfig(
                    credentials=credentials,
                    collection="base1_redis_async",
                ),
            ]

        class Base2(Base1):
            configs = [
                RedisConfig(
                    collection="base2_redis_async",
                    database=2,
                ),
            ]

        cls.base1 = Base1
        cls.base2 = Base2

    # ....................... #

    @classmethod
    def tearDownClass(cls):
        with cls.base1._client() as client:
            client.flushdb()

        with cls.base2._client() as client:
            client.flushdb()

    # ....................... #

    async def test_afind_asave(self):
        case1 = self.base1()
        case2 = self.base2(id=case1.id)

        self.assertNotEqual(case1, case2, "Instances should be different")

        self.assertIsNone(
            await self.base1.afind(case1.id, bypass=True),
            "Should return None",
        )

        case1.save()

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

    # ....................... #

    async def test_apipe(self):
        case1 = self.base1()

        async with case1.apipe() as pipe:
            await case1.awatch(pipe)
            await case1.asave(pipe)


# ----------------------- #

if __name__ == "__main__":
    unittest.main()
