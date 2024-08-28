import os
import unittest

import firebase_admin  # type: ignore

from ormwtf.service.firestore import (
    FirestoreBase,
    FirestoreConfig,
    FirestoreCredentials,
)

# ----------------------- #

os.environ["FIRESTORE_EMULATOR_HOST"] = "0.0.0.0:8096"

try:
    app = firebase_admin.get_app()

except ValueError:
    app = firebase_admin.initialize_app(options={"projectId": "test"})

credentials = FirestoreCredentials(
    app=app,
)

# ----------------------- #


class TestFirestoreBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        class Base1(FirestoreBase):
            config = FirestoreConfig(
                credentials=credentials,
                collection="base1_firestore",
            )

        class Base2(FirestoreBase):
            config = FirestoreConfig(
                credentials=credentials,
                collection="base2_firestore",
                database="second",
            )

        cls.base1 = Base1
        cls.base2 = Base2

    # ....................... #

    @classmethod
    def tearDownClass(cls):
        with cls.base1._client() as client:
            client.recursive_delete(cls.base1._get_collection())

        with cls.base2._client() as client:
            client.recursive_delete(cls.base2._get_collection())

    # ....................... #

    def test_subclass(self):
        self.assertTrue(
            issubclass(self.base1, FirestoreBase),
            "TestBase should be a subclass of FirestoreBase",
        )

        self.assertTrue(
            issubclass(self.base2, FirestoreBase),
            "TestBase2 should be a subclass of FirestoreBase",
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


class TestFirestoreBaseAsync(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        class Base1(FirestoreBase):
            config = FirestoreConfig(
                credentials=credentials,
                collection="base1_firestore_async",
            )

        class Base2(FirestoreBase):
            config = FirestoreConfig(
                credentials=credentials,
                collection="base2_firestore_async",
                database="second",
            )

        cls.base1 = Base1
        cls.base2 = Base2

    # ....................... #

    @classmethod
    def tearDownClass(cls):
        with cls.base1._client() as client:
            client.recursive_delete(cls.base1._get_collection())

        with cls.base2._client() as client:
            client.recursive_delete(cls.base2._get_collection())

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


# ----------------------- #

if __name__ == "__main__":
    unittest.main()
