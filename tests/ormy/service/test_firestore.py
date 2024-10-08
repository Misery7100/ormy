import os
import unittest

import firebase_admin  # type: ignore

from ormy.service.firestore import (
    FirestoreBase,
    FirestoreConfig,
    FirestoreCredentials,
)

# ----------------------- #

os.environ["FIRESTORE_EMULATOR_HOST"] = "0.0.0.0:8096"

try:
    app = firebase_admin.get_app()

except ValueError:
    app = firebase_admin.initialize_app(options={"projectId": "test-project"})

credentials = FirestoreCredentials(
    app=app,
)

# ----------------------- #


class TestFirestoreBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        class Base1(FirestoreBase):
            configs = [
                FirestoreConfig(
                    credentials=credentials,
                    collection="base1_firestore",
                ),
            ]

            a: int = 1

        class Base2(Base1):
            configs = [
                FirestoreConfig(collection="base2_firestore"),
            ]

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

    def test_registry(self):
        reg = FirestoreBase._registry[FirestoreConfig]
        cfg1: FirestoreConfig = self.base1.get_config(type_=FirestoreConfig)
        cfg2: FirestoreConfig = self.base2.get_config(type_=FirestoreConfig)

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

    def test_transaction(self):
        case = self.base1()
        case.save()

        doc_before = self.base1.find(case.id)
        self.assertEqual(doc_before.a, 1, "Value should be 1")

        with self.base1.transaction() as tr:
            doc_before, snap = self.base1.find(
                case.id, transaction=tr, return_snapshot=True
            )
            updates = {"a": snap.get("a") + 1}
            doc_before.update_in_transaction(
                updates=updates,
                transaction=tr,
            )

        doc_after = self.base1.find(case.id)
        self.assertEqual(doc_after.a, 2, "Value should be 2")


# ----------------------- #


class TestFirestoreBaseAsync(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        class Base1(FirestoreBase):
            configs = [
                FirestoreConfig(
                    credentials=credentials,
                    collection="base1_firestore_async",
                ),
            ]

        class Base2(Base1):
            configs = [
                FirestoreConfig(collection="base2_firestore_async"),
            ]

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
