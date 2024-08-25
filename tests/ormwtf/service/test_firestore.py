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
    app = firebase_admin.initialize_app()

credentials = FirestoreCredentials(
    app=app,
)


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


# ----------------------- #


class TestFirestoreBase(unittest.TestCase):
    def setUp(self):
        self.test_base1 = Base1
        self.test_base2 = Base2

    # ....................... #

    def tearDown(self):
        with self.test_base1._client() as client:
            client.recursive_delete(self.test_base1._get_collection())
            client.recursive_delete(self.test_base2._get_collection())

        del self.test_base1
        del self.test_base2

    # ....................... #

    def test_subclass(self):
        self.assertTrue(
            issubclass(self.test_base1, FirestoreBase),
            "TestBase should be a subclass of FirestoreBase",
        )

        self.assertTrue(
            issubclass(self.test_base2, FirestoreBase),
            "TestBase2 should be a subclass of FirestoreBase",
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
