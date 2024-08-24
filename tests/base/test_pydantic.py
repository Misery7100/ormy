import unittest

from ormwtf.base.pydantic import Base

# ----------------------- #


class TestPydantic(unittest.TestCase):
    def setUp(self):
        self.base = Base()

    # ....................... #

    def tearDown(self):
        del self.base

    # ....................... #

    def test_specific_fields(self):
        self.assertTrue(
            hasattr(self.base, "specific_fields"),
            "Base model should have specific fields",
        )
        self.assertIsInstance(
            self.base.specific_fields,
            dict,
            "Specific fields should be a dictionary",
        )

    # ....................... #

    def test_model_simple_schema(self):
        self.assertFalse(
            self.base.model_simple_schema(),
            "Model simple schema should return False",
        )
        self.assertFalse(
            self.base.model_simple_schema(exclude=["a", "b", "c"]),
            "Model simple schema should return False",
        )
        self.assertFalse(
            self.base.model_simple_schema(include=["a", "b", "c"]),
            "Model simple schema should return False",
        )
