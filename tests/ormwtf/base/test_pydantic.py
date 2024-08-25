import unittest

from pydantic import SecretStr

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

    # ....................... #

    def test_handle_secret(self):
        secret = SecretStr("secret_value")
        self.assertNotEqual(
            str(secret),
            "secret_value",
            "Handle secret should return masked value",
        )
        self.assertEqual(
            Base._handle_secret("not_secret"),
            "not_secret",
            "Handle secret should return the original value if not a SecretStr",
        )

    # ....................... #

    def test_model_dump_with_secrets(self):
        class TestModel(Base):
            secret_field: SecretStr
            normal_field: str

        model_instance = TestModel(
            secret_field=SecretStr("secret"), normal_field="normal"
        )
        dumped_with_secrets = model_instance.model_dump_with_secrets()
        dumped = model_instance.model_dump()

        self.assertNotEqual(
            dumped["secret_field"],
            "secret",
            "Dumped (normal mode) secret field should be masked",
        )
        self.assertEqual(
            dumped_with_secrets["secret_field"],
            "secret",
            "Dumped (normal mode) secret field should be masked",
        )
        self.assertEqual(
            dumped["normal_field"],
            "normal",
            "Dumped normal field should be the original value",
        )

    # ....................... #

    def test_define_dtype(self):
        self.assertEqual(
            Base._define_dtype("created_at"),
            "datetime",
            "Define dtype should return 'datetime' for 'created_at'",
        )
        self.assertEqual(
            Base._define_dtype("unknown_field", "string"),
            "string",
            "Define dtype should return the provided dtype if field is unknown",
        )
        self.assertEqual(
            Base._define_dtype("unknown_field"),
            "string",
            "Define dtype should return 'string' if dtype is not provided and field is unknown",
        )


# ----------------------- #

if __name__ == "__main__":
    unittest.main()
