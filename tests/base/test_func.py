import unittest

from ormwtf.base.func import hex_uuid4

# ----------------------- #


class TestFunc(unittest.TestCase):
    def test_hex_uuid4(self):
        """Test hex_uuid4 function"""

        self.assertNotEqual(
            hex_uuid4(),
            hex_uuid4(),
            "UUIDs should be different",
        )
        self.assertEqual(
            hex_uuid4("test"),
            hex_uuid4("test"),
            "UUIDs should be the same for the same non-empty input",
        )
