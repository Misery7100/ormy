import unittest
from datetime import datetime, timezone

from ormy.base.func import (
    datetime_to_timestamp,
    hash_from_any,
    hex_uuid4,
    hex_uuid4_from_string,
    timestamp_to_datetime,
    utcnow,
)

# ----------------------- #


class TestFunc(unittest.TestCase):
    def test_hex_uuid4(self):
        """Test hex_uuid4 function"""

        # TODO: write doctest tests
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

    # ....................... #

    def test_utcnow(self):
        """Test utcnow function"""
        now = datetime.now(timezone.utc).timestamp()
        self.assertAlmostEqual(
            utcnow(),
            now,
            delta=1,
            msg="utcnow should return the current UTC timestamp",
        )

    # ....................... #

    def test_timestamp_to_datetime(self):
        """Test timestamp_to_datetime function"""
        timestamp = 1638316800  # Corresponds to 2021-12-01 00:00:00 UTC
        dt = timestamp_to_datetime(timestamp)
        self.assertEqual(
            dt,
            datetime(2021, 12, 1, 0, 0, 0, tzinfo=timezone.utc),
            "timestamp_to_datetime should convert timestamp to datetime",
        )

    # ....................... #

    def test_datetime_to_timestamp(self):
        """Test datetime_to_timestamp function"""
        dt = datetime(2021, 12, 1, 0, 0, 0, tzinfo=timezone.utc)
        timestamp = datetime_to_timestamp(dt)
        self.assertEqual(
            timestamp,
            1638316800,
            "datetime_to_timestamp should convert datetime to timestamp",
        )

    # ....................... #

    def test_hash_from_any(self):
        """Test hash_from_any function"""
        val_str = "test"
        val_dict = {"key": "value"}
        hash_str = hash_from_any(val_str)
        hash_dict = hash_from_any(val_dict)
        self.assertIsInstance(
            hash_str, str, "hash_from_any should return a string for str input"
        )
        self.assertIsInstance(
            hash_dict, str, "hash_from_any should return a string for dict input"
        )
        self.assertEqual(
            hash_from_any(val_str),
            hash_from_any(val_str),
            "hash_from_any should return the same hash for the same input",
        )
        self.assertNotEqual(
            hash_from_any(val_str),
            hash_from_any(val_dict),
            "hash_from_any should return different hashes for different inputs",
        )

    # ....................... #

    def test_hex_uuid4_from_string(self):
        """Test hex_uuid4_from_string function"""
        val = "test"
        uuid1 = hex_uuid4_from_string(val)
        uuid2 = hex_uuid4_from_string(val)
        self.assertEqual(
            uuid1,
            uuid2,
            "hex_uuid4_from_string should return the same UUID for the same input",
        )
        self.assertIsInstance(
            uuid1, str, "hex_uuid4_from_string should return a string"
        )


# ----------------------- #

if __name__ == "__main__":
    unittest.main()
