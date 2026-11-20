import unittest

from common.messages import MsgType, make_envelope, parse_envelope


class MessageCompatibilityTests(unittest.TestCase):
    def test_message_type_is_string_compatible(self) -> None:
        self.assertIsInstance(MsgType.QUERY, str)
        self.assertEqual(MsgType.QUERY, "QUERY")
        self.assertEqual(str(MsgType.QUERY), "QUERY")

    def test_envelope_round_trip_preserves_message_type(self) -> None:
        original = make_envelope(MsgType.QUERY, payload={"month": "2026-11"})

        parsed = parse_envelope(original.to_bytes())

        self.assertEqual(parsed.msg_type, "QUERY")
        self.assertEqual(parsed.payload, {"month": "2026-11"})


if __name__ == "__main__":
    unittest.main()
