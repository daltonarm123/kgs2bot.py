import os
import unittest


os.environ.setdefault("DISCORD_TOKEN", "test-token")
os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost:5432/kg2bot_test")
os.environ.setdefault("RECON_INGEST_ENABLED", "false")

import kg2bot  # noqa: E402


class RankingsPieParsingTests(unittest.TestCase):
    def test_rankings_debug_reports_raw_keys_and_interesting_fields(self):
        rows = [
            {
                "networth": 108137,
                "allianceName": "THE A-TEAM",
                "kingdomShielded": False,
                "attackProtection": 2,
                "nested": {"pieSlicesMissing": 3},
            }
        ]

        debug = kg2bot._kg_build_rankings_raw_debug(rows)

        self.assertIn("allianceName", debug["raw_keys"])
        self.assertIn("attackProtection=2", debug["interesting_fields"])
        self.assertIn("nested.pieSlicesMissing=3", debug["interesting_fields"])

    def test_rankings_row_without_pie_fields_does_not_detect_pie(self):
        row = {
            "networth": 108137,
            "continentId": 1,
            "allianceName": "THE A-TEAM",
            "kingdomShielded": False,
            "attackEffectiveness": 0,
            "canAttack": True,
            "rank": 1,
            "id": 117,
            "name": "Example",
        }

        state = kg2bot._kg_extract_rankings_pie_state(row)

        self.assertFalse(state["active"])
        self.assertEqual("", state["signature"])

    def test_rankings_pie_parser_detects_nested_slice_count(self):
        row = {
            "id": 117,
            "name": "Example",
            "networth": 108137,
            "protection": {"slicesMissing": 2},
        }

        state = kg2bot._kg_extract_rankings_pie_state(row)

        self.assertTrue(state["active"])
        self.assertIn("2 slices missing", state["label"])


class NwJumpIgnoreTests(unittest.TestCase):
    def test_channel_ignore_matches_normalized_kingdom_name(self):
        ignores = [{"kingdom_key": "elixer 111 blues we back"}]

        self.assertTrue(
            kg2bot._nw_jump_event_ignored_in_channel(
                {"kingdom_name": "Elixer 111 blues we back"}, ignores
            )
        )
        self.assertFalse(
            kg2bot._nw_jump_event_ignored_in_channel(
                {"kingdom_name": "Other Kingdom"}, ignores
            )
        )


if __name__ == "__main__":
    unittest.main()