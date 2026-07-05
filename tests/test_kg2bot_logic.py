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


class RankingsNormalizationSafetyTests(unittest.TestCase):
    def test_normalize_rankings_rows_skips_bad_rows_and_keeps_valid(self):
        rows = [
            None,
            {"id": "not-a-number", "name": "Broken", "networth": "100000"},
            {"id": 200, "name": "Missing NW"},
            {"id": "117", "name": "Valid", "networth": "108,137", "rank": "1"},
        ]

        normalized = kg2bot._kg_normalize_rankings_rows(rows)

        self.assertEqual(1, len(normalized))
        self.assertEqual(117, normalized[0]["kingdom_id"])
        self.assertEqual("Valid", normalized[0]["kingdom_name"])
        self.assertEqual(108137, normalized[0]["networth"])
        self.assertEqual(1, normalized[0]["rank"])

    def test_normalize_rankings_rows_extracts_pie_state(self):
        rows = [
            {
                "id": 117,
                "name": "Example",
                "networth": 108137,
                "protection": {"slicesMissing": 2},
            }
        ]

        normalized = kg2bot._kg_normalize_rankings_rows(rows)

        self.assertEqual(1, len(normalized))
        self.assertTrue(normalized[0]["pie_active"])
        self.assertIn("2 slices missing", normalized[0]["pie_label"])


class NwJumpFormattingSafetyTests(unittest.TestCase):
    def test_build_nw_jump_sms_text_handles_partial_events(self):
        events = [
            {
                "kingdom_name": "Northhold",
                "delta": 5000,
                "old_networth": 100000,
                "new_networth": 105000,
            },
            {
                "kingdom_name": "Unknown",
            },
        ]

        sms = kg2bot._build_nw_jump_sms_text(events)

        self.assertIn("KG2 NW jump alert", sms)
        self.assertIn("Northhold", sms)
        self.assertIn("Unknown", sms)


class BridgeReportFormattingTests(unittest.TestCase):
    def test_format_bridge_report_text_reflows_messenger_blob(self):
        raw = (
            "Enter, Message sent 7:58 PM by General Zod: Target: Sevens house "
            "Alliance: Knights of the Fire Honour: 3.07 Ranking: 42 Networth: 16039 "
            "Spies Sent: 3000 Spies Lost: 203 Result Level: Complete Infiltration "
            "Number of Castles: 49 Our spies also found the following information about the kingdom's resources: "
            "Green Gems: 199 Blue Gems: 214 Stone: 11848 Land: 17191 / 17195 Horses: 975 "
            "Food: 294152 Wood: 16527 Gold: 1250591 Our spies also found the following information about the kingdom's troops: "
            "Approximate defensive power*: 16641*(without skill/prayer modifiers) "
            "The following technology information was also discovered: Loose Order Formation lvl 5 Crop Rotation lvl 6"
        )

        formatted = kg2bot.format_bridge_report_text(raw)

        self.assertNotIn("Enter, Message sent", formatted)
        self.assertIn("Target: Sevens house\nAlliance: Knights of the Fire", formatted)
        self.assertIn("Gold: 1250591\nOur spies also found", formatted)
        self.assertIn("Approximate defensive power*: 16641\n*(without skill/prayer modifiers)", formatted)
        self.assertIn("Loose Order Formation lvl 5\nCrop Rotation lvl 6", formatted)


if __name__ == "__main__":
    unittest.main()