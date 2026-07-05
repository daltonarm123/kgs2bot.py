import os
import unittest


os.environ.setdefault("DISCORD_TOKEN", "test-token")
os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost:5432/kg2bot_test")
os.environ.setdefault("RECON_INGEST_ENABLED", "false")

import kg2bot  # noqa: E402
import fb_messenger_bridge  # noqa: E402


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

    def test_format_bridge_report_text_strips_messenger_tail_after_tech(self):
        raw = (
            "Received: Jul 5, 2026, 5:45:16 PM Subject: Sevens house Spy Report "
            "Target: Sevens house Alliance: Knights of the Fire Honour: 3.07 Ranking: 46 Networth: 14859 "
            "Spies Sent: 3000 Spies Lost: 357 Result Level: Complete Infiltration Number of Castles: 49 "
            "Our spies also found the following information about the kingdom's troops: "
            "Footmen: 3345 Crossbowmen: 565 Peasants: 24049 "
            "Approximate defensive power*: 40298*(without skill/prayer modifiers) "
            "The following technology information was also discovered: Improved Military Buildings lvl 6 "
            "Better Building Maintenance lvl 4 Leadership Training lvl 6 Military Encampment lvl 7 "
            "Ufffffff glad I asked 11:18 PM @mk experiment tonight Mute Search Chat info Customize chat"
        )

        formatted = kg2bot.format_bridge_report_text(raw)

        self.assertIn("Received: Jul 5, 2026, 5:45:16 PM\nSubject: Sevens house Spy Report", formatted)
        self.assertIn("Footmen: 3345\nCrossbowmen: 565", formatted)
        self.assertTrue(formatted.endswith("Military Encampment lvl 7"))
        self.assertNotIn("Ufffffff", formatted)
        self.assertNotIn("Mute Search Chat info", formatted)


class FacebookBridgeStateTests(unittest.TestCase):
    def test_legacy_chat_hash_migrates_to_uninitialized_seen_state(self):
        state = {"A Team Only": "old-hash"}

        chat_state = fb_messenger_bridge._chat_state(state, "A Team Only")

        self.assertFalse(chat_state["initialized"])
        self.assertEqual("old-hash", chat_state["last_report_hash"])
        self.assertEqual(["old-hash"], chat_state["seen_report_hashes"])
        self.assertIs(state["A Team Only"], chat_state)

    def test_remember_report_hashes_dedupes_and_marks_chat_initialized(self):
        chat_state = {
            "initialized": False,
            "last_report_hash": "old-hash",
            "seen_report_hashes": ["old-hash"],
        }

        fb_messenger_bridge._remember_report_hashes(chat_state, ["old-hash", "new-hash"])

        self.assertTrue(chat_state["initialized"])
        self.assertEqual("new-hash", chat_state["last_report_hash"])
        self.assertEqual(["old-hash", "new-hash"], chat_state["seen_report_hashes"])

    def test_unseen_report_batch_keeps_multiple_new_reports(self):
        reports = [
            fb_messenger_bridge._format_report_text(
                f"Target: Multi {idx} Spies Sent: 100 Spies Lost: {idx} "
                f"Number of Castles: {idx + 1} "
                "Our spies also found the following information about the kingdom's resources: "
                f"Gold: {1000 + idx}"
            )
            for idx in range(3)
        ]

        unseen = fb_messenger_bridge._unseen_report_batch(reports, set())

        self.assertEqual(3, len(unseen))
        self.assertEqual(["Target: Multi 0", "Target: Multi 1", "Target: Multi 2"], [text.splitlines()[0] for text, _ in unseen])

    def test_unseen_report_batch_skips_seen_and_duplicate_candidates(self):
        report = fb_messenger_bridge._format_report_text(
            "Target: Seen One Spies Sent: 100 Spies Lost: 1 Number of Castles: 2 "
            "Our spies also found the following information about the kingdom's resources: Gold: 1000"
        )
        report_hash = fb_messenger_bridge._sha(report)

        self.assertEqual([], fb_messenger_bridge._unseen_report_batch([report, report], {report_hash}))

        unseen = fb_messenger_bridge._unseen_report_batch([report, report], set())

        self.assertEqual(1, len(unseen))
        self.assertEqual(report_hash, unseen[0][1])

    def test_unseen_report_batch_dedupes_same_report_with_different_messenger_tails(self):
        base = (
            "Target: Sevens house Alliance: Knights of the Fire Honour: 3.07 Ranking: 46 Networth: 14859 "
            "Spies Sent: 3000 Spies Lost: 357 Result Level: Complete Infiltration Number of Castles: 49 "
            "Our spies also found the following information about the kingdom's troops: "
            "Footmen: 3345 Crossbowmen: 565 Peasants: 24049 "
            "The following technology information was also discovered: Improved Military Buildings lvl 6 "
            "Better Building Maintenance lvl 4 Leadership Training lvl 6 Military Encampment lvl 7"
        )
        variants = [
            base + " Ufffffff glad I asked Mute Search Chat info",
            base + " Ufffffff glad I asked shhh don't tell Ant Mute Search Chat info",
            base + " Mute Search Chat info Customize chat Chat members",
        ]

        unseen = fb_messenger_bridge._unseen_report_batch(variants, set())

        self.assertEqual(1, len(unseen))
        self.assertTrue(unseen[0][0].endswith("Military Encampment lvl 7"))
        self.assertNotIn("Ufffffff", unseen[0][0])


if __name__ == "__main__":
    unittest.main()