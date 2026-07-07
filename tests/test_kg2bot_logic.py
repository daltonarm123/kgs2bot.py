import os
import unittest
from unittest.mock import patch


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

    def test_build_rankings_fallback_row_from_state_keeps_missing_kingdom_trackable(self):
        state_row = {
            "kingdom_id": 321,
            "kingdom_name": "Magic",
            "rank_pos": 99,
            "networth": 15000,
        }

        row = kg2bot._build_rankings_fallback_row_from_state(state_row, 8000)

        self.assertEqual(321, row["kingdom_id"])
        self.assertEqual("Magic", row["kingdom_name"])
        self.assertEqual(99, row["rank"])
        self.assertEqual(8000, row["networth"])
        self.assertEqual("search_fallback", row["rankings_source"])

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


class NwJumpAnchorLogicTests(unittest.TestCase):
    THRESHOLD = 5000

    def _run_polls(self, values):
        """
        Drive the pure cumulative detector across a sequence of networth polls,
        threading the persisted anchor through exactly like the DB path does.
        Returns the list of fired events as (base_nw, new_nw, delta) tuples.
        """
        anchor = None
        old_nw = None
        events = []
        for v in values:
            fired, delta, base_nw, new_anchor = kg2bot._compute_nw_jump_from_anchor(
                anchor, old_nw, v, self.THRESHOLD
            )
            if fired:
                events.append((base_nw, v, delta))
            anchor = new_anchor
            old_nw = v
        return events

    def test_sudden_drop_fires_once(self):
        events = self._run_polls([15000, 8000])
        self.assertEqual(1, len(events))
        base, new, delta = events[0]
        self.assertEqual(15000, base)
        self.assertEqual(8000, new)
        self.assertEqual(-7000, delta)

    def test_gradual_drop_under_threshold_still_fires_once(self):
        # Each single step is under 5000, but the cumulative slide is large.
        polls = [15000, 14000, 13000, 12000, 11000, 10000, 9500, 9000, 8500, 8000]
        events = self._run_polls(polls)
        self.assertEqual(1, len(events))
        base, new, delta = events[0]
        self.assertEqual(15000, base)
        self.assertEqual(10000, new)  # anchor re-seeds the first time the total crosses 5000
        self.assertEqual(-5000, delta)

    def test_small_oscillation_never_fires(self):
        polls = [10000, 10500, 9800, 10200, 9900, 10100, 9700]
        self.assertEqual([], self._run_polls(polls))

    def test_upward_jump_fires(self):
        events = self._run_polls([50000, 56000])
        self.assertEqual(1, len(events))
        base, new, delta = events[0]
        self.assertEqual(50000, base)
        self.assertEqual(56000, new)
        self.assertEqual(6000, delta)

    def test_no_repeat_alert_after_settling_at_new_level(self):
        # Drop fires once, then the kingdom sits near its new NW: no further alerts.
        polls = [15000, 8000, 8000, 7900, 8100, 8000]
        events = self._run_polls(polls)
        self.assertEqual(1, len(events))

    def test_missing_current_networth_preserves_anchor(self):
        polls = [15000, None, None, 9500]
        events = self._run_polls(polls)
        self.assertEqual(1, len(events))
        base, new, delta = events[0]
        self.assertEqual(15000, base)
        self.assertEqual(9500, new)
        self.assertEqual(-5500, delta)

    def test_first_sight_seeds_anchor_without_alert(self):
        fired, delta, base_nw, new_anchor = kg2bot._compute_nw_jump_from_anchor(
            None, None, 12345, self.THRESHOLD
        )
        self.assertFalse(fired)
        self.assertEqual(12345, new_anchor)


class _FakeRankingsCursor:
    """Minimal RealDictCursor stand-in backed by an in-memory kingdom_rankings_state."""

    def __init__(self, store):
        self.store = store
        self._pending = []

    def _norm(self, sql):
        return " ".join(str(sql or "").split())

    def execute(self, sql, params=None):
        s = self._norm(sql)
        if "FROM kingdom_rankings_state" in s and "ANY" in s:
            ids = {int(x) for x in (params[1] or [])}
            self._pending = [dict(self.store[k]) for k in ids if k in self.store]
        else:
            self._pending = []

    def executemany(self, sql, seq):
        s = self._norm(sql)
        if "INSERT INTO kingdom_rankings_state" in s:
            for t in seq:
                kid = int(t[1])
                self.store[kid] = {
                    "kingdom_id": kid,
                    "kingdom_name": t[2],
                    "rank_pos": t[3],
                    "networth": t[4],
                    "pie_active": t[5],
                    "pie_signature": t[6],
                    "pie_label": t[7],
                    "alert_anchor_networth": t[9],
                }
        # kingdom_rankings_history inserts are irrelevant to detection state.

    def fetchall(self):
        return list(self._pending)

    def fetchone(self):
        return self._pending[0] if self._pending else None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeRankingsConn:
    def __init__(self, store):
        self.store = store

    def cursor(self, *args, **kwargs):
        return _FakeRankingsCursor(self.store)

    def commit(self):
        pass

    def rollback(self):
        pass


class NwJumpDetectionIntegrationTests(unittest.TestCase):
    THRESHOLD = 5000
    WORLD_ID = 1

    def _make_db(self, store):
        from contextlib import contextmanager

        @contextmanager
        def _fake_db_conn():
            yield _FakeRankingsConn(store)

        return _fake_db_conn

    def _row(self, networth, name="Magic", kid=321, rank=42):
        return {
            "kingdom_id": kid,
            "kingdom_name": name,
            "rank": rank,
            "networth": networth,
            "pie_active": False,
            "pie_signature": "",
            "pie_label": "",
        }

    def _drive(self, networth_sequence):
        store = {}
        all_events = []
        with patch.object(kg2bot, "db_conn", self._make_db(store)):
            for nw in networth_sequence:
                result = kg2bot.sync_detect_rankings_alerts(
                    self.WORLD_ID, [self._row(nw)], self.THRESHOLD
                )
                all_events.extend(result.get("nw_events") or [])
        return all_events, store

    def test_gradual_drop_fires_once_through_real_detector(self):
        polls = [15000, 14000, 13000, 12000, 11000, 10000, 9500, 9000, 8500, 8000]
        events, store = self._drive(polls)

        self.assertEqual(1, len(events), f"expected exactly one alert, got {events}")
        evt = events[0]
        self.assertEqual("Magic", evt["kingdom_name"])
        self.assertEqual(15000, evt["old_networth"])
        self.assertEqual(10000, evt["new_networth"])
        self.assertEqual(-5000, evt["delta"])
        # Final persisted state should reflect the latest poll + a re-seeded anchor.
        self.assertEqual(8000, store[321]["networth"])
        self.assertEqual(10000, store[321]["alert_anchor_networth"])

    def test_sudden_drop_fires_once_through_real_detector(self):
        events, _store = self._drive([15000, 8000])
        self.assertEqual(1, len(events))
        self.assertEqual(-7000, events[0]["delta"])

    def test_stable_kingdom_never_alerts(self):
        events, _store = self._drive([15000, 15010, 14990, 15000])
        self.assertEqual([], events)


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

    def test_unseen_report_batch_dedupes_fb_previews_and_attack_chat_tails(self):
        attack = (
            "pulled attack report from fb From: System Date: 22:02 To: The Way "
            "Subject: Attack Report: Gandolf Attack Report: Gandolf (NW: + 9642) "
            "Attack Result: Major Victory You have gained the following during the attack: "
            "474 Land, 71954 Food, 30950 Gold, 4316 Stone, 5245 Wood, 30 Horses "
            "We regret to inform you of the following casualties during the attack: 99/1085 Footmen "
            "During this battle your troops tried to breach the Medium Town Armadyl Camp "
            "(level 8 settlement) but were unable to take the town from the defending forces."
        )
        variants = [
            attack + " Yes my old settlement liberated",
            attack + " He's lost a few settys tonight",
            attack + " 805 after a decent central coast beer Mute Search Chat info",
        ]

        unseen = fb_messenger_bridge._unseen_report_batch(variants, set())

        self.assertEqual(1, len(unseen))
        self.assertTrue(unseen[0][0].endswith("defending forces."))
        self.assertNotIn("liberated", unseen[0][0])

        spy_variants = [
            "pulled spy report from fb\nTarget: Brickshithouse Alliance: The Unforgiven "
            "Spies Sent: 1200 Spies Lost: 80 Number of Castles: 12 "
            "Our spies also found the following information about the kingdom's troops: Peasants: 1000\n"
            "fb-spy-report.txt 3 KB",
            "Target: Brickshithouse Alliance: The Unforgiven Spies Sent: 1200 Spies Lost: 80 "
            "Number of Castles: 12 Our spies also found the following information about the kingdom's troops: Peasants: 1000",
        ]

        spy_unseen = fb_messenger_bridge._unseen_report_batch(spy_variants, set())

        self.assertEqual(1, len(spy_unseen))

    def test_doubled_report_snapshot_collapses_across_polls(self):
        body = (
            "From: System Date: 22:02 To: The Way Subject: Attack Report: Gandolf "
            "Attack Report: Gandolf (NW: + 9642) Attack Result: Major Victory "
            "You have gained the following during the attack: 474 Land, 71954 Food, 30950 Gold, 4316 Stone, 5245 Wood, 30 Horses "
            "We regret to inform you of the following casualties during the attack: 99/1085 Footmen "
            "During this battle your troops tried to breach the Medium Town Armadyl Camp "
            "(level 8 settlement) but were unable to take the town from the defending forces."
        )
        doubled = f"pulled attack report from fb {body} {body}"
        poll1 = doubled + (
            " Yes my old settlement liberated Yes my old settlement liberated"
            " He's lost a few settys tonight He's lost a few settys tonight"
            " 805 after a decent central coast beer 805 after a decent central coast beer"
            " Zod what is beasties dp? Zod what is beasties dp?"
            " August 3 cubs dodgers August 3 cubs dodgers"
            " Mute Search Chat info Customize chat Chat members Media, files and links Privacy & support"
        )
        poll2 = poll1 + " 8 hour drive from the farm 8 hour drive from the farm"

        parts1 = fb_messenger_bridge._split_report_blob(poll1)
        parts2 = fb_messenger_bridge._split_report_blob(poll2)
        hashes1 = {
            fb_messenger_bridge._sha(
                fb_messenger_bridge._canonical_report_text(p)
                or fb_messenger_bridge._format_report_text(p)
            )
            for p in parts1
            if fb_messenger_bridge._is_report_text(p)
        }
        hashes2 = {
            fb_messenger_bridge._sha(
                fb_messenger_bridge._canonical_report_text(p)
                or fb_messenger_bridge._format_report_text(p)
            )
            for p in parts2
            if fb_messenger_bridge._is_report_text(p)
        }

        self.assertEqual(1, len(hashes1))
        self.assertEqual(hashes1, hashes2)

    def test_spy_report_hash_stable_when_chat_replies_arrive_after(self):
        report = (
            "pulled spy report from fb From: System Date: 23:35 To: The Way "
            "Subject: Reverse Mowhawk Spy Report Target: Reverse Mowhawk "
            "Alliance: Break Honour: 14.96 Ranking: 64 Networth: 12040 "
            "Spies Sent: 2800 Spies Lost: 208 Result Level: Complete Infiltration "
            "Number of Castles: 15 "
            "Our spies also found the following information about the kingdom's resources: "
            "Gold: 292746 Wood: 56300 Food: 909009 Blue Gems: 1 Horses: 1713 Green Gems: 80 "
            "Stone: 52900 Land: 16800 / 17870 "
            "Our spies also found the following information about the kingdom's troops: "
            "Population: 48330 / 48330 Heavy Cavalry: 1086 Pikemen: 195 Crossbowmen: 6265 "
            "Archers: 35 Peasants: 37932 Approximate defensive power*: 18490*(without skill/prayer modifiers) "
            "The following information was found regarding troop movements around this kingdom: "
            "Launched an attack on The Way (20:08) Launched an attack on Rodan (19:41) "
            "Launched an attack on Eowhyr (17:38) Attacked by The kingdom of war (16:35) "
            "Launched an attack on The kingdom of war (13:14) Attacked by Eowhyr (09:57) "
            "The following recent market transactions were also discovered: "
            "Bought 1 x Stone from Dude for 300000 gold (2026-07-04 18:11) "
            "Bought 1 x Stone from Mobius for 120000 gold (2026-07-03 19:10) "
            "Bought 1 x Stone from Mobius for 110000 gold (2026-07-02 15:43)"
        )
        poll1 = report + " Mute Search Chat info Customize chat Chat members Media, files and links Privacy & support"
        poll2 = (
            report
            + " @everyone good land @everyone good land Zoo Real good Real good"
            + " Mute Search Chat info Customize chat Chat members Media, files and links Privacy & support"
        )

        def _hashes(blob: str) -> set[str]:
            parts = fb_messenger_bridge._split_report_blob(blob)
            return {
                fb_messenger_bridge._sha(
                    fb_messenger_bridge._canonical_report_text(p)
                    or fb_messenger_bridge._format_report_text(p)
                )
                for p in parts
                if fb_messenger_bridge._is_report_text(p)
            }

        h1 = _hashes(poll1)
        h2 = _hashes(poll2)

        self.assertEqual(1, len(h1))
        self.assertEqual(h1, h2)

        # Posted body should never leak @everyone or messenger chrome.
        formatted = fb_messenger_bridge._format_report_text(poll2)
        self.assertNotIn("@everyone", formatted)
        self.assertNotIn("Zoo Real good", formatted)
        self.assertNotIn("Mute Search", formatted)
        self.assertTrue(
            formatted.rstrip().endswith(
                "Bought 1 x Stone from Mobius for 110000 gold (2026-07-02 15:43)"
            ),
            formatted,
        )

        # kg2bot.format_bridge_report_text must produce the same stable hash.
        kg_norm1 = kg2bot.format_bridge_report_text(poll1)
        kg_norm2 = kg2bot.format_bridge_report_text(poll2)
        self.assertEqual(
            kg2bot.normalized_report_hash(kg_norm1),
            kg2bot.normalized_report_hash(kg_norm2),
        )
        self.assertNotIn("@everyone", kg_norm2)
        self.assertNotIn("Mute Search", kg_norm2)

    def test_unseen_report_batch_splits_doubled_target_blob(self):
        doubled_blob = (
            "Target: Josh Alliance: Knights of the Fire Honour: 23.84 Ranking: 41 Networth: 16006 "
            "Spies Sent: 111 Spies Lost: 0 Result Level: Complete Infiltration Number of Castles: 4 "
            "Our spies also found the following information about the kingdom's resources: Gold: 541274 Food: 1400106 "
            "Our spies also found the following information about the kingdom's troops: Footmen: 13152 "
            "Approximate defensive power*: 44775*(without skill/prayer modifiers) "
            "Target: Josh Alliance: Knights of the Fire Honour: 23.84 Ranking: 41 Networth: 16006 "
            "Spies Sent: 111 Spies Lost: 0 Result Level: Complete Infiltration Number of Castles: 4 "
            "Our spies also found the following information about the kingdom's resources: Gold: 541274 Food: 1400106 "
            "Our spies also found the following information about the kingdom's troops: Footmen: 13152 "
            "Approximate defensive power*: 44775*(without skill/prayer modifiers)"
        )

        unseen = fb_messenger_bridge._unseen_report_batch([doubled_blob], set())

        self.assertEqual(1, len(unseen))
        self.assertEqual(1, unseen[0][0].count("Target: Josh"))


class BridgeIngestDedupeTests(unittest.TestCase):
    SPY_REPORT = (
        "Target: Dude\n"
        "Alliance: NWO-1\n"
        "Spies Sent: 1600\n"
        "Spies Lost: 102\n"
        "Number of Castles: 50\n"
        "Our spies also found the following information about the kingdom's troops:\n"
        "Peasants: 59445\n"
        "Approximate defensive power*: 84141\n"
        "*(without skill/prayer modifiers)\n"
        "The following technology information was also discovered: Leadership Training lvl 6"
    )

    def _run_ingest(self, spy_result):
        schedule_calls = []

        def fake_schedule(kind, text):
            schedule_calls.append((kind, text))
            return {"scheduled": True, "channel": "test"}

        with patch.object(kg2bot, "ensure_db_ready_sync", lambda: None), \
                patch.object(kg2bot, "sync_store_report", lambda text, ts: spy_result), \
                patch.object(kg2bot, "sync_store_attack_report", lambda *a, **kw: {"saved": False}), \
                patch.object(kg2bot, "sync_recon_ingest_report", lambda text: {"ok": True}), \
                patch.object(kg2bot, "schedule_bridge_report_to_discord", fake_schedule):
            result = kg2bot.sync_ingest_bridge_report(
                "facebook-messenger",
                "A Team Only:abc123",
                self.SPY_REPORT,
                None,
            )
        return result, schedule_calls

    def test_new_spy_report_triggers_single_discord_post(self):
        row = {"id": 1, "kingdom": "Dude", "defense_power": 84141, "castles": 50}
        result, schedule_calls = self._run_ingest({"saved": True, "duplicate": False, "row": row})

        self.assertEqual(1, len(schedule_calls))
        self.assertEqual("spy", schedule_calls[0][0])
        self.assertTrue(result["discord_post"].get("scheduled"))

    def test_duplicate_spy_report_does_not_post_to_discord_again(self):
        result, schedule_calls = self._run_ingest({"saved": True, "duplicate": True, "row": None})

        self.assertEqual(0, len(schedule_calls))
        self.assertFalse(result["discord_post"].get("scheduled"))
        self.assertTrue(result["discord_post"].get("duplicate"))


if __name__ == "__main__":
    unittest.main()