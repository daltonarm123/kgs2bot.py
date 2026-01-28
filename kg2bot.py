# ---------- KG2 Recon Bot • FULL BUILD (All commands requested) ----------
# Postgres (psycopg2) + Connection Pool + Async-safe (DB work offloaded to threads)
#
# Auto-capture on paste:
# - Saves spy reports (raw OR gz) + dedupe by report_hash
# - Parses DP/castles + tech section + SR troop snapshots
# - Auto-indexes tech into tech_index + updates per-kingdom best tech (kingdom_tech)
# - Auto-saves SR troop snapshots + provides !troops / !troopsdelta
# - Ensures AP session for kingdoms with DP
#
# Commands:
# !calc              -> prompts paste (90s) -> Base DP, Adjusted, HC needed + tiers
# !calc <kingdom>    -> uses latest DP report for kingdom (DB)
# !calc db           -> uses latest DP report overall (DB)
#
# !spy <kingdom>        -> latest saved spy report embed (kingdom)
# !spyid <id>           -> saved spy report by DB id
# !spyhistory <kingdom> -> last 5 report IDs + timestamps + DP/castles
#
# !ap <kingdom>       -> AP Planner with buttons (Minor/Victory/Major/Overwhelming + Reset + Rebuild)
# !apstatus <kingdom> -> read-only AP status (no buttons)
#
# !techindex              -> scans ALL saved spy reports and fills tech_index + kingdom_tech
# !tech <kingdom>         -> shows deduped battle-related tech for that kingdom (from player_tech view)
# !techtop                -> shows 15 most common indexed trainings across all kingdoms (from tech_index)
# !techpull <kingdom>     -> rescans ALL reports for that kingdom and rebuilds deduped best tech list
# !backfill [days]        -> rescans DB reports (all or last N days) to ensure tech + troops are accounted for
#
# !troops <kingdom>       -> latest SR troop snapshot (top 25 units)
# !troopsdelta <kingdom>  -> delta between last 2 SR snapshots (losses/gains)
#
# !refresh                -> admin-only restart (Render)
#
# Announcement:
# - Only announces THIS version PATCH_NOTES
# - DB-backed dedupe by version + cooldown

import os
import re
import io
import csv
import time
import gzip
import sys
import asyncio
import difflib
import hashlib
import logging
import traceback
from math import ceil
from datetime import datetime, timezone, timedelta
from contextlib import contextmanager

import discord
from discord.ext import commands
from discord.ui import View, Button
from dotenv import load_dotenv

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import pool as pg_pool


# ------------------- PATCH INFO -------------------
BOT_VERSION = "2026-01-28.2"
PATCH_NOTES = [
    "Added: Full command set restored (!spy, !spyid, !spyhistory, !techindex, !tech, !techtop, !techpull, !backfill).",
    "Fixed: !techindex / !techpull now rebuild tech_index + per-kingdom best-tech (player_tech view).",
    "Added: !apstatus (read-only AP planner).",
    "Added: !troops / !troopsdelta aliases + ensured SR snapshots accounted for on backfill.",
    "Improved: DB pool + thread offload everywhere (no event-loop blocking).",
]
# -------------------------------------------------


# ---------- Env ----------
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ERROR_CHANNEL_NAME = os.getenv("ERROR_CHANNEL_NAME", "kg2recon-updates")
KEEP_RAW_TEXT = os.getenv("KEEP_RAW_TEXT", "false").lower() in ("1", "true", "yes", "y")

if not TOKEN:
    raise RuntimeError("Missing DISCORD_TOKEN env var.")
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL env var.")

logging.basicConfig(level=logging.INFO)


# ---------- Game constants ----------
HEAVY_CAVALRY_AP = 7
AP_REDUCTIONS = [
    ("Minor Victory", 0.19),
    ("Victory", 0.35),
    ("Major Victory", 0.55),
    ("Overwhelming Victory", 0.875),
]


# ---------- Discord ----------
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)


# ---------- Locks ----------
ap_lock = asyncio.Lock()


# ---------- Announcement anti-spam ----------
ANNOUNCED_READY_THIS_PROCESS = False
ANNOUNCE_COOLDOWN_SECONDS = 15 * 60  # 15 minutes


# ---------- DB Pool ----------
DB_POOL = None  # psycopg2.pool.SimpleConnectionPool


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def normalize_to_utc(ts: datetime | None) -> datetime:
    ts = ts or now_utc()
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def init_db_pool(minconn: int = 1, maxconn: int = 10):
    """Initialize a psycopg2 connection pool."""
    global DB_POOL
    if DB_POOL:
        return
    DB_POOL = pg_pool.SimpleConnectionPool(
        minconn=minconn,
        maxconn=maxconn,
        dsn=DATABASE_URL,
        cursor_factory=RealDictCursor,
        sslmode="require",
    )
    logging.info("DB pool initialized.")


@contextmanager
def db_conn():
    """
    Pool-backed DB context manager.
    Commits on success, rolls back on error, returns connection to pool.
    """
    if not DB_POOL:
        raise RuntimeError("DB_POOL not initialized. Call init_db_pool() first.")
    conn = DB_POOL.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        DB_POOL.putconn(conn)


async def run_db(fn, *args, **kwargs):
    """Run a sync DB function in a worker thread to avoid blocking asyncio."""
    return await asyncio.to_thread(fn, *args, **kwargs)


def compress_report(text: str) -> bytes:
    return gzip.compress(text.encode("utf-8"), compresslevel=9)


def decompress_report(raw_gz: bytes) -> str:
    try:
        if isinstance(raw_gz, memoryview):
            raw_gz = raw_gz.tobytes()
        return gzip.decompress(raw_gz).decode("utf-8", errors="replace")
    except Exception:
        return ""


def extract_report_text_for_row(row) -> str:
    raw = row.get("raw")
    if raw:
        return raw
    raw_gz = row.get("raw_gz")
    if raw_gz:
        return decompress_report(raw_gz)
    return ""


def hash_report(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def castle_bonus(c: int) -> float:
    return (c ** 0.5) / 100 if c else 0.0


# ---------- Parsing ----------
def parse_spy(text: str):
    kingdom, dp, castles = None, None, 0
    for line in text.splitlines():
        ll = line.lower().strip()
        if ll.startswith("target:"):
            kingdom = line.split(":", 1)[1].strip()
        if "approximate defensive power" in ll or "defensive power" in ll:
            m = re.search(r"\d+", line.replace(",", ""))
            if m:
                dp = int(m.group())
        if "number of castles" in ll:
            m = re.search(r"\d+", line)
            if m:
                castles = int(m.group())
    return kingdom, dp, castles


def parse_sr_troops(text: str) -> dict:
    """
    Extract ALL home troop counts from SR section:
    "Our spies also found the following information about the kingdom's troops:"
    """
    troops = {}
    in_troops = False

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        ll = line.lower()

        if "our spies also found the following information about the kingdom's troops" in ll:
            in_troops = True
            continue

        if not in_troops:
            continue

        # stop conditions
        if "approximate defensive power" in ll:
            break
        if any(x in ll for x in (
            "the following recent market transactions",
            "the following technology information",
            "our spies also found the following information about the kingdom's resources",
            "the following information was found regarding troop movements",
        )):
            break

        m = re.match(r"^(.+?):\s*([\d,]+)\s*$", line)
        if not m:
            continue

        name = m.group(1).strip()
        val = int(m.group(2).replace(",", ""))

        if name.lower().startswith("population"):
            continue
        if len(name) < 2 or val < 0:
            continue

        troops[name] = val

    return troops


def parse_tech(text: str):
    """
    Extract ONLY from the explicit tech section:
    "The following technology information was also discovered:"
    """
    techs = []
    in_tech = False

    blocked_prefixes = (
        # units / troop stats
        "heavy cavalry", "light cavalry", "archers", "pikemen", "peasants", "knights",
        "spies sent", "spies lost", "population", "elites",
        # resources / misc stats
        "horses", "blue gems", "green gems", "gold", "food", "wood", "stone", "land",
        "networth", "honour", "ranking", "number of castles", "approximate defensive power",
        # settlement/building lines
        "current level", "buildings built", "housing", "barn", "granary", "stables", "inn", "mason",
    )

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            if in_tech:
                break
            continue

        ll = line.lower().strip()

        if "the following technology information was also discovered" in ll:
            in_tech = True
            continue

        if not in_tech:
            continue

        if any(x in ll for x in (
            "the following recent market transactions",
            "our spies also found the following information",
            "the following information about the",
        )):
            break

        if ll.endswith(":") and "technology information" not in ll:
            break

        s = line.lstrip("•-*—– ").strip()
        s_ll = s.lower()

        if any(s_ll.startswith(p) for p in blocked_prefixes):
            continue

        m = re.match(r"^(.+?)\s+(?:lv\.?|lvl\.?|level)\s*(\d{1,3})\s*$", s, re.IGNORECASE)
        if not m:
            continue

        name = m.group(1).strip()
        lvl = int(m.group(2))

        if not (1 <= lvl <= 300):
            continue
        if len(name) < 3:
            continue
        if name.lower().startswith(("target", "subject", "received")):
            continue

        techs.append((name, lvl))

    return techs


def is_battle_related_tech(name: str) -> bool:
    """
    Heuristic filter for "battle-related" tech.
    Keep this broad so you don't miss combat tech.
    """
    n = (name or "").lower()
    keywords = (
        "attack", "defense", "defence", "combat", "battle", "war", "weapon", "armor", "armour",
        "tactics", "training", "discipline", "fort", "fortification", "siege",
        "archer", "archery", "pikemen", "pike", "cavalry", "knight", "infantry",
        "range", "ranged", "melee", "damage", "hp", "health", "morale",
    )
    return any(k in n for k in keywords)


# ---------- Discord perms helpers ----------
def get_member_for_perms(guild: discord.Guild):
    try:
        if guild.me:
            return guild.me
        if bot.user:
            return guild.get_member(bot.user.id)
    except Exception:
        return None
    return None


def can_send(channel: discord.abc.GuildChannel, guild: discord.Guild) -> bool:
    try:
        member = get_member_for_perms(guild)
        if not member:
            return True
        return channel.permissions_for(member).send_messages
    except Exception:
        return True


def truncate_for_discord(s: str, limit: int = 1800) -> str:
    s = s or ""
    if len(s) <= limit:
        return s
    return s[:limit] + "\n…(truncated)…"


async def send_error(guild: discord.Guild, msg: str, tb: str | None = None):
    """Send safe, truncated error logs to your error channel + log full stack to console."""
    try:
        ch = discord.utils.get(guild.text_channels, name=ERROR_CHANNEL_NAME)
        if ch and can_send(ch, guild):
            payload = msg
            if tb:
                payload += "\n\n" + truncate_for_discord(tb, 1800)
            payload = truncate_for_discord(payload, 1900)
            await ch.send(f"⚠️ ERROR LOG:\n```py\n{payload}\n```")
    except Exception:
        pass
    logging.error(msg)


# ---------- DB Schema ----------
def init_db():
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS spy_reports (
            id SERIAL PRIMARY KEY,
            kingdom TEXT,
            defense_power INTEGER,
            castles INTEGER,
            created_at TIMESTAMPTZ,
            raw TEXT,
            raw_gz BYTEA,
            report_hash TEXT UNIQUE
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS dp_sessions (
            id SERIAL PRIMARY KEY,
            kingdom TEXT,
            base_dp INTEGER,
            castles INTEGER,
            current_dp INTEGER,
            hits INTEGER,
            last_hit TEXT,
            captured_at TIMESTAMPTZ
        );
        """)

        # tech history (per report)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS tech_index (
            id SERIAL PRIMARY KEY,
            kingdom TEXT,
            tech_name TEXT,
            tech_level INTEGER,
            captured_at TIMESTAMPTZ,
            report_id INTEGER REFERENCES spy_reports(id),
            UNIQUE(kingdom, tech_name, tech_level, report_id)
        );
        """)

        # deduped best tech per kingdom
        cur.execute("""
        CREATE TABLE IF NOT EXISTS kingdom_tech (
            kingdom TEXT NOT NULL,
            tech_name TEXT NOT NULL,
            best_level INTEGER NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            source_report_id INTEGER,
            PRIMARY KEY (kingdom, tech_name)
        );
        """)

        # player_tech compatibility: VIEW to kingdom_tech (so !tech reads from "player_tech")
        cur.execute("DROP VIEW IF EXISTS player_tech;")
        cur.execute("""
        CREATE VIEW player_tech AS
        SELECT kingdom, tech_name, best_level, updated_at, source_report_id
        FROM kingdom_tech;
        """)

        # troop snapshots per SR (home troops)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS troop_snapshots (
            id SERIAL PRIMARY KEY,
            kingdom TEXT NOT NULL,
            report_id INTEGER REFERENCES spy_reports(id),
            captured_at TIMESTAMPTZ NOT NULL,
            unit_name TEXT NOT NULL,
            unit_count INTEGER NOT NULL,
            UNIQUE(report_id, unit_name)
        );
        """)

        # meta for announcements
        cur.execute("""
        CREATE TABLE IF NOT EXISTS bot_meta (
            k TEXT PRIMARY KEY,
            v TEXT,
            updated_at TIMESTAMPTZ
        );
        """)

        # indices
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS spy_reports_report_hash_uq ON spy_reports(report_hash);")
        cur.execute("""
            CREATE INDEX IF NOT EXISTS spy_reports_kingdom_created_at_idx
            ON spy_reports (kingdom, created_at DESC, id DESC);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS troop_snapshots_kingdom_captured_at_idx
            ON troop_snapshots (kingdom, captured_at DESC, report_id DESC);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS tech_index_kingdom_captured_at_idx
            ON tech_index (kingdom, captured_at DESC, report_id DESC);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS tech_index_name_idx
            ON tech_index (tech_name);
        """)


def heal_sequences():
    with db_conn() as conn, conn.cursor() as cur:
        for table in ["spy_reports", "dp_sessions", "tech_index", "troop_snapshots"]:
            cur.execute(
                f"SELECT setval(pg_get_serial_sequence('{table}','id'), "
                f"COALESCE((SELECT MAX(id) FROM {table}), 1), true);"
            )


# ---------- DB Helpers (sync; call via run_db) ----------
def _meta_get(cur, key: str):
    cur.execute("SELECT v FROM bot_meta WHERE k=%s LIMIT 1;", (key,))
    row = cur.fetchone()
    return row["v"] if row else None


def _meta_set(cur, key: str, value: str):
    cur.execute("""
        INSERT INTO bot_meta (k, v, updated_at)
        VALUES (%s, %s, %s)
        ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v, updated_at=EXCLUDED.updated_at;
    """, (key, value, now_utc()))


def sync_fuzzy_kingdom(query: str):
    if not query:
        return None
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT DISTINCT kingdom FROM spy_reports WHERE kingdom IS NOT NULL;")
        names = [r["kingdom"] for r in cur.fetchall() if r.get("kingdom")]
    match = difflib.get_close_matches(query, names, 1, 0.5)
    return match[0] if match else None


def sync_get_spy_by_id(report_id: int):
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, kingdom, defense_power, castles, created_at, raw, raw_gz
            FROM spy_reports
            WHERE id=%s
            LIMIT 1;
        """, (int(report_id),))
        return cur.fetchone()


def sync_get_latest_spy_for_kingdom(kingdom: str):
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, kingdom, defense_power, castles, created_at, raw, raw_gz
            FROM spy_reports
            WHERE kingdom=%s
            ORDER BY created_at DESC NULLS LAST, id DESC
            LIMIT 1;
        """, (kingdom,))
        return cur.fetchone()


def sync_get_latest_dp_spy_for_kingdom(kingdom: str):
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, kingdom, defense_power, castles, created_at, raw, raw_gz
            FROM spy_reports
            WHERE kingdom=%s AND defense_power IS NOT NULL AND defense_power > 0
            ORDER BY created_at DESC NULLS LAST, id DESC
            LIMIT 1;
        """, (kingdom,))
        return cur.fetchone()


def sync_get_latest_dp_spy_any():
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, kingdom, defense_power, castles, created_at, raw, raw_gz
            FROM spy_reports
            WHERE defense_power IS NOT NULL AND defense_power > 0
            ORDER BY created_at DESC NULLS LAST, id DESC
            LIMIT 1;
        """)
        return cur.fetchone()


def sync_get_spy_history(kingdom: str, limit: int = 5):
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, kingdom, defense_power, castles, created_at
            FROM spy_reports
            WHERE kingdom=%s
            ORDER BY created_at DESC NULLS LAST, id DESC
            LIMIT %s;
        """, (kingdom, int(limit)))
        return cur.fetchall()


def sync_ensure_ap_session(kingdom: str) -> bool:
    if not kingdom:
        return False

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, base_dp, castles, current_dp, hits, last_hit, captured_at
            FROM dp_sessions
            WHERE kingdom=%s
            ORDER BY captured_at DESC NULLS LAST, id DESC
            LIMIT 1;
        """, (kingdom,))
        sess = cur.fetchone()

    if sess and int(sess.get("base_dp") or 0) > 0:
        return True

    # rebuild from latest DP spy report
    spy = sync_get_latest_dp_spy_for_kingdom(kingdom)
    if not spy:
        return False

    base_dp = int(spy["defense_power"] or 0)
    castles = int(spy["castles"] or 0)
    if base_dp <= 0:
        return False

    captured_at = spy.get("created_at") or now_utc()
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM dp_sessions WHERE kingdom=%s;", (kingdom,))
        cur.execute("""
            INSERT INTO dp_sessions (kingdom, base_dp, castles, current_dp, hits, last_hit, captured_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s);
        """, (kingdom, base_dp, castles, base_dp, 0, None, captured_at))
    return True


def sync_get_ap_session_row(kingdom: str):
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT base_dp, current_dp, hits, last_hit, castles, captured_at
            FROM dp_sessions
            WHERE kingdom=%s
            ORDER BY captured_at DESC NULLS LAST, id DESC
            LIMIT 1;
        """, (kingdom,))
        return cur.fetchone()


def sync_apply_ap_hit(kingdom: str, red: float, who: str):
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, current_dp, hits
            FROM dp_sessions
            WHERE kingdom=%s
            ORDER BY captured_at DESC NULLS LAST, id DESC
            LIMIT 1;
        """, (kingdom,))
        sess = cur.fetchone()
        if not sess:
            return {"ok": False}

        current_dp = int(sess.get("current_dp") or 0)
        new_dp = ceil(current_dp * (1 - red))
        new_hits = int(sess.get("hits") or 0) + 1

        cur.execute("""
            UPDATE dp_sessions
            SET current_dp=%s, hits=%s, last_hit=%s
            WHERE id=%s;
        """, (new_dp, new_hits, who, sess["id"]))
    return {"ok": True}


def sync_reset_ap_session(kingdom: str):
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, base_dp
            FROM dp_sessions
            WHERE kingdom=%s
            ORDER BY captured_at DESC NULLS LAST, id DESC
            LIMIT 1;
        """, (kingdom,))
        sess = cur.fetchone()
        if not sess:
            return {"ok": False}
        base_dp = int(sess.get("base_dp") or 0)
        cur.execute("""
            UPDATE dp_sessions
            SET current_dp=%s, hits=0, last_hit=NULL
            WHERE id=%s;
        """, (base_dp, sess["id"]))
    return {"ok": True}


def sync_rebuild_ap_session(kingdom: str) -> bool:
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM dp_sessions WHERE kingdom=%s;", (kingdom,))
    return sync_ensure_ap_session(kingdom)


def sync_upsert_troop_snapshot(cur, kingdom: str, report_id: int, captured_at, troops: dict) -> int:
    if not kingdom or not report_id or not troops:
        return 0
    captured_at = captured_at or now_utc()
    inserted = 0
    for unit_name, unit_count in troops.items():
        cur.execute("""
            INSERT INTO troop_snapshots (kingdom, report_id, captured_at, unit_name, unit_count)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (report_id, unit_name) DO NOTHING;
        """, (kingdom, report_id, captured_at, unit_name, int(unit_count)))
        inserted += 1
    return inserted


def sync_get_latest_troop_snapshot_units(kingdom: str):
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT report_id, captured_at
            FROM troop_snapshots
            WHERE kingdom=%s
            ORDER BY captured_at DESC, report_id DESC
            LIMIT 1;
        """, (kingdom,))
        head = cur.fetchone()
        if not head:
            return None, None, {}

        report_id = int(head["report_id"])
        captured_at = head["captured_at"]

        cur.execute("""
            SELECT unit_name, unit_count
            FROM troop_snapshots
            WHERE kingdom=%s AND report_id=%s
            ORDER BY unit_name ASC;
        """, (kingdom, report_id))
        rows = cur.fetchall()

    troops = {r["unit_name"]: int(r["unit_count"]) for r in rows}
    return report_id, captured_at, troops


def sync_get_last_two_troop_snapshots(kingdom: str):
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT report_id, captured_at
            FROM troop_snapshots
            WHERE kingdom=%s
            ORDER BY captured_at DESC, report_id DESC
            LIMIT 2;
        """, (kingdom,))
        heads = cur.fetchall()

        if not heads or len(heads) < 2:
            return None

        newest = heads[0]
        prev = heads[1]

        def load(report_id: int):
            cur.execute("""
                SELECT unit_name, unit_count
                FROM troop_snapshots
                WHERE kingdom=%s AND report_id=%s;
            """, (kingdom, report_id))
            rows = cur.fetchall()
            return {r["unit_name"]: int(r["unit_count"]) for r in rows}

        return {
            "new": {"report_id": int(newest["report_id"]), "captured_at": newest["captured_at"], "troops": load(int(newest["report_id"]))},
            "old": {"report_id": int(prev["report_id"]), "captured_at": prev["captured_at"], "troops": load(int(prev["report_id"]))},
        }


def sync_upsert_best_tech(cur, kingdom: str, tech_name: str, level: int, report_id: int, captured_at):
    """
    Best-tech upsert rules:
    - Higher level wins
    - If same level, newer updated_at wins
    """
    cur.execute("""
        INSERT INTO kingdom_tech (kingdom, tech_name, best_level, updated_at, source_report_id)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (kingdom, tech_name)
        DO UPDATE SET
          best_level = CASE
            WHEN EXCLUDED.best_level > kingdom_tech.best_level THEN EXCLUDED.best_level
            WHEN EXCLUDED.best_level = kingdom_tech.best_level AND EXCLUDED.updated_at > kingdom_tech.updated_at THEN EXCLUDED.best_level
            ELSE kingdom_tech.best_level
          END,
          updated_at = CASE
            WHEN EXCLUDED.best_level > kingdom_tech.best_level THEN EXCLUDED.updated_at
            WHEN EXCLUDED.best_level = kingdom_tech.best_level AND EXCLUDED.updated_at > kingdom_tech.updated_at THEN EXCLUDED.updated_at
            ELSE kingdom_tech.updated_at
          END,
          source_report_id = CASE
            WHEN EXCLUDED.best_level > kingdom_tech.best_level THEN EXCLUDED.source_report_id
            WHEN EXCLUDED.best_level = kingdom_tech.best_level AND EXCLUDED.updated_at > kingdom_tech.updated_at THEN EXCLUDED.source_report_id
            ELSE kingdom_tech.source_report_id
          END;
    """, (kingdom, tech_name, level, captured_at or now_utc(), report_id))


def sync_index_tech_for_report(cur, kingdom: str, report_id: int, captured_at, techs: list[tuple[str, int]]):
    """
    Inserts tech lines into tech_index (history) and updates kingdom_tech best list.
    Only indexes tech names that match "battle-related" filter.
    """
    if not kingdom or not report_id or not techs:
        return {"history": 0, "best_updates": 0}

    captured_at = captured_at or now_utc()
    history = 0
    best_updates = 0

    for name, lvl in techs:
        if not is_battle_related_tech(name):
            continue

        cur.execute("""
            INSERT INTO tech_index (kingdom, tech_name, tech_level, captured_at, report_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, (kingdom, name, int(lvl), captured_at, int(report_id)))
        history += 1

        sync_upsert_best_tech(cur, kingdom, name, int(lvl), int(report_id), captured_at)
        best_updates += 1

    return {"history": history, "best_updates": best_updates}


def sync_store_report(msg_content: str, created_at_utc: datetime):
    """
    Stores spy report deduped by hash. Also indexes tech + troops, ensures AP session if DP.
    """
    kingdom, dp, castles = parse_spy(msg_content)
    techs = parse_tech(msg_content)
    sr_troops = parse_sr_troops(msg_content)

    should_save = bool(kingdom) and (
        (dp is not None and dp >= 1000) or
        (techs and len(techs) >= 1) or
        (sr_troops and len(sr_troops) >= 1)
    )
    if not should_save:
        return {"saved": False}

    h = hash_report(msg_content)
    raw_gz = psycopg2.Binary(compress_report(msg_content))
    raw_text = msg_content if KEEP_RAW_TEXT else None

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT id FROM spy_reports WHERE report_hash=%s LIMIT 1;", (h,))
        exists = cur.fetchone()

        if not exists:
            cur.execute("""
                INSERT INTO spy_reports (kingdom, defense_power, castles, created_at, raw, raw_gz, report_hash)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                RETURNING id, kingdom, defense_power, castles, created_at, raw, raw_gz;
            """, (kingdom, dp, castles, created_at_utc, raw_text, raw_gz, h))
            row = cur.fetchone()

            if techs:
                sync_index_tech_for_report(cur, kingdom, int(row["id"]), row.get("created_at") or created_at_utc, techs)

            if sr_troops:
                sync_upsert_troop_snapshot(cur, kingdom, int(row["id"]), row.get("created_at") or created_at_utc, sr_troops)

            if dp is not None and dp >= 1000:
                sync_ensure_ap_session(kingdom)

            return {"saved": True, "duplicate": False, "row": row}

        # duplicate: repair-mode (index against existing id)
        rep_id = int(exists["id"])
        if techs or sr_troops:
            # load kingdom from message parse (best-effort)
            if techs:
                sync_index_tech_for_report(cur, kingdom, rep_id, created_at_utc, techs)
            if sr_troops:
                sync_upsert_troop_snapshot(cur, kingdom, rep_id, created_at_utc, sr_troops)

        return {"saved": True, "duplicate": True, "row": None}


def sync_techindex_all(days: int | None = None):
    """
    Scans saved spy_reports and ensures tech_index + kingdom_tech are filled.
    If days provided, only scans reports from last N days.
    """
    since = None
    if days and int(days) > 0:
        since = now_utc() - timedelta(days=int(days))

    stats = {"reports_scanned": 0, "reports_with_tech": 0, "tech_history_rows": 0, "best_updates": 0}

    with db_conn() as conn, conn.cursor() as cur:
        if since:
            cur.execute("""
                SELECT id, kingdom, created_at, raw, raw_gz
                FROM spy_reports
                WHERE created_at >= %s AND kingdom IS NOT NULL
                ORDER BY created_at DESC NULLS LAST, id DESC;
            """, (since,))
        else:
            cur.execute("""
                SELECT id, kingdom, created_at, raw, raw_gz
                FROM spy_reports
                WHERE kingdom IS NOT NULL
                ORDER BY created_at DESC NULLS LAST, id DESC;
            """)

        rows = cur.fetchall()

        for row in rows:
            stats["reports_scanned"] += 1
            k = row.get("kingdom")
            if not k:
                continue

            text = row.get("raw") or (decompress_report(row.get("raw_gz")) if row.get("raw_gz") else "")
            if not text:
                continue

            techs = parse_tech(text)
            if not techs:
                continue

            stats["reports_with_tech"] += 1
            res = sync_index_tech_for_report(cur, k, int(row["id"]), row.get("created_at") or now_utc(), techs)
            stats["tech_history_rows"] += int(res["history"])
            stats["best_updates"] += int(res["best_updates"])

    return stats


def sync_techpull_kingdom(kingdom: str):
    """
    Rebuild deduped best tech list for a single kingdom from ALL its saved reports.
    Clears kingdom_tech rows for that kingdom and re-indexes from spy_reports.
    """
    stats = {"reports_scanned": 0, "reports_with_tech": 0, "tech_history_rows": 0, "best_updates": 0}

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM kingdom_tech WHERE kingdom=%s;", (kingdom,))

        cur.execute("""
            SELECT id, kingdom, created_at, raw, raw_gz
            FROM spy_reports
            WHERE kingdom=%s
            ORDER BY created_at ASC NULLS LAST, id ASC;
        """, (kingdom,))
        rows = cur.fetchall()

        for row in rows:
            stats["reports_scanned"] += 1
            text = row.get("raw") or (decompress_report(row.get("raw_gz")) if row.get("raw_gz") else "")
            if not text:
                continue

            techs = parse_tech(text)
            if not techs:
                continue

            stats["reports_with_tech"] += 1
            res = sync_index_tech_for_report(cur, kingdom, int(row["id"]), row.get("created_at") or now_utc(), techs)
            stats["tech_history_rows"] += int(res["history"])
            stats["best_updates"] += int(res["best_updates"])

    return stats


def sync_get_best_tech_for_kingdom(kingdom: str, limit: int = 60):
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT tech_name, best_level, updated_at, source_report_id
            FROM player_tech
            WHERE kingdom=%s
            ORDER BY best_level DESC, updated_at DESC
            LIMIT %s;
        """, (kingdom, int(limit)))
        return cur.fetchall()


def sync_get_techtop_common(limit: int = 15):
    """
    "Most common indexed trainings across all kingdoms"
    -> count occurrences in tech_index by tech_name.
    """
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT tech_name, COUNT(*) AS ct
            FROM tech_index
            GROUP BY tech_name
            ORDER BY ct DESC, tech_name ASC
            LIMIT %s;
        """, (int(limit),))
        return cur.fetchall()


def sync_backfill(days: int | None = None):
    """
    Ensures:
    - tech_index + kingdom_tech updated for all relevant reports
    - troop snapshots exist where SR troops exist
    - (does not re-save spy_reports; it re-parses stored raw/raw_gz)
    If days provided: only scans that time window.
    """
    since = None
    if days and int(days) > 0:
        since = now_utc() - timedelta(days=int(days))

    stats = {
        "reports_scanned": 0,
        "tech_reports": 0,
        "tech_history_rows": 0,
        "best_updates": 0,
        "troop_reports": 0,
        "troop_rows": 0,
    }

    with db_conn() as conn, conn.cursor() as cur:
        if since:
            cur.execute("""
                SELECT id, kingdom, created_at, raw, raw_gz
                FROM spy_reports
                WHERE created_at >= %s AND kingdom IS NOT NULL
                ORDER BY created_at DESC NULLS LAST, id DESC;
            """, (since,))
        else:
            cur.execute("""
                SELECT id, kingdom, created_at, raw, raw_gz
                FROM spy_reports
                WHERE kingdom IS NOT NULL
                ORDER BY created_at DESC NULLS LAST, id DESC;
            """)

        rows = cur.fetchall()

        for row in rows:
            stats["reports_scanned"] += 1
            k = row.get("kingdom")
            if not k:
                continue

            text = row.get("raw") or (decompress_report(row.get("raw_gz")) if row.get("raw_gz") else "")
            if not text:
                continue

            # tech
            techs = parse_tech(text)
            if techs:
                stats["tech_reports"] += 1
                res = sync_index_tech_for_report(cur, k, int(row["id"]), row.get("created_at") or now_utc(), techs)
                stats["tech_history_rows"] += int(res["history"])
                stats["best_updates"] += int(res["best_updates"])

            # troops
            troops = parse_sr_troops(text)
            if troops:
                stats["troop_reports"] += 1
                inserted = sync_upsert_troop_snapshot(cur, k, int(row["id"]), row.get("created_at") or now_utc(), troops)
                stats["troop_rows"] += int(inserted)

    return stats


# ---------- Embeds ----------
def build_spy_embed(row):
    dp = int(row.get("defense_power") or 0) if row.get("defense_power") is not None else 0
    castles = int(row.get("castles") or 0)
    adjusted = ceil(dp * (1 + castle_bonus(castles))) if dp > 0 else 0

    embed = discord.Embed(title="🕵️ Spy Report", color=0x5865F2)
    embed.add_field(name="Kingdom", value=row.get("kingdom") or "Unknown", inline=False)
    embed.add_field(name="Base DP", value=(f"{dp:,}" if dp else "N/A"), inline=True)
    embed.add_field(name="Adjusted DP", value=(f"{adjusted:,}" if adjusted else "N/A"), inline=True)
    embed.add_field(name="Castles", value=str(castles), inline=True)
    embed.set_footer(text=f"ID {row['id']} • Captured {row.get('created_at')}")
    return embed


def build_calc_embed(target: str, dp: int, castles: int, used: str):
    adj = ceil(dp * (1 + castle_bonus(castles)))
    embed = discord.Embed(title="⚔️ Combat Calculator", color=0x5865F2)
    embed.add_field(name="Target", value=f"{target} {used}", inline=False)
    embed.add_field(name="Base DP", value=f"{dp:,}", inline=True)
    embed.add_field(name="Adjusted DP", value=f"{adj:,}", inline=True)
    embed.add_field(name="Castles", value=str(castles), inline=True)
    embed.add_field(name="HC Needed (est.)", value=f"{ceil(adj / HEAVY_CAVALRY_AP):,}", inline=True)

    for label, red in AP_REDUCTIONS:
        rem = ceil(adj * (1 - red))
        embed.add_field(
            name=f"{label} (-{int(red*100)}%)",
            value=f"Remaining DP: {rem:,}\nRemaining HC: {ceil(rem/HEAVY_CAVALRY_AP):,}",
            inline=False
        )
    return embed


def build_ap_embed_from_row(kingdom: str, row):
    if not row:
        return None
    base_dp = int(row.get("base_dp") or 0)
    current_dp = int(row.get("current_dp") or 0)
    hits = int(row.get("hits") or 0)
    castles = int(row.get("castles") or 0)

    embed = discord.Embed(title=f"⚔️ AP Planner • {kingdom}", color=0xE74C3C)
    embed.add_field(name="Base DP", value=f"{base_dp:,}", inline=True)
    embed.add_field(name="Current DP", value=f"{current_dp:,}", inline=True)
    embed.add_field(name="Hits Applied", value=str(hits), inline=True)
    embed.add_field(name="Castles", value=str(castles), inline=True)
    embed.add_field(name="HC Needed (est.)", value=f"{ceil(current_dp / HEAVY_CAVALRY_AP):,}", inline=True)
    if row.get("last_hit"):
        embed.set_footer(text=f"Last hit by {row['last_hit']} • Captured {row.get('captured_at')}")
    else:
        embed.set_footer(text=f"Captured {row.get('captured_at')}")
    return embed


# ---------- AP View ----------
class APView(View):
    def __init__(self, kingdom: str, timeout: float = 600):
        super().__init__(timeout=timeout)
        self.kingdom = kingdom

        for label, red in AP_REDUCTIONS:
            self.add_item(self._make_hit_button(label, red))
        self.add_item(self._make_reset_button())
        self.add_item(self._make_rebuild_button())

    def _make_hit_button(self, label: str, red: float) -> Button:
        async def callback(interaction: discord.Interaction):
            await interaction.response.defer(thinking=False)
            try:
                async with ap_lock:
                    who = interaction.user.display_name if interaction.user else "Unknown"
                    res = await run_db(sync_apply_ap_hit, self.kingdom, red, who)

                if not res.get("ok"):
                    return await interaction.followup.send("❌ No active session. Paste a DP spy report first, then run `!ap` again.")

                row = await run_db(sync_get_ap_session_row, self.kingdom)
                embed = build_ap_embed_from_row(self.kingdom, row)
                if embed:
                    try:
                        await interaction.message.edit(embed=embed, view=self)
                    except Exception:
                        await interaction.followup.send(embed=embed, view=self)

            except Exception as e:
                tb = traceback.format_exc()
                logging.exception("AP hit button error")
                if interaction.guild:
                    await send_error(interaction.guild, f"AP hit button error: {e}", tb=tb)
                await interaction.followup.send("⚠️ Failed to apply hit.")

        btn = Button(label=label, style=discord.ButtonStyle.danger)
        btn.callback = callback
        return btn

    def _make_reset_button(self) -> Button:
        async def callback(interaction: discord.Interaction):
            await interaction.response.defer(thinking=False)
            try:
                async with ap_lock:
                    res = await run_db(sync_reset_ap_session, self.kingdom)

                if not res.get("ok"):
                    return await interaction.followup.send("❌ No active session to reset.")

                row = await run_db(sync_get_ap_session_row, self.kingdom)
                embed = build_ap_embed_from_row(self.kingdom, row)
                if embed:
                    try:
                        await interaction.message.edit(embed=embed, view=self)
                    except Exception:
                        await interaction.followup.send(embed=embed, view=self)

            except Exception as e:
                tb = traceback.format_exc()
                logging.exception("AP reset error")
                if interaction.guild:
                    await send_error(interaction.guild, f"AP reset error: {e}", tb=tb)
                await interaction.followup.send("⚠️ Failed to reset.")

        btn = Button(label="Reset", style=discord.ButtonStyle.secondary)
        btn.callback = callback
        return btn

    def _make_rebuild_button(self) -> Button:
        async def callback(interaction: discord.Interaction):
            await interaction.response.defer(thinking=False)
            try:
                async with ap_lock:
                    ok = await run_db(sync_rebuild_ap_session, self.kingdom)

                if not ok:
                    return await interaction.followup.send("❌ Could not rebuild (no valid DP spy report found).")

                row = await run_db(sync_get_ap_session_row, self.kingdom)
                embed = build_ap_embed_from_row(self.kingdom, row)
                if embed:
                    try:
                        await interaction.message.edit(embed=embed, view=self)
                    except Exception:
                        await interaction.followup.send(embed=embed, view=self)

            except Exception as e:
                tb = traceback.format_exc()
                logging.exception("AP rebuild error")
                if interaction.guild:
                    await send_error(interaction.guild, f"AP rebuild error: {e}", tb=tb)
                await interaction.followup.send("⚠️ Failed to rebuild.")

        btn = Button(label="Rebuild", style=discord.ButtonStyle.primary)
        btn.callback = callback
        return btn


# ---------- Admin helper ----------
def _is_admin(ctx: commands.Context) -> bool:
    try:
        if ctx.guild and ctx.author and getattr(ctx.author, "guild_permissions", None):
            return bool(ctx.author.guild_permissions.administrator)
    except Exception:
        pass
    return False


# ---------- Events ----------
@bot.event
async def on_ready():
    global ANNOUNCED_READY_THIS_PROCESS

    try:
        await run_db(init_db_pool, 1, 10)
        await run_db(init_db)
        await run_db(heal_sequences)
    except Exception:
        logging.exception("DB init failed")

    if ANNOUNCED_READY_THIS_PROCESS:
        logging.info("on_ready called again (same process) - announcement suppressed.")
        return
    ANNOUNCED_READY_THIS_PROCESS = True

    # version/cooldown dedupe (DB-backed)
    try:
        def _dedupe():
            with db_conn() as conn, conn.cursor() as cur:
                last_ver = _meta_get(cur, "announce_last_ver")
                last_ts = _meta_get(cur, "announce_last_ts")
                now_ts = int(time.time())
                last_ts_int = int(last_ts) if last_ts and str(last_ts).isdigit() else 0

                if last_ver == BOT_VERSION and (now_ts - last_ts_int) < ANNOUNCE_COOLDOWN_SECONDS:
                    return False

                _meta_set(cur, "announce_last_ver", BOT_VERSION)
                _meta_set(cur, "announce_last_ts", str(now_ts))
                return True

        ok = await run_db(_dedupe)
        if not ok:
            logging.info("Announcement suppressed (same version + cooldown).")
            return

    except Exception:
        logging.exception("Announcement dedupe failed")

    patch_lines = "\n".join([f"• {x}" for x in PATCH_NOTES])
    for guild in bot.guilds:
        ch = discord.utils.get(guild.text_channels, name=ERROR_CHANNEL_NAME)
        if ch and can_send(ch, guild):
            try:
                await ch.send(
                    f"✅ **KG2 Recon Bot restarted**\n"
                    f"Version: `{BOT_VERSION}`\n"
                    f"Patch:\n{patch_lines}"
                )
            except Exception:
                pass


@bot.event
async def on_message(msg: discord.Message):
    if msg.author.bot or not msg.guild:
        return

    try:
        ts = normalize_to_utc(msg.created_at)
        result = await run_db(sync_store_report, msg.content, ts)

        if result.get("saved") and not result.get("duplicate") and result.get("row"):
            row = result["row"]
            if can_send(msg.channel, msg.guild):
                await msg.channel.send(embed=build_spy_embed(row))
        elif result.get("saved") and result.get("duplicate"):
            if can_send(msg.channel, msg.guild):
                await msg.channel.send("✅ Duplicate spy report detected (repair mode applied).")

    except Exception as e:
        tb = traceback.format_exc()
        logging.exception("on_message error")
        await send_error(msg.guild, f"on_message error: {e}", tb=tb)

    await bot.process_commands(msg)


# ---------- Commands ----------
@bot.command()
async def calc(ctx, *, kingdom: str = None):
    """
    - !calc            -> prompts paste (default)
    - !calc <kingdom>  -> uses latest DP report for that kingdom (DB)
    - !calc db         -> uses latest DP report overall (DB)
    """
    try:
        arg = (kingdom or "").strip()

        if arg.lower() in ("db", "last", "latest"):
            row = await run_db(sync_get_latest_dp_spy_any)
            if not row:
                return await ctx.send("❌ No saved DP spy reports in DB yet.")
            dp = int(row["defense_power"])
            c = int(row["castles"] or 0)
            target = row["kingdom"] or "Unknown"
            return await ctx.send(embed=build_calc_embed(target, dp, c, f"(from DB: {row['id']})"))

        if arg:
            real = await run_db(sync_fuzzy_kingdom, arg)
            real = real or arg
            row = await run_db(sync_get_latest_dp_spy_for_kingdom, real)
            if not row:
                return await ctx.send(f"❌ No saved DP reports for **{real}**. Paste a full spy report and try again.")
            dp = int(row["defense_power"])
            c = int(row["castles"] or 0)
            target = row["kingdom"] or real
            return await ctx.send(embed=build_calc_embed(target, dp, c, f"(from DB: {row['id']})"))

        await ctx.send("📄 Paste the spy report you want to calculate against (you have 90 seconds).")
        try:
            m = await bot.wait_for(
                "message",
                timeout=90,
                check=lambda x: x.author == ctx.author and x.channel == ctx.channel
            )
        except asyncio.TimeoutError:
            return await ctx.send("⏰ Timed out. Run `!calc` again.")

        k, dp, c = parse_spy(m.content)
        if not k or not dp:
            return await ctx.send("❌ Could not parse that spy report. Make sure it includes Target + Defensive Power.")
        await ctx.send(embed=build_calc_embed(k, int(dp), int(c or 0), "(pasted)"))

    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("⚠️ calc failed.")
        await send_error(ctx.guild, f"calc error: {e}", tb=tb)


@bot.command()
async def spy(ctx, *, kingdom: str):
    """!spy <kingdom> -> latest saved spy report for that kingdom."""
    try:
        real = await run_db(sync_fuzzy_kingdom, kingdom)
        real = real or kingdom
        row = await run_db(sync_get_latest_spy_for_kingdom, real)
        if not row:
            return await ctx.send(f"❌ No saved reports for **{real}**.")
        await ctx.send(embed=build_spy_embed(row))
    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("⚠️ spy failed.")
        await send_error(ctx.guild, f"spy error: {e}", tb=tb)


@bot.command()
async def spyid(ctx, report_id: int):
    """!spyid <id> -> shows a saved spy report by DB ID."""
    try:
        row = await run_db(sync_get_spy_by_id, int(report_id))
        if not row:
            return await ctx.send("❌ No report found with that ID.")
        await ctx.send(embed=build_spy_embed(row))
    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("⚠️ spyid failed.")
        await send_error(ctx.guild, f"spyid error: {e}", tb=tb)


@bot.command()
async def spyhistory(ctx, *, kingdom: str):
    """!spyhistory <kingdom> -> shows last 5 saved reports for that kingdom."""
    try:
        real = await run_db(sync_fuzzy_kingdom, kingdom)
        real = real or kingdom
        rows = await run_db(sync_get_spy_history, real, 5)
        if not rows:
            return await ctx.send(f"❌ No saved reports for **{real}**.")

        lines = []
        for r in rows:
            dp = r.get("defense_power")
            castles = r.get("castles")
            ts = r.get("created_at")
            dp_txt = f"{int(dp):,}" if dp else "N/A"
            c_txt = str(int(castles or 0))
            lines.append(f"• ID `{r['id']}` • {ts} • DP `{dp_txt}` • Castles `{c_txt}`")

        await ctx.send(f"🧾 **Spy History • {real}**\n" + "\n".join(lines))

    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("⚠️ spyhistory failed.")
        await send_error(ctx.guild, f"spyhistory error: {e}", tb=tb)


@bot.command()
async def ap(ctx, *, kingdom: str):
    """!ap <kingdom> -> opens AP planner with buttons."""
    try:
        real = await run_db(sync_fuzzy_kingdom, kingdom)
        real = real or kingdom

        ok = await run_db(sync_ensure_ap_session, real)
        if not ok:
            return await ctx.send("❌ No DP spy report found for that kingdom.")

        row = await run_db(sync_get_ap_session_row, real)
        emb = build_ap_embed_from_row(real, row)
        if not emb:
            return await ctx.send("❌ No active session. Paste a DP spy report first.")
        await ctx.send(embed=emb, view=APView(real))

    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("⚠️ ap failed.")
        await send_error(ctx.guild, f"ap error: {e}", tb=tb)


@bot.command()
async def apstatus(ctx, *, kingdom: str):
    """!apstatus <kingdom> -> read-only AP planner status (no buttons)."""
    try:
        real = await run_db(sync_fuzzy_kingdom, kingdom)
        real = real or kingdom

        ok = await run_db(sync_ensure_ap_session, real)
        if not ok:
            return await ctx.send("❌ No DP spy report found for that kingdom.")

        row = await run_db(sync_get_ap_session_row, real)
        emb = build_ap_embed_from_row(real, row)
        if not emb:
            return await ctx.send("❌ No active session.")
        await ctx.send(embed=emb)

    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("⚠️ apstatus failed.")
        await send_error(ctx.guild, f"apstatus error: {e}", tb=tb)


@bot.command()
async def techindex(ctx):
    """!techindex -> scans ALL saved spy reports and saves battle-related tech into tech_index + kingdom_tech."""
    if not _is_admin(ctx):
        return await ctx.send("❌ Admin only.")
    try:
        await ctx.send("🔧 Running `!techindex` across ALL saved reports…")
        stats = await run_db(sync_techindex_all, None)
        await ctx.send(
            "✅ **Tech index complete**\n"
            f"Reports scanned: `{stats['reports_scanned']}`\n"
            f"Reports with tech: `{stats['reports_with_tech']}`\n"
            f"Tech lines indexed: `{stats['tech_history_rows']}`\n"
            f"Best-tech updates: `{stats['best_updates']}`"
        )
    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("⚠️ techindex failed.")
        await send_error(ctx.guild, f"techindex error: {e}", tb=tb)


@bot.command()
async def tech(ctx, *, kingdom: str):
    """!tech <kingdom> -> shows indexed battle-related tech for that kingdom (from player_tech view)."""
    try:
        real = await run_db(sync_fuzzy_kingdom, kingdom)
        real = real or kingdom

        rows = await run_db(sync_get_best_tech_for_kingdom, real, 60)
        if not rows:
            return await ctx.send(f"❌ No battle-tech indexed yet for **{real}**. Run `!techpull {real}` or `!techindex`.")

        lines = []
        for r in rows[:25]:
            lines.append(f"• **{r['tech_name']}** — lvl `{int(r['best_level'])}`")

        more = len(rows) - len(lines)
        if more > 0:
            lines.append(f"… +{more} more")

        await ctx.send(f"🧪 **Battle Tech • {real}**\n" + "\n".join(lines))

    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("⚠️ tech failed.")
        await send_error(ctx.guild, f"tech error: {e}", tb=tb)


@bot.command()
async def techtop(ctx):
    """!techtop -> shows the 15 most common indexed trainings across all kingdoms (from tech_index)."""
    try:
        rows = await run_db(sync_get_techtop_common, 15)
        if not rows:
            return await ctx.send("❌ No indexed tech found yet. Run `!techindex` first.")

        lines = []
        for i, r in enumerate(rows, start=1):
            lines.append(f"{i}. **{r['tech_name']}** — `{int(r['ct'])}` hits")

        await ctx.send("🏆 **Most Common Indexed Trainings (Top 15)**\n" + "\n".join(lines))

    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("⚠️ techtop failed.")
        await send_error(ctx.guild, f"techtop error: {e}", tb=tb)


@bot.command()
async def techpull(ctx, *, kingdom: str):
    """!techpull <kingdom> -> scans ALL saved reports for that kingdom and prints deduped best tech list."""
    try:
        real = await run_db(sync_fuzzy_kingdom, kingdom)
        real = real or kingdom

        await ctx.send(f"🔧 Rebuilding best battle-tech for **{real}** (scan ALL saved reports)…")
        stats = await run_db(sync_techpull_kingdom, real)
        rows = await run_db(sync_get_best_tech_for_kingdom, real, 200)

        if not rows:
            return await ctx.send(
                f"✅ Scan done for **{real}**, but no battle-tech matched the filter.\n"
                f"(Scanned `{stats['reports_scanned']}` reports)"
            )

        lines = []
        for r in rows[:30]:
            lines.append(f"• **{r['tech_name']}** — lvl `{int(r['best_level'])}`")

        more = len(rows) - len(lines)
        if more > 0:
            lines.append(f"… +{more} more")

        await ctx.send(
            f"✅ **Best Battle-Tech • {real}**\n"
            f"Reports scanned: `{stats['reports_scanned']}` • Tech lines indexed: `{stats['tech_history_rows']}`\n\n" +
            "\n".join(lines)
        )

    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("⚠️ techpull failed.")
        await send_error(ctx.guild, f"techpull error: {e}", tb=tb)


@bot.command()
async def backfill(ctx, days: int = None):
    """
    !backfill [days]
    Goes through DB reports and ensures research + troops are accounted for.
    - If days provided: only last N days
    - Else: whole DB
    """
    if not _is_admin(ctx):
        return await ctx.send("❌ Admin only.")

    try:
        if days and int(days) > 0:
            await ctx.send(f"🧱 Backfilling last **{int(days)}** days of reports (tech + troops)…")
        else:
            await ctx.send("🧱 Backfilling **ALL** reports (tech + troops)…")

        stats = await run_db(sync_backfill, int(days) if days else None)

        await ctx.send(
            "✅ **Backfill complete**\n"
            f"Reports scanned: `{stats['reports_scanned']}`\n"
            f"Tech reports: `{stats['tech_reports']}` • Tech lines indexed: `{stats['tech_history_rows']}` • Best updates: `{stats['best_updates']}`\n"
            f"Troop reports: `{stats['troop_reports']}` • Troop rows inserted: `{stats['troop_rows']}`"
        )

    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("⚠️ backfill failed.")
        await send_error(ctx.guild, f"backfill error: {e}", tb=tb)


@bot.command()
async def troops(ctx, *, kingdom: str):
    """!troops <kingdom> -> latest SR troop snapshot (home troops) for a kingdom."""
    try:
        real = await run_db(sync_fuzzy_kingdom, kingdom)
        real = real or kingdom

        report_id, captured_at, troops_map = await run_db(sync_get_latest_troop_snapshot_units, real)
        if not report_id:
            return await ctx.send(f"❌ No troop snapshots saved for **{real}** yet. Paste an SR first.")

        items = sorted(troops_map.items(), key=lambda x: x[1], reverse=True)
        lines = [f"• {name}: {cnt:,}" for name, cnt in items[:25]]
        more = len(items) - len(lines)
        if more > 0:
            lines.append(f"… +{more} more")

        await ctx.send(
            f"🏰 **Troops (Home) • {real}**\n"
            f"From SR `#{report_id}` • {captured_at}\n" +
            "\n".join(lines)
        )
    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("⚠️ troops failed.")
        await send_error(ctx.guild, f"troops error: {e}", tb=tb)


@bot.command(name="troopsdelta")
async def troopsdelta(ctx, *, kingdom: str):
    """!troopsdelta <kingdom> -> delta between last two SR troop snapshots."""
    try:
        real = await run_db(sync_fuzzy_kingdom, kingdom)
        real = real or kingdom

        pair = await run_db(sync_get_last_two_troop_snapshots, real)
        if not pair:
            return await ctx.send(f"❌ Need at least **2** SR troop snapshots for **{real}**. Paste two SRs first.")

        new = pair["new"]
        old = pair["old"]

        new_t = new["troops"]
        old_t = old["troops"]

        units = sorted(set(new_t.keys()) | set(old_t.keys()))
        deltas = []
        for u in units:
            a = int(old_t.get(u, 0))
            b = int(new_t.get(u, 0))
            d = b - a
            if d != 0:
                deltas.append((u, d))

        if not deltas:
            return await ctx.send(
                f"✅ **No troop count changes detected** for **{real}** between SR `#{old['report_id']}` and `#{new['report_id']}`."
            )

        losses = sorted([x for x in deltas if x[1] < 0], key=lambda x: x[1])  # most negative first
        gains = sorted([x for x in deltas if x[1] > 0], key=lambda x: x[1], reverse=True)

        lines = []
        if losses:
            lines.append("📉 **Estimated Losses (SR diff)**")
            for u, d in losses[:20]:
                lines.append(f"• {u}: {-d:,}")
        if gains:
            lines.append("\n📈 **Estimated Gains (trained/returned/etc.)**")
            for u, d in gains[:20]:
                lines.append(f"• {u}: {d:,}")

        await ctx.send(
            f"🧮 **Troops Delta • {real}**\n"
            f"Old SR `#{old['report_id']}` • {old['captured_at']}\n"
            f"New SR `#{new['report_id']}` • {new['captured_at']}\n\n" +
            "\n".join(lines)
        )

    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("⚠️ troopsdelta failed.")
        await send_error(ctx.guild, f"troopsdelta error: {e}", tb=tb)


# Back-compat alias
@bot.command(name="troopdelta")
async def troopdelta_alias(ctx, *, kingdom: str):
    return await troopsdelta(ctx, kingdom=kingdom)


@bot.command(name="refresh")
async def refresh(ctx):
    """Admin-only manual restart (Render will restart the service)."""
    try:
        if not _is_admin(ctx):
            return await ctx.send("❌ You don’t have permission to use this command.")

        try:
            await ctx.send("🔄 Refreshing bot now… (manual restart)")
        except Exception:
            pass

        if ctx.guild:
            try:
                ch = discord.utils.get(ctx.guild.text_channels, name=ERROR_CHANNEL_NAME)
                if ch and can_send(ch, ctx.guild):
                    await ch.send(f"🔄 Manual refresh requested by **{ctx.author.display_name}**")
            except Exception:
                pass

        await asyncio.sleep(1.0)
        os.execv(sys.executable, [sys.executable] + sys.argv)

    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("⚠️ Refresh failed.")
        if ctx.guild:
            await send_error(ctx.guild, f"refresh error: {e}", tb=tb)


# ---------- START ----------
if __name__ == "__main__":
    bot.run(TOKEN)
