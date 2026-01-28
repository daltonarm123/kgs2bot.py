# ---------- KG2 Recon Bot • FULL PATCHED BUILD (2026-01-28) ----------
# Spy auto-capture (Postgres) • Combat Calc • AP Planner w/ Buttons • Spy History/ID
# Tech capture/indexing (supports raw OR gz) • Per-kingdom deduped tech table + CSV export
# SR troop snapshots (home troops) • !troops • !troopdelta
#
# NEW THIS UPDATE:
# - FIX: Avoid blocking asyncio event loop (all DB work is offloaded to a thread)
# - ADD: psycopg2 connection pool (reduces connect churn, improves reliability)
# - ADD: Helpful DB indexes for "latest per kingdom" queries
# - FIX: Better error logging (tracebacks) + safe truncation for Discord error channel
# - FIX: Safer timestamp normalization (UTC)
# - MAINT: __main__ guard (safe imports/tests) + cleaner startup init
#
# Announcement behavior:
# - Only announces THIS VERSION’s PATCH_NOTES (no old history)
# - Dedupes announcements by version + cooldown

import os, re, asyncio, difflib, hashlib, logging, gzip, sys, time, traceback
from math import ceil
from datetime import datetime, timezone
from contextlib import contextmanager

import discord
from discord.ext import commands
from discord.ui import View, Button
from dotenv import load_dotenv

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import pool as pg_pool

# ------------------- PATCH INFO -------------------
BOT_VERSION = "2026-01-28.1"
PATCH_NOTES = [
    "Fixed: DB work no longer blocks the bot (offloaded to thread executor).",
    "Added: psycopg2 connection pooling (faster + fewer connection issues).",
    "Added: DB indexes for faster latest-report + troop snapshot queries.",
    "Improved: Error logs now include tracebacks (sanitized + truncated).",
    "Fixed: Message timestamps normalized safely to UTC.",
    "Maint: Added __main__ guard for safer imports/testing.",
]
# -------------------------------------------------

load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ERROR_CHANNEL_NAME = "kg2recon-updates"

logging.basicConfig(level=logging.INFO)

if not TOKEN:
    raise RuntimeError("Missing DISCORD_TOKEN env var.")
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL env var.")

KEEP_RAW_TEXT = os.getenv("KEEP_RAW_TEXT", "false").lower() in ("1", "true", "yes", "y")

# ---------- Constants ----------
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
DB_POOL: pg_pool.SimpleConnectionPool | None = None

def init_db_pool(minconn: int = 1, maxconn: int = 10):
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
    Pool-backed connection context manager.
    Always returns connection to pool.
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

        # History: tech lines per report
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

        # Deduped best tech per kingdom
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

        # Troop snapshots per SR (home troops)
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

        # Restart announcement dedupe
        cur.execute("""
        CREATE TABLE IF NOT EXISTS bot_meta (
            k TEXT PRIMARY KEY,
            v TEXT,
            updated_at TIMESTAMPTZ
        );
        """)

        # Minimal "self-heal" (keep it light; migrations later if you want)
        cur.execute("ALTER TABLE spy_reports ADD COLUMN IF NOT EXISTS report_hash TEXT;")
        cur.execute("ALTER TABLE spy_reports ADD COLUMN IF NOT EXISTS raw_gz BYTEA;")

        # Indexes (performance)
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS spy_reports_report_hash_uq ON spy_reports(report_hash);")
        cur.execute("""
            CREATE INDEX IF NOT EXISTS spy_reports_kingdom_created_at_idx
            ON spy_reports (kingdom, created_at DESC, id DESC);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS troop_snapshots_kingdom_captured_at_idx
            ON troop_snapshots (kingdom, captured_at DESC, report_id DESC);
        """)

def heal_sequences():
    with db_conn() as conn, conn.cursor() as cur:
        for table in ["spy_reports", "dp_sessions", "tech_index", "troop_snapshots"]:
            cur.execute(
                f"SELECT setval(pg_get_serial_sequence('{table}','id'), "
                f"COALESCE((SELECT MAX(id) FROM {table}), 1), true);"
            )

# ---------- Async DB offload ----------
async def run_db(fn, *args, **kwargs):
    """
    Offload synchronous DB work to a thread so we do not block the asyncio loop.
    """
    return await asyncio.to_thread(fn, *args, **kwargs)

# ---------- Helpers ----------
def now_utc():
    return datetime.now(timezone.utc)

def normalize_to_utc(ts: datetime | None) -> datetime:
    ts = ts or now_utc()
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)

def castle_bonus(c: int) -> float:
    return (c ** 0.5) / 100 if c else 0.0

def hash_report(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

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

        # Stop conditions
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
    techs = []
    in_tech = False

    blocked_prefixes = (
        "heavy cavalry", "light cavalry", "archers", "pikemen", "peasants", "knights",
        "spies sent", "spies lost", "population", "elites",
        "horses", "blue gems", "green gems", "gold", "food", "wood", "stone", "land",
        "networth", "honour", "ranking", "number of castles", "approximate defensive power",
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
    """
    Sends a safe, truncated error message to the error channel and logs full traceback to console.
    """
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

# ---------- Sync DB Functions (run via run_db) ----------
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

def sync_get_latest_spy_report_for_kingdom(kingdom: str):
    if not kingdom:
        return None
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, kingdom, defense_power, castles, created_at, raw, raw_gz
            FROM spy_reports
            WHERE kingdom=%s AND defense_power IS NOT NULL AND defense_power > 0
            ORDER BY created_at DESC NULLS LAST, id DESC
            LIMIT 1;
        """, (kingdom,))
        return cur.fetchone()

def sync_get_latest_spy_report_any():
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, kingdom, defense_power, castles, created_at, raw, raw_gz
            FROM spy_reports
            WHERE defense_power IS NOT NULL AND defense_power > 0
            ORDER BY created_at DESC NULLS LAST, id DESC
            LIMIT 1;
        """)
        return cur.fetchone()

def sync_get_latest_report_for_kingdom_any(kingdom: str):
    if not kingdom:
        return None
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, kingdom, defense_power, castles, created_at, raw, raw_gz
            FROM spy_reports
            WHERE kingdom=%s
            ORDER BY created_at DESC NULLS LAST, id DESC
            LIMIT 1;
        """, (kingdom,))
        return cur.fetchone()

def sync_get_recent_reports_for_kingdom(kingdom: str, limit: int = 25):
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, kingdom, defense_power, castles, created_at, raw, raw_gz
            FROM spy_reports
            WHERE kingdom=%s
            ORDER BY created_at DESC NULLS LAST, id DESC
            LIMIT %s;
        """, (kingdom, limit))
        return cur.fetchall()

def sync_rebuild_ap_session(kingdom: str) -> bool:
    spy = sync_get_latest_spy_report_for_kingdom(kingdom)
    if not spy:
        return False
    base_dp = int(spy["defense_power"] or 0)
    castles = int(spy["castles"] or 0)
    if base_dp <= 0:
        return False

    captured_at = spy["created_at"] or now_utc()
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM dp_sessions WHERE kingdom=%s;", (kingdom,))
        cur.execute("""
            INSERT INTO dp_sessions (kingdom, base_dp, castles, current_dp, hits, last_hit, captured_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s);
        """, (kingdom, base_dp, castles, base_dp, 0, None, captured_at))
    return True

def sync_ensure_ap_session(kingdom: str) -> bool:
    if not kingdom:
        return False
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, base_dp, current_dp
            FROM dp_sessions
            WHERE kingdom=%s
            ORDER BY captured_at DESC NULLS LAST, id DESC
            LIMIT 1;
        """, (kingdom,))
        sess = cur.fetchone()

    if sess:
        if int(sess.get("base_dp") or 0) <= 0:
            return sync_rebuild_ap_session(kingdom)
        return True

    return sync_rebuild_ap_session(kingdom)

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

# Tech dedupe (atomic-ish upsert)
def sync_upsert_kingdom_tech(cur, kingdom: str, tech_name: str, level: int, report_id: int, captured_at):
    cur.execute("""
        INSERT INTO kingdom_tech (kingdom, tech_name, best_level, updated_at, source_report_id)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (kingdom, tech_name)
        DO UPDATE SET
          best_level = GREATEST(kingdom_tech.best_level, EXCLUDED.best_level),
          updated_at = EXCLUDED.updated_at,
          source_report_id =
            CASE
              WHEN EXCLUDED.best_level > kingdom_tech.best_level THEN EXCLUDED.source_report_id
              ELSE kingdom_tech.source_report_id
            END;
    """, (kingdom, tech_name, level, captured_at or now_utc(), report_id))

def sync_index_tech_from_parsed(cur, kingdom: str, report_id: int, captured_at, techs):
    if not kingdom or not report_id or not techs:
        return (0, 0)

    history_count = 0
    dedupe_updates = 0
    captured_at = captured_at or now_utc()

    for name, lvl in techs:
        cur.execute("""
            INSERT INTO tech_index (kingdom, tech_name, tech_level, captured_at, report_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, (kingdom, name, lvl, captured_at, report_id))
        history_count += 1

        # For dedupe, just do the atomic upsert (no pre-select needed)
        sync_upsert_kingdom_tech(cur, kingdom, name, lvl, report_id, captured_at)
        dedupe_updates += 1

    return (history_count, dedupe_updates)

def sync_upsert_troop_snapshot(cur, kingdom: str, report_id: int, captured_at, troops: dict):
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
            "new": {
                "report_id": int(newest["report_id"]),
                "captured_at": newest["captured_at"],
                "troops": load(int(newest["report_id"]))
            },
            "old": {
                "report_id": int(prev["report_id"]),
                "captured_at": prev["captured_at"],
                "troops": load(int(prev["report_id"]))
            },
        }

def sync_store_report(msg_content: str, created_at_utc: datetime):
    """
    Stores a spy report row (deduped by report_hash), indexes tech, stores troop snapshot.
    Returns:
      {
        "saved": bool,
        "duplicate": bool,
        "row": dict|None,
        "kingdom": str|None,
        "dp": int|None,
        "castles": int,
      }
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
        return {"saved": False, "duplicate": False, "row": None, "kingdom": kingdom, "dp": dp, "castles": castles}

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
                sync_index_tech_from_parsed(cur, kingdom, row["id"], row.get("created_at") or created_at_utc, techs)

            if sr_troops:
                sync_upsert_troop_snapshot(cur, kingdom, row["id"], row.get("created_at") or created_at_utc, sr_troops)

            if dp is not None and dp >= 1000:
                sync_ensure_ap_session(kingdom)

            return {"saved": True, "duplicate": False, "row": row, "kingdom": kingdom, "dp": dp, "castles": castles}

        # Duplicate: "repair mode" index tech/troops against existing report_id
        rep_id = int(exists["id"])
        if techs:
            sync_index_tech_from_parsed(cur, kingdom, rep_id, created_at_utc, techs)
        if sr_troops:
            sync_upsert_troop_snapshot(cur, kingdom, rep_id, created_at_utc, sr_troops)

        return {"saved": True, "duplicate": True, "row": None, "kingdom": kingdom, "dp": dp, "castles": castles}

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
            return {"ok": False, "reason": "no_leave"}

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

# ---------- Embeds (no DB inside these) ----------
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

    for label, red in AP_REDUCTIONS:
        rem = ceil(adj * (1 - red))
        embed.add_field(
            name=f"{label} (-{int(red*100)}%)",
            value=f"DP: {rem:,}\nHC: {ceil(rem/HEAVY_CAVALRY_AP):,}",
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
                    return await interaction.followup.send("❌ No active session. Paste a spy report first, then run `!ap` again.")

                row = await run_db(sync_get_ap_session_row, self.kingdom)
                embed = build_ap_embed_from_row(self.kingdom, row)
                if embed:
                    try:
                        await interaction.message.edit(embed=embed, view=self)
                    except Exception:
                        await interaction.followup.send(embed=embed, view=self)

            except Exception as e:
                tb = traceback.format_exc()
                await interaction.followup.send("⚠️ Failed to apply hit.")
                if interaction.guild:
                    logging.exception("AP hit button error")
                    await send_error(interaction.guild, f"AP hit button error: {e}", tb=tb)

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
                await interaction.followup.send("⚠️ Failed to reset session.")
                if interaction.guild:
                    logging.exception("AP reset error")
                    await send_error(interaction.guild, f"AP reset error: {e}", tb=tb)

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
                await interaction.followup.send("⚠️ Failed to rebuild session.")
                if interaction.guild:
                    logging.exception("AP rebuild error")
                    await send_error(interaction.guild, f"AP rebuild error: {e}", tb=tb)

        btn = Button(label="Rebuild", style=discord.ButtonStyle.primary)
        btn.callback = callback
        return btn

# ---------- Bot Events ----------
@bot.event
async def on_ready():
    global ANNOUNCED_READY_THIS_PROCESS

    try:
        # Init pool + schema in a thread (safe)
        await run_db(init_db_pool, 1, 10)
        await run_db(init_db)
        await run_db(heal_sequences)
    except Exception as e:
        logging.exception("DB init failed")
        # can't send to discord reliably yet, just log
        logging.error(f"DB init failed: {e}")

    if ANNOUNCED_READY_THIS_PROCESS:
        logging.info("on_ready called again (same process) - announcement suppressed.")
        return
    ANNOUNCED_READY_THIS_PROCESS = True

    # version/cooldown dedupe (DB-backed)
    try:
        def _dedupe_write():
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

        ok_to_announce = await run_db(_dedupe_write)
        if not ok_to_announce:
            logging.info("Announcement suppressed (same version + cooldown).")
            return

    except Exception as e:
        logging.exception("Announcement dedupe DB write failed")
        logging.error(f"Announcement dedupe DB write failed: {e}")

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
        logging.exception("on_message spy capture error")
        await send_error(msg.guild, f"on_message spy capture error: {e}", tb=tb)

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
            row = await run_db(sync_get_latest_spy_report_any)
            if not row:
                return await ctx.send("❌ No saved DP spy reports in DB yet.")
            dp = int(row["defense_power"])
            c = int(row["castles"] or 0)
            target = row["kingdom"] or "Unknown"
            used = f"(from DB: {row['id']})"
            return await ctx.send(embed=build_calc_embed(target, dp, c, used))

        if arg:
            real = await run_db(sync_fuzzy_kingdom, arg)
            real = real or arg
            row = await run_db(sync_get_latest_spy_report_for_kingdom, real)
            if not row:
                return await ctx.send(f"❌ No saved DP reports for **{real}**. Paste a full spy report and try again.")
            dp = int(row["defense_power"])
            c = int(row["castles"] or 0)
            target = row["kingdom"] or real
            used = f"(from DB: {row['id']})"
            return await ctx.send(embed=build_calc_embed(target, dp, c, used))

        await ctx.send("📄 Paste the spy report you want to calculate against (you have 90 seconds).")
        try:
            msg = await bot.wait_for(
                "message",
                timeout=90,
                check=lambda m: m.author == ctx.author and m.channel == ctx.channel
            )
        except asyncio.TimeoutError:
            return await ctx.send("⏰ Timed out. Run `!calc` again.")

        k, dp, c = parse_spy(msg.content)
        if not k or not dp:
            return await ctx.send("❌ Could not parse that spy report. Make sure it includes Target + Defensive Power.")
        await ctx.send(embed=build_calc_embed(k, int(dp), int(c or 0), "(pasted)"))

    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("⚠️ calc failed.")
        await send_error(ctx.guild, f"calc error: {e}", tb=tb)

@bot.command()
async def ap(ctx, *, kingdom: str):
    try:
        real = await run_db(sync_fuzzy_kingdom, kingdom)
        real = real or kingdom

        ok = await run_db(sync_ensure_ap_session, real)
        if not ok:
            return await ctx.send("❌ No DP spy report found for that kingdom.")

        row = await run_db(sync_get_ap_session_row, real)
        emb = build_ap_embed_from_row(real, row)
        if not emb:
            return await ctx.send("❌ No active session embed. Try `!apfix <kingdom>`.")

        await ctx.send(embed=emb, view=APView(real))

    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("⚠️ ap failed.")
        await send_error(ctx.guild, f"ap error: {e}", tb=tb)

@bot.command()
async def apfix(ctx, *, kingdom: str):
    try:
        real = await run_db(sync_fuzzy_kingdom, kingdom)
        real = real or kingdom

        ok = await run_db(sync_rebuild_ap_session, real)
        if not ok:
            return await ctx.send("❌ No valid DP spy report found.")

        await ctx.send(f"✅ Rebuilt AP session for **{real}**.")
        row = await run_db(sync_get_ap_session_row, real)
        emb = build_ap_embed_from_row(real, row)
        if emb:
            await ctx.send(embed=emb, view=APView(real))

    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("⚠️ apfix failed.")
        await send_error(ctx.guild, f"apfix error: {e}", tb=tb)

@bot.command()
async def troops(ctx, *, kingdom: str):
    """Show latest SR troop snapshot (home troops) for a kingdom."""
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

@bot.command()
async def troopdelta(ctx, *, kingdom: str):
    """Estimate troop losses/gains between the last two SR snapshots for a kingdom."""
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

        losses = sorted([x for x in deltas if x[1] < 0], key=lambda x: x[1])
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
            f"🧮 **Troop Delta • {real}**\n"
            f"Old SR `#{old['report_id']}` • {old['captured_at']}\n"
            f"New SR `#{new['report_id']}` • {new['captured_at']}\n\n" +
            "\n".join(lines)
        )

    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("⚠️ troopdelta failed.")
        await send_error(ctx.guild, f"troopdelta error: {e}", tb=tb)

def _is_admin(ctx: commands.Context) -> bool:
    try:
        if ctx.guild and ctx.author and getattr(ctx.author, "guild_permissions", None):
            return bool(ctx.author.guild_permissions.administrator)
    except Exception:
        pass
    return False

@bot.command()
async def techreset(ctx, *, kingdom: str = None):
    """
    Admin-only: clears deduped research list (kingdom_tech) so you can rebuild cleanly.
      - !techreset         -> clears ALL kingdoms
      - !techreset beef    -> clears just Beef
    """
    if not _is_admin(ctx):
        return await ctx.send("❌ Admin only.")

    try:
        def _do_delete(real_kingdom: str | None):
            with db_conn() as conn, conn.cursor() as cur:
                if real_kingdom:
                    cur.execute("DELETE FROM kingdom_tech WHERE kingdom=%s;", (real_kingdom,))
                else:
                    cur.execute("DELETE FROM kingdom_tech;")

        if kingdom:
            real = await run_db(sync_fuzzy_kingdom, kingdom)
            real = real or kingdom
            await run_db(_do_delete, real)
            await ctx.send(f"✅ Cleared research list for **{real}**.")
        else:
            await run_db(_do_delete, None)
            await ctx.send("✅ Cleared research list for **ALL kingdoms**.")

    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("⚠️ techreset failed.")
        await send_error(ctx.guild, f"techreset error: {e}", tb=tb)

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

# ---------- START BOT ----------
if __name__ == "__main__":
    bot.run(TOKEN)
