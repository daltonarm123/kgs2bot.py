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
# !track [date]         -> daily attack-loss tracker + TSV export
#
# !ap <kingdom>       -> AP Planner with buttons (Minor/Victory/Major/Overwhelming + Reset + Rebuild)
# !apstatus <kingdom> -> read-only AP status (no buttons)
#
# !techindex              -> scans ALL saved spy reports and fills tech_index + kingdom_tech
# !tech <kingdom>         -> shows deduped battle-related tech for that kingdom (from player_tech view)
# !techtop                -> shows 15 most common indexed trainings across all kingdoms (from tech_index)
# !techpull <kingdom>     -> rescans ALL reports for that kingdom and rebuilds deduped best tech list
# !techcsv                -> exports all indexed kingdom research to CSV (upload in Discord)
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
import json
import time
import gzip
import sys
import asyncio
import difflib
import hashlib
import logging
import traceback
import urllib.request
import urllib.error
import urllib.parse
import base64
from math import ceil
from datetime import datetime, timezone, timedelta
from contextlib import contextmanager
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

import discord
from discord.ext import commands
from discord.ui import View, Button
from dotenv import load_dotenv

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import pool as pg_pool


# ------------------- PATCH INFO -------------------
BOT_VERSION = "2026-07-05.4"
PATCH_NOTES = [
    "Rankings tracking now records pie-status changes silently during background refreshes while leaving NW alert posting unchanged.",
    "Added live rankings history so the bot can compare kingdom changes over a lookback window instead of only showing the latest poll.",
    "Added !kingdomlive / !intel for a live kingdom profile with rank, NW, pie, recent attacks, latest SR, and tracked-home context.",
    "NW jump diagnostics now show current state rows, rankings history rows, and live pie detections from the current pull.",
    "Rankings tracking now always keeps at least the top 100 kingdoms in scope.",
    "Added !rankingsrefresh so admins can force an immediate live top-100 rankings update between automatic poll cycles.",
]
# -------------------------------------------------


# ---------- Env ----------
def _clean_env_value(v) -> str:
    s = str(v if v is not None else "").strip()
    # Railway/UI copy-paste often stores quoted values like '"1"'.
    if len(s) >= 2 and ((s[0] == '"' and s[-1] == '"') or (s[0] == "'" and s[-1] == "'")):
        s = s[1:-1].strip()
    return s


def _env_text(name: str, default: str = "") -> str:
    raw = os.getenv(name, default)
    return _clean_env_value(raw)


def _env_int(name: str, default: int) -> int:
    txt = _env_text(name, str(default))
    try:
        return int(txt or str(default))
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    txt = _env_text(name, str(default))
    try:
        return float(txt or str(default))
    except Exception:
        return float(default)


def _env_bool(name: str, default: bool = False) -> bool:
    txt = _env_text(name, "true" if default else "false").lower()
    return txt in ("1", "true", "yes", "y", "on")


def _env_csv_ints(name: str) -> set[int]:
    txt = _env_text(name, "")
    out = set()
    for part in txt.split(","):
        p = _clean_env_value(part).strip()
        if not p:
            continue
        try:
            out.add(int(p))
        except Exception:
            continue
    return out


load_dotenv()
TOKEN = _env_text("DISCORD_TOKEN", "")
DATABASE_URL = _env_text("DATABASE_URL", "")
DATABASE_PUBLIC_URL = _env_text("DATABASE_PUBLIC_URL", "")
DB_SSLMODE = _env_text("DB_SSLMODE", "prefer").lower() or "prefer"
ERROR_CHANNEL_NAME = _env_text("ERROR_CHANNEL_NAME", "kg2recon-updates")
TARGET_GUILD_ID = _env_int("TARGET_GUILD_ID", 1405247393112395866)
UPDATES_CHANNEL_ID = _env_int("UPDATES_CHANNEL_ID", 0)
UPDATES_CHANNEL_NAME = _env_text("UPDATES_CHANNEL_NAME", ERROR_CHANNEL_NAME) or ERROR_CHANNEL_NAME
LIVE_BATTLE_CHANNEL_ID = _env_int("LIVE_BATTLE_CHANNEL_ID", 1463579633449697334)
KEEP_RAW_TEXT = _env_bool("KEEP_RAW_TEXT", False)
RECON_INGEST_URL = _env_text("RECON_INGEST_URL", "https://recon-hub.onrender.com/api/reports/spy")
RECON_INGEST_ENABLED = _env_bool("RECON_INGEST_ENABLED", True)
RECON_INGEST_TIMEOUT = _env_float("RECON_INGEST_TIMEOUT", 10.0)
BACKFILL_FORWARD_ENABLED = _env_bool("BACKFILL_FORWARD_ENABLED", False)
RECON_CALC_BASE_URL = _env_text("RECON_CALC_BASE_URL", "https://recon-hub.onrender.com/kg-calc.html")
KG_GAME_API_BASE = "https://kingdomgame.net"
if str(_env_text("KG_GAME_API_BASE", "")).strip().rstrip("/") not in ("", KG_GAME_API_BASE):
    logging.warning("Ignoring KG_GAME_API_BASE override; bot is locked to https://kingdomgame.net")
KG_GAME_API_TIMEOUT = _env_float("KG_GAME_API_TIMEOUT", 4.0)
KG_GAME_API_CACHE_SECONDS = _env_int("KG_GAME_API_CACHE_SECONDS", 60)
KG_GAME_WORLD_ID = _env_int("KG_GAME_WORLD_ID", 1)
KG_GAME_ACCOUNT_ID = _env_text("KG_GAME_ACCOUNT_ID", "")
KG_GAME_TOKEN = _env_text("KG_GAME_TOKEN", "")
KG_GAME_TOKEN_KINGDOM_ID = _env_text("KG_GAME_TOKEN_KINGDOM_ID", "")
KG_GAME_EMAIL = _env_text("KG_GAME_EMAIL", "")
KG_GAME_PASSWORD = _env_text("KG_GAME_PASSWORD", "")
KG_GAME_WEB_COOKIE = _env_text("KG_GAME_WEB_COOKIE", "")
KG_GAME_AUTH_CACHE_SECONDS = _env_int("KG_GAME_AUTH_CACHE_SECONDS", 1800)
KG_GAME_SEARCH_KINGDOM_ID = _env_int("KG_GAME_SEARCH_KINGDOM_ID", 0)
KG_GAME_RANKINGS_CONTINENT_ID = _env_int("KG_GAME_RANKINGS_CONTINENT_ID", -1)
KG_GAME_RANKINGS_PAGE_SIZE = max(1, _env_int("KG_GAME_RANKINGS_PAGE_SIZE", 20))
KG_GAME_RANKINGS_TARGET_ROWS = max(100, _env_int("KG_GAME_RANKINGS_TARGET_ROWS", 100))
KG_REPORT_DEFAULT_TZ = _env_text("KG_REPORT_DEFAULT_TZ", "UTC")
KG_REPORT_MAX_FUTURE_MINUTES = _env_int("KG_REPORT_MAX_FUTURE_MINUTES", 10)
KG_REPORT_AUTO_INFER_TZ = _env_bool("KG_REPORT_AUTO_INFER_TZ", True)
KG_REPORT_INFER_IF_DELTA_MINUTES = _env_int("KG_REPORT_INFER_IF_DELTA_MINUTES", 180)
KG_REPORT_INFER_MAX_PAST_HOURS = _env_int("KG_REPORT_INFER_MAX_PAST_HOURS", 8)
ADMIN_USER_IDS = {944024167081209867}
ADMIN_USER_IDS.update(_env_csv_ints("ADMIN_USER_IDS"))
PREMIUM_FREE_USER_IDS = {944024167081209867}
PREMIUM_FREE_USER_IDS.update(_env_csv_ints("PREMIUM_FREE_USER_IDS"))

if not TOKEN:
    raise RuntimeError("Missing DISCORD_TOKEN env var.")
if not (DATABASE_URL or DATABASE_PUBLIC_URL):
    raise RuntimeError("Missing DATABASE_URL or DATABASE_PUBLIC_URL env var.")

logging.basicConfig(level=logging.INFO)


# ---------- Game constants ----------
HEAVY_CAVALRY_AP = 7
# KG live behavior: Footmen are treated as 2 AP.
FOOTMEN_AP = 2
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
bot.remove_command("help")

db_init_lock = asyncio.Lock()

# ---------- Locks ----------
ap_lock = asyncio.Lock()


# ---------- Announcement anti-spam ----------
ANNOUNCED_READY_THIS_PROCESS = False
ANNOUNCE_COOLDOWN_SECONDS = 15 * 60  # 15 minutes
MAX_HISTORY_SCAN_MESSAGES_PER_CHANNEL = _env_int("MAX_HISTORY_SCAN_MESSAGES_PER_CHANNEL", 0)
BACKFILL_CHANNEL_CONCURRENCY = _env_int("BACKFILL_CHANNEL_CONCURRENCY", 4)
INGEST_PROGRESS_EVERY_MESSAGES = _env_int("INGEST_PROGRESS_EVERY_MESSAGES", 2000)
INGEST_PREFILTER_ENABLED = _env_bool("INGEST_PREFILTER_ENABLED", True)
KG_BASE_RETURN_MINUTES = _env_float("KG_BASE_RETURN_MINUTES", 20.0)
KG_SEASON_EPOCH_UTC = _env_text("KG_SEASON_EPOCH_UTC", "2026-01-01T00:00:00Z")
KG_HIT_UP_RETURN_MULT = _env_float("KG_HIT_UP_RETURN_MULT", 0.90)
KG_HIT_DOWN_RETURN_MULT = _env_float("KG_HIT_DOWN_RETURN_MULT", 1.10)
KG_RETURN_MODEL_ENABLED = _env_bool("KG_RETURN_MODEL_ENABLED", True)
KG_TRACK_ATTACK_REPORT_MOVEMENTS = _env_bool("KG_TRACK_ATTACK_REPORT_MOVEMENTS", False)
KG_TRACK_INCOMING_ALERT_MOVEMENTS = _env_bool("KG_TRACK_INCOMING_ALERT_MOVEMENTS", True)
KG_TROOP_TRACKING_ENABLED = KG_TRACK_ATTACK_REPORT_MOVEMENTS or KG_TRACK_INCOMING_ALERT_MOVEMENTS
KG_RETURN_LINEAR_SLOPE = _env_float("KG_RETURN_LINEAR_SLOPE", -185.6)
KG_RETURN_LINEAR_INTERCEPT = _env_float("KG_RETURN_LINEAR_INTERCEPT", 290.4)
KG_RETURN_LINEAR_X_MIN = _env_float("KG_RETURN_LINEAR_X_MIN", 0.0)
KG_RETURN_LINEAR_X_MAX = _env_float("KG_RETURN_LINEAR_X_MAX", 4.5)
KG_RETURN_MIN_MINUTES = _env_float("KG_RETURN_MIN_MINUTES", 18.85)
KG_RETURN_MAX_MINUTES = _env_float("KG_RETURN_MAX_MINUTES", 288.0)
KG_GEM_SPEEDUP_PCT = _env_float("KG_GEM_SPEEDUP_PCT", 0.0)
KG_ROUND_TO_TICK = _env_bool("KG_ROUND_TO_TICK", False)
KG_TICK_MINUTES = _env_int("KG_TICK_MINUTES", 5)
KG_TICK_ROUND_MODE = _env_text("KG_TICK_ROUND_MODE", "floor").lower()
RETURN_ALERT_POLL_SECONDS = _env_int("RETURN_ALERT_POLL_SECONDS", 30)
NW_JUMP_ALERTS_ENABLED = _env_bool("NW_JUMP_ALERTS_ENABLED", True)
NW_JUMP_ALERT_POLL_SECONDS = _env_int("NW_JUMP_ALERT_POLL_SECONDS", 60)
NW_JUMP_ALERT_DEFAULT_THRESHOLD = _env_int("NW_JUMP_ALERT_DEFAULT_THRESHOLD", 5000)
NW_JUMP_ALERT_SILENT_NO_SUBS = _env_bool("NW_JUMP_ALERT_SILENT_NO_SUBS", True)
KG_GAME_PIE_ALERTS_ENABLED = _env_bool("KG_GAME_PIE_ALERTS_ENABLED", True)
KINGDOM_LIVE_DEFAULT_LOOKBACK_HOURS = _env_int("KINGDOM_LIVE_DEFAULT_LOOKBACK_HOURS", 1)
KINGDOM_LIVE_ATTACK_WINDOW_HOURS = _env_int("KINGDOM_LIVE_ATTACK_WINDOW_HOURS", 24)
OVEN_ESTIMATOR_ENABLED = _env_bool("OVEN_ESTIMATOR_ENABLED", True)
OVEN_LOOKBACK_HOURS = _env_int("OVEN_LOOKBACK_HOURS", 36)
OVEN_MAX_ALERT_LINES = _env_int("OVEN_MAX_ALERT_LINES", 3)
OVEN_MAX_RESULTS = _env_int("OVEN_MAX_RESULTS", 6)
OVEN_NW_TOLERANCE_PCT = _env_float("OVEN_NW_TOLERANCE_PCT", 12.0)
OVEN_PEASANT_TOLERANCE_PCT = _env_float("OVEN_PEASANT_TOLERANCE_PCT", 3.0)
ALERT_SMS_ENABLED = _env_bool("ALERT_SMS_ENABLED", False)
ALERT_SMS_TWILIO_ACCOUNT_SID = _env_text("ALERT_SMS_TWILIO_ACCOUNT_SID", "")
ALERT_SMS_TWILIO_AUTH_TOKEN = _env_text("ALERT_SMS_TWILIO_AUTH_TOKEN", "")
ALERT_SMS_TWILIO_API_KEY_SID = _env_text("ALERT_SMS_TWILIO_API_KEY_SID", "")
ALERT_SMS_TWILIO_API_KEY_SECRET = _env_text("ALERT_SMS_TWILIO_API_KEY_SECRET", "")
ALERT_SMS_TWILIO_FROM = _env_text("ALERT_SMS_TWILIO_FROM", "")
ALERT_SMS_TO = _env_text("ALERT_SMS_TO", "")
ALERT_SMS_WATCHLIST = _env_text("ALERT_SMS_WATCHLIST", "")
ALERT_SMS_MAX_PER_ALERT = _env_int("ALERT_SMS_MAX_PER_ALERT", 10)
PREMIUM_GATE_ENABLED = _env_bool("PREMIUM_GATE_ENABLED", True)
BATTLE_RETURNS_LOOP_STARTED = False
NW_JUMP_ALERTS_LOOP_STARTED = False


def parse_utc_dt(s: str) -> datetime:
    txt = str(s or "").strip().replace("Z", "+00:00")
    try:
        d = datetime.fromisoformat(txt)
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d.astimezone(timezone.utc)
    except Exception:
        return now_utc()


SEASON_EPOCH = parse_utc_dt(KG_SEASON_EPOCH_UTC)
SEASON_ORDER = ("spring", "summer", "autumn", "winter")
SEASON_MULT = {
    "spring": 1.00,
    "summer": 0.75,  # return times decreased 25%
    "autumn": 0.90,  # return times decreased 10%
    "winter": 1.25,  # return times increased 25%
}
SEASON_LEN = timedelta(days=5)


# ---------- DB Pool ----------
DB_READY = False
DB_POOL = None  # psycopg2.pool.SimpleConnectionPool
NW_API_CACHE: dict[str, tuple[int | None, float]] = {}
KG_API_AUTH_CACHE: dict[str, object] = {}
BACKFILL_PROGRESS: dict[str, dict] = {}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def normalize_to_utc(ts: datetime | None) -> datetime:
    ts = ts or now_utc()
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def season_index_and_start(ts: datetime) -> tuple[int, datetime]:
    t = normalize_to_utc(ts)
    if t < SEASON_EPOCH:
        return 0, SEASON_EPOCH
    delta = t - SEASON_EPOCH
    slot = int(delta.total_seconds() // SEASON_LEN.total_seconds())
    idx = slot % 4
    start = SEASON_EPOCH + (slot * SEASON_LEN)
    return idx, start


def season_name_at(ts: datetime) -> str:
    idx, _ = season_index_and_start(ts)
    return SEASON_ORDER[idx]


def season_end_at(ts: datetime) -> datetime:
    _, start = season_index_and_start(ts)
    return start + SEASON_LEN


def estimate_return_time_season_aware(departed_at: datetime, base_minutes: float | None = None) -> datetime:
    """
    Convert base return minutes into actual return timestamp with seasonal speed modifiers.
    Handles season boundary crossings by integrating segment-by-segment.
    """
    remaining_base = float(base_minutes if base_minutes is not None else KG_BASE_RETURN_MINUTES)
    t = normalize_to_utc(departed_at)
    if remaining_base <= 0:
        return t

    guard = 0
    while remaining_base > 0 and guard < 64:
        guard += 1
        s_name = season_name_at(t)
        mult = float(SEASON_MULT.get(s_name, 1.0))
        mult = 1.0 if mult <= 0 else mult
        seg_end = season_end_at(t)
        seg_real_minutes = max(0.0, (seg_end - t).total_seconds() / 60.0)
        seg_base_capacity = seg_real_minutes / mult
        if seg_base_capacity >= remaining_base:
            return t + timedelta(minutes=(remaining_base * mult))
        remaining_base -= seg_base_capacity
        t = seg_end
    return t


def apply_hit_direction_return_modifier(base_minutes: float, hit_direction: str | None) -> float:
    """
    Applies hit-up/hit-down return-time multiplier before season integration.
    - up: faster returns (default 0.90x)
    - down: slower returns (default 1.10x)
    """
    b = float(base_minutes or 0.0)
    if b <= 0:
        return b
    d = str(hit_direction or "").strip().lower()
    if d == "up":
        return b * max(0.01, KG_HIT_UP_RETURN_MULT)
    if d == "down":
        return b * max(0.01, KG_HIT_DOWN_RETURN_MULT)
    return b


def _compute_piecewise_base_minutes_from_nw(attacker_nw: int | None, defender_nw: int | None) -> float | None:
    """
    Piecewise NW-ratio model:
      x = defender_nw / attacker_nw
      x <= X_MIN => MAX minutes (ceiling)
      X_MIN < x < X_MAX => linear
      x >= X_MAX => MIN minutes (floor)
    """
    try:
        a = float(attacker_nw or 0)
        d = float(defender_nw or 0)
        if a <= 0 or d <= 0:
            return None
        x = d / a
        if x <= KG_RETURN_LINEAR_X_MIN:
            return float(KG_RETURN_MAX_MINUTES)
        if x >= KG_RETURN_LINEAR_X_MAX:
            return float(KG_RETURN_MIN_MINUTES)
        y = (KG_RETURN_LINEAR_SLOPE * x) + KG_RETURN_LINEAR_INTERCEPT
        return float(max(KG_RETURN_MIN_MINUTES, min(KG_RETURN_MAX_MINUTES, y)))
    except Exception:
        return None


def _apply_gem_speedup(minutes: float, pct: float | None = None) -> float:
    p = float(KG_GEM_SPEEDUP_PCT if pct is None else pct)
    p = max(0.0, min(100.0, p))
    return max(0.01, float(minutes) * (1.0 - (p / 100.0)))


def compute_base_return_minutes_cur(
    cur,
    attacker: str | None,
    defender: str | None,
    departed_at: datetime,
    attacker_nw_hint: int | None = None,
    defender_nw_hint: int | None = None,
) -> float:
    """
    Compute baseline return minutes before seasonal integration.
    Priority:
    1) NW-ratio model (if enabled and NWs available)
    2) hit-direction adjusted legacy base
    3) gem speed adjustment always applied at the end
    """
    hit_dir = infer_hit_direction_from_nw_cur(cur, attacker, defender, departed_at)
    base_minutes = None
    if KG_RETURN_MODEL_ENABLED:
        a_nw = _safe_int_or_none(attacker_nw_hint) or sync_get_latest_networth_for_kingdom_before_cur(cur, attacker, departed_at)
        d_nw = _safe_int_or_none(defender_nw_hint) or sync_get_latest_networth_for_kingdom_before_cur(cur, defender, departed_at)
        base_minutes = _compute_piecewise_base_minutes_from_nw(a_nw, d_nw)
    if base_minutes is None:
        base_minutes = apply_hit_direction_return_modifier(KG_BASE_RETURN_MINUTES, hit_dir)
    return _apply_gem_speedup(base_minutes)


def _round_ts_to_tick(ts: datetime, tick_minutes: int | None = None, mode: str | None = None) -> datetime:
    t = normalize_to_utc(ts)
    tick = max(1, int(tick_minutes or KG_TICK_MINUTES or 5))
    m = str(mode or KG_TICK_ROUND_MODE or "floor").strip().lower()
    tick_seconds = tick * 60
    epoch = int(t.timestamp())
    if m == "ceil":
        rounded = ((epoch + tick_seconds - 1) // tick_seconds) * tick_seconds
    else:
        rounded = (epoch // tick_seconds) * tick_seconds
    return datetime.fromtimestamp(rounded, tz=timezone.utc)


def init_db_pool(minconn: int = 1, maxconn: int = 10):
    """Initialize a psycopg2 connection pool."""
    global DB_POOL
    if DB_POOL:
        return
    dsns = []
    if DATABASE_URL:
        dsns.append(DATABASE_URL)
    if DATABASE_PUBLIC_URL and DATABASE_PUBLIC_URL not in dsns:
        dsns.append(DATABASE_PUBLIC_URL)

    # Ignore unresolved template strings pasted into env vars (e.g. "{{RAILWAY_TCP_PROXY_PORT}}").
    dsns = [d for d in dsns if "{{" not in d and "}}" not in d]

    last_err = None
    for dsn in dsns:
        host = "unknown"
        try:
            host = urllib.parse.urlparse(dsn).hostname or "unknown"
        except Exception:
            pass

        try:
            DB_POOL = pg_pool.SimpleConnectionPool(
                minconn=minconn,
                maxconn=maxconn,
                dsn=dsn,
                cursor_factory=RealDictCursor,
                sslmode=DB_SSLMODE,
            )
            logging.info("DB pool initialized using host=%s sslmode=%s", host, DB_SSLMODE)
            return
        except Exception as e:
            last_err = e
            DB_POOL = None
            logging.warning("DB connect failed using host=%s (%s)", host, e.__class__.__name__)

    if last_err:
        raise last_err
    raise RuntimeError("No valid database DSN configured (found unresolved template placeholders in DATABASE_URL/DATABASE_PUBLIC_URL).")


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
    await ensure_db_ready()
    return await asyncio.to_thread(fn, *args, **kwargs)


async def ensure_db_ready():
    """
    Lazy, idempotent DB bootstrap.
    Prevents command handlers from failing if on_ready DB init did not run yet.
    """
    global DB_READY
    if DB_READY and DB_POOL:
        return

    async with db_init_lock:
        if DB_READY and DB_POOL:
            return
        await asyncio.to_thread(init_db_pool, 1, 10)
        await asyncio.to_thread(init_db)
        await asyncio.to_thread(heal_sequences)
        DB_READY = True


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


def fmt_int(value) -> str:
    if value is None:
        return "N/A"
    try:
        return f"{int(value):,}"
    except Exception:
        return "N/A"


def _supply_confidence_label(tx_count: int, qty_sum: int, gold_sum: int, zero_gold_count: int) -> str:
    tx = max(0, int(tx_count or 0))
    qty = max(0, int(qty_sum or 0))
    gold = max(0, int(gold_sum or 0))
    zero = max(0, int(zero_gold_count or 0))
    if tx <= 0:
        return "Unknown"

    zero_ratio = (zero / tx) if tx > 0 else 0.0
    if zero_ratio >= 0.60 and qty >= 100_000:
        return "Likely Feed"
    if zero_ratio >= 0.30 and qty >= 50_000:
        return "Possible Feed"
    if zero == 0 and tx >= 5 and qty > 0:
        return "Likely Market Trade"
    if gold == 0 and qty > 0:
        return "Likely Feed"
    return "Mixed"


def _build_supply_resource_breakdown(details: list[dict]) -> dict:
    """
    Returns per-seller aggregates:
    {
      "seller_lower": {
        "display": "SellerName",
        "resources": {"Food": {"qty": 123, "tx": 4}, ...}
      }
    }
    """
    out = {}
    for d in details or []:
        seller = str(d.get("seller_kingdom") or "").strip()
        if not seller:
            continue
        sk = seller.lower()
        res = str(d.get("resource") or "").strip() or "Unknown"
        qty = int(d.get("quantity") or 0)
        slot = out.setdefault(sk, {"display": seller, "resources": {}})
        r = slot["resources"].setdefault(res, {"qty": 0, "tx": 0})
        r["qty"] += max(0, qty)
        r["tx"] += 1
    return out


def _top_resource_text_for_seller(resource_map: dict, seller_key: str) -> str:
    s = (resource_map or {}).get(str(seller_key or "").lower()) or {}
    resources = s.get("resources") or {}
    if not resources:
        return "Top: N/A"
    top_name, top_vals = max(resources.items(), key=lambda kv: (int(kv[1].get("qty") or 0), int(kv[1].get("tx") or 0), kv[0]))
    return f"Top: {top_name} {fmt_int(int(top_vals.get('qty') or 0))} ({int(top_vals.get('tx') or 0)} tx)"


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
            v = parse_first_int_from_value_line(line)
            if v is not None:
                dp = v
        if "number of castles" in ll:
            v = parse_first_int_from_value_line(line)
            if v is not None:
                castles = v
    return kingdom, dp, castles


def parse_first_int_from_value_line(line: str):
    m = re.search(r":\s*([\d,]+)", line)
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", ""))
    except Exception:
        return None


def normalize_kingdom_lookup_key(value: str | None) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _safe_int_or_none(v) -> int | None:
    try:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            n = int(v)
            return n if n > 0 else None
        s = str(v).strip().replace(",", "")
        if not s:
            return None
        n = int(float(s))
        return n if n > 0 else None
    except Exception:
        return None


def _tzinfo_from_token(token: str | None):
    t = str(token or "").strip().upper()
    if not t:
        return None
    if t in ("Z", "UTC", "GMT"):
        return timezone.utc
    offsets = {
        "EST": -5, "EDT": -4,
        "CST": -6, "CDT": -5,
        "MST": -7, "MDT": -6,
        "PST": -8, "PDT": -7,
    }
    if t in offsets:
        return timezone(timedelta(hours=offsets[t]))
    m = re.match(r"^([+-])(\d{2})(?::?(\d{2}))?$", t)
    if m:
        sign = -1 if m.group(1) == "-" else 1
        hh = int(m.group(2))
        mm = int(m.group(3) or "0")
        return timezone(sign * timedelta(hours=hh, minutes=mm))
    return None


def _resolve_report_default_tzinfo():
    txt = str(KG_REPORT_DEFAULT_TZ or "UTC").strip()
    tzi = _tzinfo_from_token(txt)
    if tzi:
        return tzi
    if ZoneInfo is not None:
        try:
            return ZoneInfo(txt)
        except Exception:
            pass
    logging.warning("Invalid KG_REPORT_DEFAULT_TZ=%r, defaulting to UTC", txt)
    return timezone.utc


REPORT_DEFAULT_TZINFO = _resolve_report_default_tzinfo()


def parse_report_datetime_from_line(text: str) -> tuple[datetime | None, bool]:
    """
    Parse report timestamps robustly across formats:
    - Date: 2026-03-01 12:00:00
    - Received: Mar 1, 2026, 12:00:00 PM
    - Optional timezone suffixes (UTC, CST, +01:00)
    - [mytime] wrappers, including epoch payloads
    """
    raw = str(text or "").strip()
    if not raw:
        return None, False

    # Normalize common wrappers seen in pasted reports.
    s = re.sub(r"\[/?mytime\]", " ", raw, flags=re.IGNORECASE)
    s = re.sub(r"/mytime", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    # Epoch payload inside wrappers (seconds or milliseconds).
    m_epoch = re.search(r"\b(\d{10,13})\b", s)
    if m_epoch:
        try:
            n = int(m_epoch.group(1))
            if n > 10**12:
                n = int(n / 1000)
            if 0 < n < 32503680000:  # before year 3000
                return datetime.fromtimestamp(n, tz=timezone.utc), True
        except Exception:
            pass

    # YYYY-MM-DD HH:MM:SS [TZ]
    m_iso = re.search(
        r"(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})(?:\s*(UTC|GMT|[ECMP][SD]T|Z|[+-]\d{2}:?\d{2}))?",
        s,
        re.IGNORECASE,
    )
    if m_iso:
        try:
            dt = datetime.strptime(m_iso.group(1).replace("T", " "), "%Y-%m-%d %H:%M:%S")
            explicit = bool(_tzinfo_from_token(m_iso.group(2)))
            tzi = _tzinfo_from_token(m_iso.group(2)) or REPORT_DEFAULT_TZINFO
            return dt.replace(tzinfo=tzi).astimezone(timezone.utc), explicit
        except Exception:
            pass

    # Month Day, Year, HH:MM:SS AM/PM [TZ]
    m_named = re.search(
        r"([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4},\s+\d{1,2}:\d{2}:\d{2}\s+[AP]M)(?:\s*(UTC|GMT|[ECMP][SD]T|Z|[+-]\d{2}:?\d{2}))?",
        s,
        re.IGNORECASE,
    )
    if m_named:
        part = m_named.group(1)
        explicit = bool(_tzinfo_from_token(m_named.group(2)))
        tzi = _tzinfo_from_token(m_named.group(2)) or REPORT_DEFAULT_TZINFO
        for fmt in ("%b %d, %Y, %I:%M:%S %p", "%B %d, %Y, %I:%M:%S %p"):
            try:
                dt = datetime.strptime(part, fmt)
                return dt.replace(tzinfo=tzi).astimezone(timezone.utc), explicit
            except Exception:
                continue

    return None, False


def _auto_infer_report_time(rt: datetime, ct: datetime) -> datetime:
    """
    For no-TZ report lines only: try hour-shifting to align with capture time.
    This handles mixed user timezones where Date/Received lacks timezone data.
    """
    if not KG_REPORT_AUTO_INFER_TZ:
        return rt
    base_abs = abs((ct - rt).total_seconds())
    if base_abs < max(0, int(KG_REPORT_INFER_IF_DELTA_MINUTES or 0)) * 60:
        return rt

    future_allow = timedelta(minutes=max(0, int(KG_REPORT_MAX_FUTURE_MINUTES or 0)))
    max_past = timedelta(hours=max(1, int(KG_REPORT_INFER_MAX_PAST_HOURS or 1)))

    best = rt
    best_abs = base_abs
    for h in range(-14, 15):
        if h == 0:
            continue
        cand = rt + timedelta(hours=h)
        if cand > ct + future_allow:
            continue
        if cand < ct - max_past:
            continue
        score = abs((ct - cand).total_seconds())
        if score < best_abs:
            best = cand
            best_abs = score

    # Require at least 60m improvement to avoid noisy flips.
    if best is not rt and (best_abs + 3600) < base_abs:
        return best
    return rt


def coerce_report_time(
    report_ts: datetime | None,
    captured_ts: datetime | None,
    has_explicit_tz: bool = False,
) -> datetime | None:
    """
    Keep parsed report time sane relative to message capture time.
    If parsed time lands in the future beyond tolerance, clamp to captured time.
    """
    if report_ts is None:
        return None
    rt = normalize_to_utc(report_ts)
    if captured_ts is None:
        return rt
    ct = normalize_to_utc(captured_ts)
    if not has_explicit_tz:
        rt = _auto_infer_report_time(rt, ct)
    if rt > ct + timedelta(minutes=max(0, int(KG_REPORT_MAX_FUTURE_MINUTES or 0))):
        return ct
    return rt


def _extract_nw_from_game_api_payload(payload: dict) -> int | None:
    if not isinstance(payload, dict):
        return None
    candidates = [
        payload.get("networth"),
        payload.get("net_worth"),
        (payload.get("kingdom") or {}).get("networth") if isinstance(payload.get("kingdom"), dict) else None,
        (payload.get("kingdom") or {}).get("net_worth") if isinstance(payload.get("kingdom"), dict) else None,
    ]
    for c in candidates:
        n = _safe_int_or_none(c)
        if n:
            return n
    return None


def _dict_pick(d: dict, *keys):
    if not isinstance(d, dict):
        return None
    for k in keys:
        if k in d:
            return d.get(k)
    return None


def _decode_kg_asmx_response(body_text: str) -> dict | None:
    try:
        data = json.loads(body_text or "{}")
    except Exception:
        return None
    if isinstance(data, dict) and "d" in data:
        inner = data.get("d")
        if isinstance(inner, str):
            try:
                data = json.loads(inner)
            except Exception:
                return None
        elif isinstance(inner, dict):
            data = inner
    return data if isinstance(data, dict) else None


def _kg_webservice_post(service: str, method: str, payload: dict) -> dict | None:
    data, _dbg = _kg_webservice_post_debug(service, method, payload)
    return data


def _kg_webservice_post_debug(service: str, method: str, payload: dict) -> tuple[dict | None, dict]:
    base = str(KG_GAME_API_BASE or "").strip().rstrip("/")
    if not base:
        return None, {"ok": False, "reason": "no_base_url"}
    url = f"{base}/WebService/{service}.asmx/{method}"
    # Compact JSON (no spaces) so the body matches the browser byte-for-byte.
    body = json.dumps(payload or {}, separators=(",", ":")).encode("utf-8")
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"
        ),
        "World-Id": str(max(1, int(KG_GAME_WORLD_ID or 1))),
        "Origin": "https://kingdomgame.net",
        "Referer": "https://kingdomgame.net/rankings",
    }
    if KG_GAME_WEB_COOKIE:
        headers["Cookie"] = KG_GAME_WEB_COOKIE
    try:
        req = urllib.request.Request(url, data=body, method="POST", headers=headers)
        with urllib.request.urlopen(req, timeout=max(1.0, float(KG_GAME_API_TIMEOUT))) as resp:
            text = resp.read().decode("utf-8", errors="replace")
            parsed = _decode_kg_asmx_response(text)
            dbg = {
                "ok": bool(parsed is not None),
                "status": int(getattr(resp, "status", 0) or 0),
                "body_preview": (text or "")[:220],
                "reason": "ok" if parsed is not None else "decode_failed",
            }
            return parsed, dbg
    except urllib.error.HTTPError as e:
        try:
            err_body = e.read().decode("utf-8", errors="replace")
        except Exception:
            err_body = ""
        logging.debug("KG webservice %s/%s failed: %s %s", service, method, e.code, err_body[:200])
        return None, {
            "ok": False,
            "status": int(getattr(e, "code", 0) or 0),
            "reason": "http_error",
            "body_preview": (err_body or "")[:220],
        }
    except Exception:
        return None, {
            "ok": False,
            "reason": "request_exception",
        }


def _kg_extract_auth_credentials(login_payload: dict) -> tuple[int | None, str]:
    account_id = _safe_int_or_none(_dict_pick(login_payload, "accountId", "AccountId"))
    token = str(_dict_pick(login_payload, "token", "Token") or "").strip()
    return account_id, token


def _kg_extract_first_kingdom_id(payload: dict) -> int | None:
    rows = _dict_pick(payload, "kingdoms", "Kingdoms")
    if isinstance(rows, list):
        for row in rows:
            if isinstance(row, dict):
                kid = _safe_int_or_none(_dict_pick(row, "id", "Id", "kingdomId", "KingdomId"))
                if kid:
                    return kid
    return _safe_int_or_none(_dict_pick(payload, "id", "Id", "kingdomId", "KingdomId"))


def _kg_get_static_auth(now_ts: float) -> dict | None:
    account_id = _safe_int_or_none(KG_GAME_ACCOUNT_ID)
    token = str(KG_GAME_TOKEN or "").strip()
    if not account_id or not token:
        return None
    search_kingdom_id = int(KG_GAME_SEARCH_KINGDOM_ID or 0)
    if search_kingdom_id <= 0:
        search_kingdom_id = _safe_int_or_none(KG_GAME_TOKEN_KINGDOM_ID) or 0
    auth_row = {
        "account_id": int(account_id),
        "token": token,
        "search_kingdom_id": int(search_kingdom_id or 0),
        "expires_at": now_ts + max(60, int(KG_GAME_AUTH_CACHE_SECONDS or 1800)),
        "auth_mode": "static_token",
    }
    KG_API_AUTH_CACHE.clear()
    KG_API_AUTH_CACHE.update(auth_row)
    return KG_API_AUTH_CACHE


def _kg_get_auth(force_refresh: bool = False) -> dict | None:
    now_ts = time.time()
    # Prefer login auth when credentials are available; static tokens may expire silently.
    login_auth = _kg_get_login_auth(force_refresh)
    if login_auth:
        return login_auth
    static_auth = _kg_get_static_auth(now_ts)
    if static_auth:
        return static_auth
    return None


def _kg_get_login_auth(force_refresh: bool = False) -> dict | None:
    """
    Mint a fresh token via email/password login.
    Used as a fallback when the static token is expired/invalid.
    """
    now_ts = time.time()
    if (
        not force_refresh
        and KG_API_AUTH_CACHE
        and str(KG_API_AUTH_CACHE.get("auth_mode") or "") == "login"
        and float(KG_API_AUTH_CACHE.get("expires_at", 0) or 0) > now_ts
    ):
        return KG_API_AUTH_CACHE
    if not KG_GAME_EMAIL or not KG_GAME_PASSWORD:
        return None

    login_payload = _kg_webservice_post("User", "Login", {"email": KG_GAME_EMAIL, "password": KG_GAME_PASSWORD}) or {}
    account_id, token = _kg_extract_auth_credentials(login_payload)
    if not account_id or not token:
        return None

    search_kingdom_id = int(KG_GAME_SEARCH_KINGDOM_ID or 0)
    if search_kingdom_id <= 0:
        search_kingdom_id = _safe_int_or_none(KG_GAME_TOKEN_KINGDOM_ID) or 0
    if search_kingdom_id <= 0:
        kingdoms_payload = _kg_webservice_post(
            "Kingdoms",
            "GetKingdoms",
            {"accountId": int(account_id), "token": token},
        ) or {}
        search_kingdom_id = _kg_extract_first_kingdom_id(kingdoms_payload) or 0

    KG_API_AUTH_CACHE.clear()
    KG_API_AUTH_CACHE.update({
        "account_id": int(account_id),
        "token": token,
        "search_kingdom_id": int(search_kingdom_id or 0),
        "expires_at": now_ts + max(60, int(KG_GAME_AUTH_CACHE_SECONDS or 1800)),
        "auth_mode": "login",
    })
    return KG_API_AUTH_CACHE


def _kg_extract_networth_from_search_payload(search_term: str, payload: dict) -> int | None:
    if not isinstance(payload, dict):
        return None
    rows = _dict_pick(payload, "kingdoms", "Kingdoms")
    if not isinstance(rows, list) or not rows:
        return None

    needle = str(search_term or "").strip().casefold()
    chosen = None
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = str(_dict_pick(row, "name", "Name") or "").strip().casefold()
        if name and name == needle:
            chosen = row
            break
    if chosen is None:
        for row in rows:
            if not isinstance(row, dict):
                continue
            name = str(_dict_pick(row, "name", "Name") or "").strip().casefold()
            if needle and needle in name:
                chosen = row
                break
    if chosen is None:
        chosen = rows[0] if isinstance(rows[0], dict) else None
    if not isinstance(chosen, dict):
        return None
    return _safe_int_or_none(_dict_pick(chosen, "networth", "netWorth", "net_worth", "NetWorth"))


def fetch_kingdom_networth_from_game_api(kingdom: str) -> int | None:
    """
    Optional fallback NW source when local spy DB does not have fresh data.
    Prefers authenticated KingdomGame WebService lookup and falls back to legacy public API probes.
    """
    base = str(KG_GAME_API_BASE or "").strip().rstrip("/")
    k = str(kingdom or "").strip()
    if not base or not k:
        return None

    cache_key = k.lower()
    now_ts = time.time()
    cached = NW_API_CACHE.get(cache_key)
    if cached and cached[1] > now_ts:
        return int(cached[0]) if cached[0] else None

    auth = _kg_get_auth(force_refresh=False)
    if auth:
        payload = {
            "accountId": int(auth.get("account_id") or 0),
            "token": str(auth.get("token") or ""),
            "kingdomId": int(auth.get("search_kingdom_id") or 0),
            "searchTerm": k,
        }
        search_data = _kg_webservice_post("Kingdoms", "SearchByName", payload) or {}
        nw = _kg_extract_networth_from_search_payload(k, search_data)
        if nw is None:
            auth = _kg_get_auth(force_refresh=True)
            if auth:
                payload = {
                    "accountId": int(auth.get("account_id") or 0),
                    "token": str(auth.get("token") or ""),
                    "kingdomId": int(auth.get("search_kingdom_id") or 0),
                    "searchTerm": k,
                }
                search_data = _kg_webservice_post("Kingdoms", "SearchByName", payload) or {}
                nw = _kg_extract_networth_from_search_payload(k, search_data)
        if nw:
            NW_API_CACHE[cache_key] = (int(nw), now_ts + max(5, int(KG_GAME_API_CACHE_SECONDS)))
            return int(nw)

    # Legacy probes: leave in place as a non-auth fallback.
    enc = urllib.parse.quote(k)
    for url in (f"{base}/api/war-room/{enc}", f"{base}/api/kingdom/{enc}"):
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"}, method="GET")
            with urllib.request.urlopen(req, timeout=max(1.0, float(KG_GAME_API_TIMEOUT))) as resp:
                body = resp.read().decode("utf-8", errors="replace")
                data = json.loads(body) if body else {}
                nw = _extract_nw_from_game_api_payload(data)
                if nw:
                    NW_API_CACHE[cache_key] = (int(nw), now_ts + max(5, int(KG_GAME_API_CACHE_SECONDS)))
                    return int(nw)
        except Exception:
            continue
    NW_API_CACHE[cache_key] = (None, now_ts + 15)
    return None


def parse_spy_details(text: str) -> dict:
    """
    Pull extra fields for !spy presentation from a raw report.
    Keeps parsing permissive because report wording can vary.
    """
    details = {
        "target": None,
        "king_name": None,
        "alliance": None,
        "spies_sent": None,
        "spies_lost": None,
        "result": None,
        "net_worth": None,
    }

    for raw_line in (text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        ll = line.lower()

        if ll.startswith("target:"):
            details["target"] = line.split(":", 1)[1].strip()
            continue
        if ll.startswith("king:") or ll.startswith("king name:"):
            details["king_name"] = line.split(":", 1)[1].strip()
            continue
        if ll.startswith("alliance:"):
            details["alliance"] = line.split(":", 1)[1].strip()
            continue
        if "spies sent" in ll:
            v = parse_first_int_from_value_line(line)
            if v is not None:
                details["spies_sent"] = v
            continue
        if "spies lost" in ll:
            v = parse_first_int_from_value_line(line)
            if v is not None:
                details["spies_lost"] = v
            continue
        if "networth" in ll or "net worth" in ll:
            v = parse_first_int_from_value_line(line)
            if v is not None:
                details["net_worth"] = v
            continue

        if details["result"] is None:
            if ll.startswith("result level:") or ll.startswith("result:"):
                details["result"] = line.split(":", 1)[1].strip()
            elif "spy mission was successful" in ll or "spies were successful" in ll:
                details["result"] = "Success"
            elif "spy mission failed" in ll or "spies were caught" in ll:
                details["result"] = "Failed"

    return details


UNIT_ALIASES = {
    "lc": "light_cavalry",
    "light cav": "light_cavalry",
    "light cavalry": "light_cavalry",
    "hc": "heavy_cavalry",
    "heavy cav": "heavy_cavalry",
    "heavy cavalry": "heavy_cavalry",
    "knight": "knights",
    "knights": "knights",
    "pike": "pikemen",
    "pikeman": "pikemen",
    "pikemen": "pikemen",
    "archer": "archers",
    "archers": "archers",
    "crossbow": "crossbowmen",
    "crossbowmen": "crossbowmen",
    "footman": "footmen",
    "footmen": "footmen",
    "peasant": "peasants",
    "peasants": "peasants",
    "elite": "elites",
    "elites": "elites",
}

UNIT_DISPLAY = {
    "light_cavalry": "Light Cavalry",
    "heavy_cavalry": "Heavy Cavalry",
    "knights": "Knights",
    "pikemen": "Pikemen",
    "archers": "Archers",
    "crossbowmen": "Crossbowmen",
    "footmen": "Footmen",
    "peasants": "Peasants",
    "elites": "Elites",
}


def normalize_unit_name(unit_name: str) -> str | None:
    n = str(unit_name or "").strip().lower()
    n = re.sub(r"\s+", " ", n)
    if not n:
        return None
    if n in UNIT_ALIASES:
        return UNIT_ALIASES[n]
    for k, v in UNIT_ALIASES.items():
        if k in n:
            return v
    return None


def parse_units_inline(text: str) -> dict:
    """
    Parse simple inline unit expressions like:
    '3000 LC', '1,500 Heavy Cavalry, 200 Pike'
    """
    out = {}
    for m in re.finditer(r"(\d[\d,]*)\s+([A-Za-z][A-Za-z ]{1,30})", str(text or "")):
        count = parse_first_int_from_value_line(f"x:{m.group(1)}")
        raw_name = m.group(2).strip()
        key = normalize_unit_name(raw_name)
        if count is None or key is None:
            continue
        out[key] = int(out.get(key, 0) or 0) + int(count)
    return out


def parse_incoming_attack_alert(text: str) -> dict | None:
    """
    Examples:
    - 'you have been attacked by Galileo! He sent 3000 LC'
    - 'You have been attacked by Galileo (NW:86440)\n...enemy forces was as follows: 38000 Light Cavalry'
    """
    s = str(text or "").strip()
    if not s:
        return None

    attacker = None
    defender = None
    attacker_nw = None
    occurred_at = None
    occurred_at_has_tz = False
    units = {}

    for raw_line in s.splitlines():
        line = str(raw_line or "").strip()
        if not line:
            continue
        ll = line.lower()
        if ll.startswith("date:") or ll.startswith("received:"):
            dt, has_tz = parse_report_datetime_from_line(line)
            if dt and (occurred_at is None or dt < occurred_at):
                occurred_at = dt
                occurred_at_has_tz = bool(has_tz)

    # Legacy one-line alert format.
    m = re.search(r"attacked by\s+(.+?)!\s*he sent\s+(.+)$", s, re.IGNORECASE)
    if m:
        attacker = m.group(1).strip()
        units = parse_units_inline(m.group(2))

    # Multi-line attack report alert format.
    if not attacker:
        m2 = re.search(r"you have been attacked by\s+(.+?)(?:\s*\(|$)", s, re.IGNORECASE)
        if m2:
            attacker = m2.group(1).strip()
    m_nw = re.search(r"\(\s*NW\s*[:=]\s*([\d,]+)\s*\)", s, re.IGNORECASE)
    if m_nw:
        attacker_nw = _safe_int_or_none(m_nw.group(1))
    if not units:
        m3 = re.search(
            r"(?:composition of the enemy forces was as follows|enemy forces was as follows)\s*:\s*(.+)$",
            s,
            re.IGNORECASE | re.MULTILINE,
        )
        if m3:
            units = parse_units_inline(m3.group(1))

    # Defender/target hints from richer report formats.
    m_def = re.search(r"^\s*recipient(?:\(s\))?\s*:\s*(.+)$", s, re.IGNORECASE | re.MULTILINE)
    if not m_def:
        m_def = re.search(r"^\s*to\s*:\s*(.+)$", s, re.IGNORECASE | re.MULTILINE)
    if not m_def:
        m_def = re.search(r"^\s*target\s*:\s*(.+)$", s, re.IGNORECASE | re.MULTILINE)
    if m_def:
        defender = str(m_def.group(1) or "").strip()

    # Outgoing AR style: Subject: Attack Report: Defender
    if not defender:
        m_sub_def = re.search(r"^\s*subject\s*:\s*attack report\s*:\s*(.+)$", s, re.IGNORECASE | re.MULTILINE)
        if m_sub_def:
            defender = str(m_sub_def.group(1) or "").strip()

    if not attacker or not units:
        return None
    return {
        "attacker": attacker,
        "attacker_nw": attacker_nw,
        "defender": defender,
        "occurred_at": occurred_at,
        "occurred_at_has_tz": occurred_at_has_tz,
        "units": units,
    }


def parse_attack_details(text: str) -> dict:
    """
    Parse core fields from an attack report.
    Kept permissive because report text varies between accounts/views.
    """
    details = {
        "attacker": None,
        "defender": None,
        "result": None,
        "land_taken": None,
        "settlements_lost_count": 0,
        "settlements_lost": [],
        "reported_at": None,
        "reported_at_has_tz": False,
        "sent_units": {},
        "lost_units": {},
    }

    lines = (text or "").splitlines()
    for raw_line in lines:
        line = (raw_line or "").strip()
        if not line:
            continue
        ll = line.lower()

        # Date line can contain wrappers like [mytime]...[/mytime], epoch values, or explicit TZ suffixes.
        if ll.startswith("date:") or ll.startswith("received:"):
            dt, has_tz = parse_report_datetime_from_line(line)
            if dt and (details["reported_at"] is None or dt < details["reported_at"]):
                details["reported_at"] = dt
                details["reported_at_has_tz"] = bool(has_tz)
            continue

        if ll.startswith("target:"):
            details["defender"] = line.split(":", 1)[1].strip()
            continue
        if ll.startswith("recipient"):
            # In many inbox formats, recipient is the attacker (you).
            details["attacker"] = details["attacker"] or line.split(":", 1)[1].strip()
            continue
        if ll.startswith("sender:") or ll.startswith("from:"):
            details["attacker"] = details["attacker"] or line.split(":", 1)[1].strip()
            continue
        if ll.startswith("subject:"):
            subj = line.split(":", 1)[1].strip()
            m_sub = re.match(r"attack report:\s*(.+)$", subj, re.IGNORECASE)
            if m_sub and not details["defender"]:
                details["defender"] = m_sub.group(1).strip()
            continue

        if ll.startswith("attack result:") or ll.startswith("result:"):
            details["result"] = line.split(":", 1)[1].strip()
            continue

        if "casualties during the attack" in ll:
            # ex: "25861/160619 Heavy Cavalry"
            for mm in re.finditer(r"(\d[\d,]*)\s*/\s*(\d[\d,]*)\s+([A-Za-z][A-Za-z ]{1,30})", line):
                lost = parse_first_int_from_value_line(f"x:{mm.group(1)}")
                sent = parse_first_int_from_value_line(f"x:{mm.group(2)}")
                unit = normalize_unit_name(mm.group(3))
                if unit is None:
                    continue
                if sent is not None:
                    details["sent_units"][unit] = int(details["sent_units"].get(unit, 0) or 0) + int(sent)
                if lost is not None:
                    details["lost_units"][unit] = int(details["lost_units"].get(unit, 0) or 0) + int(lost)
            continue

        # Subject/Attack header: "... Attack Report: Attacker attacked Defender"
        if "attack report:" in ll and "attacked" in ll:
            right = line.split("attack report:", 1)[1].strip()
            m_pair = re.match(r"(.+?)\s+attacked\s+(.+)$", right, re.IGNORECASE)
            if m_pair:
                details["attacker"] = details["attacker"] or m_pair.group(1).strip()
                details["defender"] = details["defender"] or m_pair.group(2).strip()
            continue

        # Header-only format: "Attack Report: Galileo (NW: + 171041)"
        if ll.startswith("attack report:") and "attacked" not in ll and not details["defender"]:
            m_hdr = re.match(r"attack report:\s*(.+?)(?:\s*\(.*\))?$", line, re.IGNORECASE)
            if m_hdr:
                details["defender"] = m_hdr.group(1).strip()
            continue

        # Land parse (strict to avoid NW/other-number misreads).
        if details["land_taken"] is None and ("land" in ll or "acre" in ll):
            if "you have gained the following during the attack" in ll:
                m_gained_land = re.search(r"([\d,]+)\s+land\b", line, re.IGNORECASE)
                if m_gained_land:
                    try:
                        details["land_taken"] = int(m_gained_land.group(1).replace(",", ""))
                        continue
                    except Exception:
                        pass
            if ll.startswith("land taken:") or ll.startswith("land:"):
                m_land = re.search(r":\s*([\d,]+)\s*(?:acres?)?", line, re.IGNORECASE)
                if m_land:
                    try:
                        details["land_taken"] = int(m_land.group(1).replace(",", ""))
                        continue
                    except Exception:
                        pass
            if "acres" in ll and any(k in ll for k in ("gained", "taken", "captured", "conquered", "stolen")):
                m_land = re.search(r"([\d,]+)\s*acres?", line, re.IGNORECASE)
                if m_land:
                    try:
                        details["land_taken"] = int(m_land.group(1).replace(",", ""))
                        continue
                    except Exception:
                        pass

        # Settlement movement/loss markers.
        if any(k in ll for k in ("settlement", "town", "city")) and any(
            k in ll for k in ("lost", "sacked", "captured", "taken", "took", "take")
        ):
            if any(bad in ll for bad in ("unable to take", "failed to take", "could not take", "unsuccessful")):
                continue
            name = None
            m_name = re.search(r"(?:settlement|town|city)\s+([A-Za-z0-9][A-Za-z0-9 '\-]{1,48})", line, re.IGNORECASE)
            if m_name:
                name = m_name.group(1).strip()
            if name:
                details["settlements_lost"].append(name)
            else:
                details["settlements_lost"].append(line[:120])

    # Fallback source for attacker/defender
    if not details["attacker"]:
        for raw_line in lines:
            line = (raw_line or "").strip()
            if line.lower().startswith("from:"):
                details["attacker"] = line.split(":", 1)[1].strip()
                break
    if not details["defender"]:
        for raw_line in lines:
            line = (raw_line or "").strip()
            if line.lower().startswith("to:"):
                details["defender"] = line.split(":", 1)[1].strip()
                break

    # Dedup settlement entries preserving order.
    seen = set()
    uniq = []
    for x in details["settlements_lost"]:
        k = str(x).strip().lower()
        if not k or k in seen:
            continue
        seen.add(k)
        uniq.append(str(x).strip())
    details["settlements_lost"] = uniq
    details["settlements_lost_count"] = len(uniq)
    return details


def estimate_enemy_cavalry(troops: dict) -> int:
    """
    Estimate "their cav" by summing troop lines that contain "cavalry".
    """
    total = 0
    for name, count in (troops or {}).items():
        n = (name or "").lower()
        if "cavalry" in n:
            try:
                total += int(count or 0)
            except Exception:
                continue
    return total


def estimate_enemy_pikemen(troops: dict) -> int:
    """
    Estimate enemy pike by summing troop lines that contain 'pikemen' or 'pike'.
    """
    total = 0
    for name, count in (troops or {}).items():
        n = (name or "").lower()
        if "pikemen" in n or "pike" in n:
            try:
                total += int(count or 0)
            except Exception:
                continue
    return total


def build_spy_text_report(row) -> tuple[str, str]:
    """
    Returns:
    - human-readable summary text
    - full raw report text (for .txt attachment)
    """
    text = extract_report_text_for_row(row)
    details = parse_spy_details(text)
    troops = parse_sr_troops(text)

    kingdom = details.get("target") or row.get("kingdom") or "Unknown"
    king_name = details.get("king_name") or "N/A"
    alliance = details.get("alliance") or "N/A"
    spies_sent = details.get("spies_sent")
    spies_lost = details.get("spies_lost")
    spy_result = details.get("result") or "N/A"
    net_worth = details.get("net_worth")
    captured_at = row.get("created_at")
    captured_txt = str(captured_at).replace("T", " ").split(".", 1)[0] if captured_at else "Unknown"

    dp = int(row.get("defense_power") or 0) if row.get("defense_power") is not None else 0
    castles = int(row.get("castles") or 0)
    dp_with_castles = ceil(dp * (1 + castle_bonus(castles))) if dp > 0 else 0

    enemy_cav = estimate_enemy_cavalry(troops)
    pike_to_send = (enemy_cav // 4) + 1 if enemy_cav > 0 else 0
    enemy_pike = estimate_enemy_pikemen(troops)
    cav_to_counter_pike = (4 * enemy_pike) + 1 if enemy_pike > 0 else 0

    lines = [
        f"Report Date/Time: {captured_txt}",
        f"Kingdom: {kingdom}",
        f"Alliance: {alliance}",
        f"Spies Sent/Lost/Result: {fmt_int(spies_sent)} / {fmt_int(spies_lost)} / {spy_result}",
        f"Net Worth: {fmt_int(net_worth)}",
        f"DP: {fmt_int(dp)}",
        f"DP with Castles: {fmt_int(dp_with_castles)} (Castles: {castles})",
        f"Enemy Cav (parsed): {fmt_int(enemy_cav)}",
        f"Enemy Pike (parsed): {fmt_int(enemy_pike)}",
        f"Pike to send (1/4 cav + 1): {fmt_int(pike_to_send)}",
        f"Cav to counter enemy pike (4x enemy pike + 1): {fmt_int(cav_to_counter_pike)}",
        f"Report ID: {row.get('id')}",
    ]
    return "\n".join(lines), text


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

        if len(name) < 2 or val < 0:
            continue

        troops[name] = val

    return troops


def _oven_float_env(unit_key: str, suffix: str, default: float) -> float:
    env_name = f"OVEN_{unit_key.upper()}_{suffix}"
    return _env_float(env_name, default)


def _oven_unit_models() -> dict[str, dict]:
    """
    Training inference constants.

    Defaults are configurable because KG balance values may change. Override per unit with env vars like:
      OVEN_LIGHT_CAVALRY_PEASANTS=1
      OVEN_LIGHT_CAVALRY_NW=0.25
      OVEN_LIGHT_CAVALRY_MINUTES_PER_1000=90
    """
    defaults = {
        # NW defaults are from the KG troop stats table. Peasant use and train minutes stay env-tunable.
        "footmen": ("Footmen", 1.0, 0.38, 60.0, "If this is foot, usually sit/wait or avoid cav trades."),
        "pikemen": ("Pikemen", 1.0, 0.50, 90.0, "LC/Pike/Archers have similar NW footprints; verify by later SR."),
        "archers": ("Archers", 1.0, 0.50, 90.0, "If this is archers, cav pressure can punish it."),
        "crossbowmen": ("Crossbowmen", 1.0, 0.38, 90.0, "Crossbow/Foot have similar NW footprints; verify by later SR."),
        "light_cavalry": ("Light Cavalry", 1.0, 0.50, 90.0, "If this is LC, pike timing matters."),
        "heavy_cavalry": ("Heavy Cavalry", 1.0, 0.63, 120.0, "If this is HC, pop/train Footmen into it."),
        "knights": ("Knights", 1.0, 1.63, 180.0, "If this is Knights, expect a high-NW cavalry pop."),
    }
    out = {}
    for key, (display, peasants, nw, minutes_per_1000, counter) in defaults.items():
        env_key = key.upper()
        out[key] = {
            "display": display,
            "peasants": max(0.01, _oven_float_env(env_key, "PEASANTS", peasants)),
            "nw": max(0.0, _oven_float_env(env_key, "NW", nw)),
            "minutes_per_1000": max(0.0, _oven_float_env(env_key, "MINUTES_PER_1000", minutes_per_1000)),
            "counter": counter,
        }
    return out


OVEN_UNIT_MODELS = _oven_unit_models()


def _snapshot_count(troops: dict, normalized_names: set[str], contains_names: set[str] | None = None) -> int | None:
    contains_names = contains_names or set()
    total = 0
    found = False
    for raw_name, raw_count in (troops or {}).items():
        name = str(raw_name or "").strip()
        norm = normalize_unit_name(name)
        low = name.lower()
        if norm in normalized_names or any(part in low for part in contains_names):
            try:
                total += int(raw_count or 0)
                found = True
            except Exception:
                continue
    return total if found else None


def _snapshot_peasant_signal(troops: dict) -> tuple[int | None, str]:
    peasants = _snapshot_count(troops, {"peasants"}, {"peasant"})
    if peasants is not None:
        return peasants, "Peasants"
    population = _snapshot_count(troops, set(), {"population"})
    if population is not None:
        return population, "Population"
    return None, "Peasants/Population"


def _format_dt_short(ts) -> str:
    if not ts:
        return "unknown"
    try:
        return normalize_to_utc(ts).strftime("%b %-d %H:%M UTC")
    except Exception:
        return str(ts).replace("T", " ").split(".", 1)[0]


def _oven_completion_window(old_ts, new_ts, count: int, model: dict, event_time=None) -> str:
    mins_per_1000 = float(model.get("minutes_per_1000") or 0.0)
    if mins_per_1000 <= 0 or count <= 0:
        return "pop time unknown"
    duration = timedelta(minutes=(float(count) / 1000.0) * mins_per_1000)
    if event_time:
        return f"popped/detected around {_format_dt_short(event_time)}"
    if old_ts and new_ts:
        return f"pop window {_format_dt_short(normalize_to_utc(old_ts) + duration)} - {_format_dt_short(normalize_to_utc(new_ts) + duration)}"
    if new_ts:
        return f"possible pop by {_format_dt_short(normalize_to_utc(new_ts) + duration)}"
    return "pop time unknown"


def _oven_confidence(score: float, has_nw: bool) -> str:
    if has_nw and score <= 0.08:
        return "High"
    if score <= 0.18:
        return "Medium"
    return "Low"


def build_oven_candidates(peasant_delta: int, nw_delta: int | None, old_ts=None, new_ts=None, event_time=None) -> list[dict]:
    missing = max(0, int(peasant_delta or 0))
    if missing <= 0:
        return []
    has_nw = nw_delta is not None and int(nw_delta or 0) > 0
    target_nw = int(nw_delta or 0)
    peas_tol = max(1.0, missing * max(0.0, float(OVEN_PEASANT_TOLERANCE_PCT or 0.0)) / 100.0)
    nw_tol = max(1.0, target_nw * max(0.0, float(OVEN_NW_TOLERANCE_PCT or 0.0)) / 100.0) if has_nw else 0.0

    candidates = []
    for unit_key, model in OVEN_UNIT_MODELS.items():
        peas_per = float(model.get("peasants") or 0.0)
        nw_per = float(model.get("nw") or 0.0)
        if peas_per <= 0:
            continue
        count = max(1, int(round(float(missing) / peas_per)))
        expected_peas = count * peas_per
        expected_nw = count * nw_per
        peas_err = abs(expected_peas - missing)
        nw_err = abs(expected_nw - target_nw) if has_nw else 0.0
        if peas_err > peas_tol:
            continue
        peas_score = peas_err / max(1.0, float(missing))
        nw_score = (nw_err / max(1.0, float(target_nw))) if has_nw else 0.10
        score = (nw_score * 0.75) + (peas_score * 0.25) if has_nw else peas_score + 0.10
        if has_nw and nw_err > (nw_tol * 3):
            score += 1.0
        candidates.append({
            "unit_key": unit_key,
            "unit": str(model.get("display") or unit_key),
            "count": int(count),
            "expected_peasants": int(round(expected_peas)),
            "expected_nw": int(round(expected_nw)),
            "peas_error": int(round(peas_err)),
            "nw_error": int(round(nw_err)),
            "score": float(score),
            "confidence": _oven_confidence(float(score), has_nw),
            "time_text": _oven_completion_window(old_ts, new_ts, int(count), model, event_time=event_time),
            "counter": str(model.get("counter") or ""),
        })

    candidates.sort(key=lambda c: (float(c.get("score") if c.get("score") is not None else 999.0), -int(c.get("expected_nw") or 0), str(c.get("unit") or "")))
    return candidates[: max(1, int(OVEN_MAX_RESULTS or 6))]


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


def parse_market_transactions(text: str, buyer_kingdom: str | None = None) -> list[dict]:
    """
    Parse SR market section:
    "The following recent market transactions were also discovered:"
    """
    txs = []
    in_market = False
    buyer = str(buyer_kingdom or "").strip() or None

    for idx, raw_line in enumerate((text or "").splitlines(), start=1):
        line = (raw_line or "").strip()
        ll = line.lower()
        line_clean = line.lstrip("•-* ").strip()
        ll_clean = line_clean.lower()

        if "the following recent market transactions were also discovered" in ll:
            in_market = True
            continue

        if not in_market:
            continue

        if not line:
            continue

        # stop when we hit another section header
        if any(x in ll for x in (
            "our spies also found the following information",
            "the following technology information",
            "the following information was found regarding troop movements",
            "subject:",
            "sender:",
            "recipient",
        )):
            break

        m = re.match(
            r"^(Bought|Sold)\s+([\d,]+)\s+x\s+(.+?)\s+(from|to)\s+(.+?)\s+for\s+([\d,]+)\s+gold(?:\s*\(([^)]+)\))?\s*$",
            line_clean,
            re.IGNORECASE,
        )
        if not m:
            continue

        verb = m.group(1).strip().lower()
        qty = int(m.group(2).replace(",", ""))
        resource = m.group(3).strip()
        edge = m.group(4).strip().lower()
        partner = m.group(5).strip()
        gold = int(m.group(6).replace(",", ""))
        tx_time_txt = (m.group(7) or "").strip() or None

        seller = None
        inferred_buyer = buyer
        if verb == "bought" and edge == "from":
            seller = partner
        elif verb == "sold" and edge == "to":
            inferred_buyer = partner
        elif edge == "from":
            seller = partner
        elif edge == "to":
            inferred_buyer = partner

        txs.append(
            {
                "line_no": idx,
                "tx_type": verb,
                "buyer_kingdom": inferred_buyer,
                "seller_kingdom": seller,
                "partner_kingdom": partner,
                "resource": resource,
                "quantity": qty,
                "gold_amount": gold,
                "tx_time_text": tx_time_txt,
                "raw_line": line,
            }
        )

    return txs


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


def can_read_history(channel: discord.abc.GuildChannel, guild: discord.Guild) -> bool:
    try:
        member = get_member_for_perms(guild)
        if not member:
            return True
        perms = channel.permissions_for(member)
        return bool(perms.view_channel and perms.read_message_history)
    except Exception:
        return False


def is_target_guild(guild: discord.Guild | None) -> bool:
    if not guild:
        return False
    if int(TARGET_GUILD_ID or 0) <= 0:
        return True
    return int(guild.id) == int(TARGET_GUILD_ID)


def get_updates_channel(guild: discord.Guild, fallback: discord.abc.GuildChannel | None = None):
    """
    Preferred channel for bot updates/errors.
    Resolution order:
    1) exact ERROR_CHANNEL_NAME
    2) case-insensitive name match
    3) configured live battle channel
    4) provided fallback
    """
    if not guild:
        return None

    # If a target guild is configured, keep update routing scoped to that guild.
    if int(TARGET_GUILD_ID or 0) > 0 and int(guild.id) != int(TARGET_GUILD_ID):
        return None

    try:
        if int(UPDATES_CHANNEL_ID or 0) > 0:
            by_id = guild.get_channel(int(UPDATES_CHANNEL_ID))
            if by_id and can_send(by_id, guild):
                return by_id

        preferred_name = str(UPDATES_CHANNEL_NAME or ERROR_CHANNEL_NAME or "").strip()
        ch = discord.utils.get(guild.text_channels, name=preferred_name)
        if ch and can_send(ch, guild):
            return ch

        wanted = preferred_name.lower()
        if wanted:
            for t in guild.text_channels:
                if str(getattr(t, "name", "")).strip().lower() == wanted and can_send(t, guild):
                    return t
    except Exception:
        pass

    ch = get_live_battle_channel(guild, fallback)
    if ch and can_send(ch, guild):
        return ch
    if fallback and can_send(fallback, guild):
        return fallback
    return None


def looks_like_spy_report(text: str) -> bool:
    ll = (text or "").lower()
    if "target:" not in ll:
        return False
    return any(x in ll for x in (
        "defensive power",
        "the following technology information was also discovered",
        "our spies also found the following information about the kingdom's troops",
    ))


def looks_like_attack_report(text: str) -> bool:
    ll = (text or "").lower()
    if "subject: attack report:" in ll:
        return True
    if "attack report:" in ll and "attacked" in ll:
        return True
    if "attack report:" in ll and "attack result:" in ll:
        return True
    if "attack result:" in ll and ("land taken" in ll or "acres" in ll):
        return True
    if "attack result:" in ll and ("subject:" in ll or "target:" in ll):
        return True
    return False


def looks_like_recon_report(text: str) -> bool:
    return looks_like_spy_report(text) or looks_like_attack_report(text)


def looks_like_history_candidate_fast(text: str) -> bool:
    """
    Cheap first-pass prefilter to avoid expensive parse/DB work for obvious non-reports.
    This is intentionally permissive to avoid losing valid reports.
    """
    ll = (text or "").lower()
    if len(ll) < 30:
        return False
    if "target:" in ll:
        return True
    if "attack report" in ll or "attack result:" in ll:
        return True
    if "defensive power" in ll:
        return True
    if "our spies also found" in ll:
        return True
    if "the following technology information was also discovered" in ll:
        return True
    if "you have been attacked by" in ll:
        return True
    return False


def sync_ingest_history_candidate(msg_content: str, created_at_utc: datetime, source_message_id: int, source_channel_id: int):
    """
    Process one candidate text in one worker call to reduce async/thread overhead.
    """
    out = {
        "matched": 0,
        "reports_saved": 0,
        "duplicates": 0,
        "attack_reports_saved": 0,
        "attack_duplicates": 0,
        "reports_forwarded": 0,
        "forward_skipped": 0,
        "forward_failures": 0,
        "forward_failure_reason": None,
    }

    res = sync_store_report(msg_content, created_at_utc)
    if res.get("saved"):
        out["matched"] = 1
        if not res.get("duplicate"):
            out["reports_saved"] += 1
        else:
            out["duplicates"] += 1

    ares = sync_store_attack_report(msg_content, created_at_utc, source_message_id, source_channel_id)
    if ares.get("saved"):
        out["matched"] = 1
        if not ares.get("duplicate"):
            out["attack_reports_saved"] += 1
        else:
            out["attack_duplicates"] += 1

    looks_recon = looks_like_recon_report(msg_content)
    newly_saved_local = (int(out["reports_saved"]) + int(out["attack_reports_saved"])) > 0
    if looks_recon and BACKFILL_FORWARD_ENABLED and newly_saved_local:
        fwd = sync_recon_ingest_report(msg_content)
        if fwd.get("ok"):
            out["reports_forwarded"] += 1
        elif fwd.get("disabled"):
            out["forward_skipped"] += 1
        else:
            out["forward_failures"] += 1
            status = int(fwd.get("status") or 0)
            err = str(fwd.get("error") or fwd.get("reason") or "").strip().lower()
            if status:
                if 400 <= status < 500:
                    out["forward_failure_reason"] = f"http_{status}_client"
                elif 500 <= status < 600:
                    out["forward_failure_reason"] = f"http_{status}_server"
                else:
                    out["forward_failure_reason"] = f"http_{status}"
            elif "timed out" in err:
                out["forward_failure_reason"] = "timeout"
            elif "name or service not known" in err or "temporary failure in name resolution" in err:
                out["forward_failure_reason"] = "dns"
            elif "connection refused" in err:
                out["forward_failure_reason"] = "connection_refused"
            else:
                out["forward_failure_reason"] = "other"
    elif looks_recon:
        # Skip forward during backfill by default, and skip duplicates even when enabled.
        out["forward_skipped"] += 1

    return out


def sync_recon_ingest_report(msg_content: str):
    """
    Forward a raw spy/attack report to recon-hub ingest API.
    """
    if not RECON_INGEST_ENABLED:
        return {"ok": False, "disabled": True}
    if not RECON_INGEST_URL:
        return {"ok": False, "disabled": True, "reason": "missing RECON_INGEST_URL"}

    payload = json.dumps({"raw_text": msg_content}).encode("utf-8")
    req = urllib.request.Request(
        RECON_INGEST_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=RECON_INGEST_TIMEOUT) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            try:
                data = json.loads(body) if body else {}
            except Exception:
                data = {"raw": body}
            return {"ok": True, "status": getattr(resp, "status", 200), "data": data}
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            body = ""
        return {"ok": False, "status": int(getattr(e, "code", 0) or 0), "error": body or str(e)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def build_calc_link_from_ingest_data(data: dict) -> str | None:
    """
    Build calc deep-link using recon-hub stored spy report id/kingdom.
    """
    try:
        if not RECON_CALC_BASE_URL:
            return None
        if not isinstance(data, dict):
            return None
        if str(data.get("report_type") or "").lower() != "spy":
            return None
        stored = data.get("stored") or {}
        report_id = stored.get("id")
        parsed = data.get("parsed") or {}
        kingdom = parsed.get("target")
        if report_id is None:
            return None
        q = f"?report_id={int(report_id)}"
        if kingdom:
            q += "&kingdom=" + urllib.parse.quote(str(kingdom))
        return RECON_CALC_BASE_URL + q
    except Exception:
        return None


def truncate_for_discord(s: str, limit: int = 1800) -> str:
    s = s or ""
    if len(s) <= limit:
        return s
    return s[:limit] + "\n…(truncated)…"


def split_for_discord(s: str, limit: int = 1900) -> list[str]:
    s = (s or "").strip()
    if not s:
        return []
    if len(s) <= limit:
        return [s]

    chunks = []
    cur = ""
    for line in s.splitlines(True):
        if len(cur) + len(line) <= limit:
            cur += line
            continue
        if cur:
            chunks.append(cur.rstrip("\n"))
            cur = ""
        while len(line) > limit:
            chunks.append(line[:limit])
            line = line[limit:]
        cur += line
    if cur:
        chunks.append(cur.rstrip("\n"))
    return chunks


def extract_discord_message_texts(msg: discord.Message) -> list[str]:
    """
    Collect candidate report text from message content and embeds.
    This helps when users/bots paste reports as embed descriptions.
    """
    texts = []
    if (msg.content or "").strip():
        texts.append(msg.content.strip())

    for emb in (msg.embeds or []):
        parts = []
        if getattr(emb, "title", None):
            parts.append(str(emb.title))
        if getattr(emb, "description", None):
            parts.append(str(emb.description))
        for f in (getattr(emb, "fields", None) or []):
            try:
                if getattr(f, "name", None):
                    parts.append(str(f.name))
                if getattr(f, "value", None):
                    parts.append(str(f.value))
            except Exception:
                continue
        footer = getattr(emb, "footer", None)
        if footer and getattr(footer, "text", None):
            parts.append(str(footer.text))

        s = "\n".join([p for p in parts if (p or "").strip()]).strip()
        if s:
            texts.append(s)

    # De-dup preserve order.
    seen = set()
    out = []
    for t in texts:
        k = t.strip()
        if not k:
            continue
        hk = hashlib.sha1(k.encode("utf-8", errors="ignore")).hexdigest()
        if hk in seen:
            continue
        seen.add(hk)
        out.append(k)
    return out


def parse_track_day_arg(arg: str | None) -> datetime:
    """
    Returns start-of-day UTC for !track.
    Supports: None/today/yesterday/YYYY-MM-DD.
    """
    now = now_utc()
    token = (arg or "").strip().lower()
    if not token or token == "today":
        return datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    if token == "yesterday":
        d = now - timedelta(days=1)
        return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    parsed = datetime.strptime(token, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return datetime(parsed.year, parsed.month, parsed.day, tzinfo=timezone.utc)


async def send_error(guild: discord.Guild, msg: str, tb: str | None = None):
    """Send safe, truncated error logs to your error channel + log full stack to console."""
    try:
        ch = get_updates_channel(guild, None)
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
        # Handle legacy schema where player_tech may be a TABLE.
        cur.execute("SELECT to_regclass('player_tech')::text AS reg;")
        reg = cur.fetchone()
        reg_name = (reg or {}).get("reg")
        if reg_name:
            cur.execute(
                """
                SELECT c.relkind
                FROM pg_class c
                WHERE c.oid = to_regclass('player_tech');
                """
            )
            rk_row = cur.fetchone() or {}
            relkind = rk_row.get("relkind")
            if relkind in ("r", "p"):
                cur.execute("DROP TABLE IF EXISTS player_tech;")
            elif relkind in ("v", "m"):
                cur.execute("DROP VIEW IF EXISTS player_tech;")
            else:
                # Fallback for uncommon relation types.
                cur.execute("DROP VIEW IF EXISTS player_tech;")
                cur.execute("DROP TABLE IF EXISTS player_tech;")
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

        cur.execute("""
        CREATE TABLE IF NOT EXISTS attack_reports (
            id SERIAL PRIMARY KEY,
            attacker TEXT,
            defender TEXT,
            attack_result TEXT,
            land_taken INTEGER,
            settlements_lost_count INTEGER NOT NULL DEFAULT 0,
            settlements_lost TEXT,
            reported_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL,
            raw TEXT,
            raw_text TEXT,
            raw_gz BYTEA,
            report_hash TEXT UNIQUE,
            source_message_id BIGINT,
            source_channel_id BIGINT
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS troop_movements (
            id SERIAL PRIMARY KEY,
            owner_kingdom TEXT NOT NULL,
            target_kingdom TEXT,
            unit_name TEXT NOT NULL,
            units_sent INTEGER NOT NULL,
            departed_at TIMESTAMPTZ NOT NULL,
            expected_return_at TIMESTAMPTZ NOT NULL,
            status TEXT NOT NULL DEFAULT 'out',
            source_attack_report_id INTEGER REFERENCES attack_reports(id),
            source_message_id BIGINT,
            source_channel_id BIGINT,
            season_at_departure TEXT,
            note TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS market_transactions (
            id SERIAL PRIMARY KEY,
            report_id INTEGER NOT NULL REFERENCES spy_reports(id) ON DELETE CASCADE,
            captured_at TIMESTAMPTZ NOT NULL,
            line_no INTEGER NOT NULL,
            tx_type TEXT,
            buyer_kingdom TEXT,
            seller_kingdom TEXT,
            partner_kingdom TEXT,
            resource TEXT,
            quantity INTEGER,
            gold_amount BIGINT,
            tx_time_text TEXT,
            raw_line TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(report_id, line_no)
        );
        """)

        # Rankings baseline state for NW jump detection.
        cur.execute("""
        CREATE TABLE IF NOT EXISTS kingdom_rankings_state (
            world_id INTEGER NOT NULL,
            kingdom_id BIGINT NOT NULL,
            kingdom_name TEXT,
            rank_pos INTEGER,
            networth BIGINT,
            pie_active BOOLEAN NOT NULL DEFAULT FALSE,
            pie_signature TEXT,
            pie_label TEXT,
            updated_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (world_id, kingdom_id)
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS kingdom_rankings_history (
            id BIGSERIAL PRIMARY KEY,
            world_id INTEGER NOT NULL,
            kingdom_id BIGINT NOT NULL,
            kingdom_name TEXT,
            lookup_key TEXT NOT NULL,
            rank_pos INTEGER,
            networth BIGINT,
            pie_active BOOLEAN NOT NULL DEFAULT FALSE,
            pie_signature TEXT,
            pie_label TEXT,
            snapshot_at TIMESTAMPTZ NOT NULL
        );
        """)

        # Per-guild alert routing for NW jump alerts.
        cur.execute("""
        CREATE TABLE IF NOT EXISTS nw_jump_alert_subscriptions (
            guild_id BIGINT PRIMARY KEY,
            channel_id BIGINT NOT NULL,
            min_jump BIGINT NOT NULL DEFAULT 5000,
            enabled BOOLEAN NOT NULL DEFAULT TRUE,
            updated_at TIMESTAMPTZ NOT NULL
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS nw_jump_alert_channels (
            guild_id BIGINT NOT NULL,
            channel_id BIGINT NOT NULL,
            enabled BOOLEAN NOT NULL DEFAULT TRUE,
            updated_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (guild_id, channel_id)
        );
        """)

        # Migration-safe upgrades for older attack_reports schema versions.
        cur.execute("""
            SELECT column_name, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'attack_reports';
        """)
        attack_col_rows = cur.fetchall()
        attack_cols = {r["column_name"] for r in attack_col_rows}
        attack_nullable = {r["column_name"]: (str(r.get("is_nullable") or "").upper() == "YES") for r in attack_col_rows}

        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'kingdom_rankings_state';
            """
        )
        rankings_state_cols = {r["column_name"] for r in (cur.fetchall() or [])}

        if "attacker" not in attack_cols:
            cur.execute("ALTER TABLE attack_reports ADD COLUMN attacker TEXT;")
        if "defender" not in attack_cols:
            cur.execute("ALTER TABLE attack_reports ADD COLUMN defender TEXT;")
        if "pie_active" not in rankings_state_cols:
            cur.execute("ALTER TABLE kingdom_rankings_state ADD COLUMN pie_active BOOLEAN NOT NULL DEFAULT FALSE;")
        if "pie_signature" not in rankings_state_cols:
            cur.execute("ALTER TABLE kingdom_rankings_state ADD COLUMN pie_signature TEXT;")
        if "pie_label" not in rankings_state_cols:
            cur.execute("ALTER TABLE kingdom_rankings_state ADD COLUMN pie_label TEXT;")
        if "attack_result" not in attack_cols:
            cur.execute("ALTER TABLE attack_reports ADD COLUMN attack_result TEXT;")
        if "land_taken" not in attack_cols:
            cur.execute("ALTER TABLE attack_reports ADD COLUMN land_taken INTEGER;")
        if "settlements_lost_count" not in attack_cols:
            cur.execute("ALTER TABLE attack_reports ADD COLUMN settlements_lost_count INTEGER DEFAULT 0;")
        if "settlements_lost" not in attack_cols:
            cur.execute("ALTER TABLE attack_reports ADD COLUMN settlements_lost TEXT;")
        if "reported_at" not in attack_cols:
            cur.execute("ALTER TABLE attack_reports ADD COLUMN reported_at TIMESTAMPTZ;")
        if "created_at" not in attack_cols:
            cur.execute("ALTER TABLE attack_reports ADD COLUMN created_at TIMESTAMPTZ;")
        if "raw" not in attack_cols:
            cur.execute("ALTER TABLE attack_reports ADD COLUMN raw TEXT;")
        if "raw_text" not in attack_cols:
            cur.execute("ALTER TABLE attack_reports ADD COLUMN raw_text TEXT;")
        if "raw_gz" not in attack_cols:
            cur.execute("ALTER TABLE attack_reports ADD COLUMN raw_gz BYTEA;")
        if "report_hash" not in attack_cols:
            cur.execute("ALTER TABLE attack_reports ADD COLUMN report_hash TEXT;")
        if "source_message_id" not in attack_cols:
            cur.execute("ALTER TABLE attack_reports ADD COLUMN source_message_id BIGINT;")
        if "source_channel_id" not in attack_cols:
            cur.execute("ALTER TABLE attack_reports ADD COLUMN source_channel_id BIGINT;")

        # Backfill from common legacy names if present.
        if "target_kingdom" in attack_cols and "defender" in attack_cols:
            cur.execute("""
                UPDATE attack_reports
                SET defender = COALESCE(defender, target_kingdom)
                WHERE defender IS NULL AND target_kingdom IS NOT NULL;
            """)
        if "target_kingdom" in attack_cols and not attack_nullable.get("target_kingdom", True):
            # Legacy schema had target_kingdom NOT NULL; our insert path doesn't always populate it.
            # Relax this so !attackbackfill and live ingest can store all valid formats.
            cur.execute("ALTER TABLE attack_reports ALTER COLUMN target_kingdom DROP NOT NULL;")

        if "result" in attack_cols and "attack_result" in attack_cols:
            cur.execute("""
                UPDATE attack_reports
                SET attack_result = COALESCE(attack_result, result::text)
                WHERE attack_result IS NULL;
            """)
        if "land" in attack_cols and "land_taken" in attack_cols:
            cur.execute("""
                UPDATE attack_reports
                SET land_taken = COALESCE(land_taken, land)
                WHERE land_taken IS NULL;
            """)
        if "captured_at" in attack_cols and "reported_at" in attack_cols:
            cur.execute("""
                UPDATE attack_reports
                SET reported_at = COALESCE(reported_at, captured_at)
                WHERE reported_at IS NULL;
            """)
        if "captured_at" in attack_cols and "created_at" in attack_cols:
            cur.execute("""
                UPDATE attack_reports
                SET created_at = COALESCE(created_at, captured_at)
                WHERE created_at IS NULL;
            """)
        if "raw_text" in attack_cols and "raw" in attack_cols:
            cur.execute("""
                UPDATE attack_reports
                SET raw_text = COALESCE(raw_text, raw, '')
                WHERE raw_text IS NULL;
            """)
        if "raw_text" in attack_cols and not attack_nullable.get("raw_text", True):
            # Legacy schema may enforce NOT NULL; ensure existing rows satisfy it.
            cur.execute("""
                UPDATE attack_reports
                SET raw_text = COALESCE(raw_text, '')
                WHERE raw_text IS NULL;
            """)
        cur.execute("""
            UPDATE attack_reports
            SET settlements_lost_count = COALESCE(settlements_lost_count, 0)
            WHERE settlements_lost_count IS NULL;
        """)
        cur.execute("""
            UPDATE attack_reports
            SET created_at = COALESCE(created_at, reported_at, NOW())
            WHERE created_at IS NULL;
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
        cur.execute("""
            CREATE INDEX IF NOT EXISTS attack_reports_created_at_idx
            ON attack_reports (created_at DESC, id DESC);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS attack_reports_defender_created_at_idx
            ON attack_reports (defender, created_at DESC, id DESC);
        """)
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS attack_reports_report_hash_uq
            ON attack_reports (report_hash);
        """)
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS attack_reports_source_message_id_uq
            ON attack_reports (source_message_id)
            WHERE source_message_id IS NOT NULL;
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS troop_movements_owner_out_idx
            ON troop_movements (owner_kingdom, status, departed_at DESC, expected_return_at ASC);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS troop_movements_return_due_idx
            ON troop_movements (status, expected_return_at ASC);
        """)
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS troop_movements_src_msg_unit_uq
            ON troop_movements (source_message_id, unit_name)
            WHERE source_message_id IS NOT NULL;
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS market_transactions_buyer_captured_idx
            ON market_transactions (buyer_kingdom, captured_at DESC, id DESC);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS market_transactions_seller_captured_idx
            ON market_transactions (seller_kingdom, captured_at DESC, id DESC);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS kingdom_rankings_state_updated_idx
            ON kingdom_rankings_state (updated_at DESC);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS kingdom_rankings_history_lookup_snapshot_idx
            ON kingdom_rankings_history (lookup_key, snapshot_at DESC, id DESC);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS kingdom_rankings_history_world_snapshot_idx
            ON kingdom_rankings_history (world_id, snapshot_at DESC, id DESC);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS nw_jump_alert_subscriptions_enabled_idx
            ON nw_jump_alert_subscriptions (enabled, updated_at DESC);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS nw_jump_alert_channels_enabled_idx
            ON nw_jump_alert_channels (guild_id, enabled, updated_at DESC);
        """)


def heal_sequences():
    with db_conn() as conn, conn.cursor() as cur:
        for table in ["spy_reports", "dp_sessions", "tech_index", "troop_snapshots", "attack_reports", "troop_movements", "market_transactions"]:
            cur.execute(
                f"SELECT setval(pg_get_serial_sequence('{table}','id'), "
                f"COALESCE((SELECT MAX(id) FROM {table}), 1), true);"
            )


def _kg_extract_rankings_rows(payload: dict) -> list[dict]:
    if not isinstance(payload, dict):
        return []
    for k in ("kingdoms", "Kingdoms", "rankings", "Rankings", "rows", "Rows", "data", "Data"):
        v = payload.get(k)
        if isinstance(v, list):
            return [x for x in v if isinstance(x, dict)]
    for v in payload.values():
        if isinstance(v, list) and v and all(isinstance(x, dict) for x in v[: min(5, len(v))]):
            return v
    return []


def _kg_extract_rankings_pie_state(row: dict) -> dict:
    if not isinstance(row, dict):
        return {"active": False, "signature": "", "label": ""}

    candidates = {}
    for raw_key, raw_value in row.items():
        if raw_value is None:
            continue
        key = str(raw_key or "").strip()
        if not key:
            continue
        key_norm = re.sub(r"[^a-z0-9]+", "", key.casefold())
        if not key_norm:
            continue
        if (
            "pie" in key_norm
            or key_norm in {
                "protectionstatus",
                "landprotectionstatus",
                "landstatus",
                "hitstatus",
                "kingdomstatus",
            }
            or ("protect" in key_norm and ("status" in key_norm or "land" in key_norm))
        ):
            candidates[key] = raw_value

    if not candidates:
        return {"active": False, "signature": "", "label": ""}

    def _is_active_value(value) -> bool:
        if value is None:
            return False
        if isinstance(value, bool):
            return bool(value)
        if isinstance(value, (int, float)):
            return float(value) > 0
        if isinstance(value, (list, tuple, set, dict)):
            return len(value) > 0

        txt = str(value or "").strip()
        if not txt:
            return False
        low = txt.casefold()
        if low in {"0", "0.0", "false", "none", "null", "[]", "{}", "clear", "empty", "n/a", "na", "no pie", "nopie"}:
            return False
        num = _safe_int_or_none(txt.replace(",", ""))
        if num is not None:
            return int(num) > 0
        return True

    active = any(_is_active_value(v) for v in candidates.values())
    signature = json.dumps(candidates, sort_keys=True, separators=(",", ":"), default=str)
    label_parts = []
    for key, value in sorted(candidates.items()):
        if isinstance(value, (dict, list, tuple, set)):
            pretty = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
        else:
            pretty = str(value).strip()
        if len(pretty) > 40:
            pretty = pretty[:37] + "..."
        label_parts.append(f"{key}={pretty}")

    return {
        "active": bool(active),
        "signature": signature,
        "label": "; ".join(label_parts[:3]),
    }


def _kg_normalize_rankings_rows(rows: list[dict]) -> list[dict]:
    out = []
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        kid = _safe_int_or_none(_dict_pick(r, "kingdomId", "KingdomId", "id", "Id"))
        if not kid:
            continue
        name = str(_dict_pick(r, "name", "Name", "kingdom", "Kingdom", "kingdomName", "KingdomName") or "").strip()
        nw = _safe_int_or_none(_dict_pick(r, "networth", "Networth", "netWorth", "NetWorth", "net_worth"))
        if nw is None:
            continue
        rank = _safe_int_or_none(_dict_pick(r, "rank", "Rank", "position", "Position", "ranking", "Ranking"))
        pie_state = _kg_extract_rankings_pie_state(r)
        out.append({
            "kingdom_id": int(kid),
            "kingdom_name": name or f"Kingdom #{kid}",
            "rank": int(rank) if rank else None,
            "networth": int(nw),
            "pie_active": bool(pie_state.get("active")),
            "pie_signature": str(pie_state.get("signature") or ""),
            "pie_label": str(pie_state.get("label") or ""),
        })
    out.sort(key=lambda x: (x.get("rank") is None, int(x.get("rank") or 10**9), -int(x.get("networth") or 0), str(x.get("kingdom_name") or "")))
    for i, row in enumerate(out, start=1):
        if not row.get("rank"):
            row["rank"] = i
    return out


def _kg_sort_normalized_rankings_rows(rows: list[dict]) -> list[dict]:
    out = [dict(r) for r in (rows or []) if isinstance(r, dict)]
    out.sort(key=lambda x: (x.get("rank") is None, int(x.get("rank") or 10**9), -int(x.get("networth") or 0), str(x.get("kingdom_name") or "")))
    for i, row in enumerate(out, start=1):
        if not row.get("rank"):
            row["rank"] = i
    return out


def fetch_world_kingdom_rankings_debug() -> tuple[list[dict], dict]:
    meta = {
        "auth_mode": "none",
        "attempts": [],
        "configured_continent_id": int(KG_GAME_RANKINGS_CONTINENT_ID or -1),
        "target_rows": int(KG_GAME_RANKINGS_TARGET_ROWS or 100),
        "page_size": int(KG_GAME_RANKINGS_PAGE_SIZE or 20),
    }

    auth = _kg_get_auth(force_refresh=False)
    if not auth:
        meta["error"] = "missing_auth"
        return [], meta

    def _attempt_with_auth(auth_row: dict, label: str):
        continent_candidates = [int(KG_GAME_RANKINGS_CONTINENT_ID or -1), 1, -1]
        seen = set()
        ordered = []
        for c in continent_candidates:
            if c in seen:
                continue
            seen.add(c)
            ordered.append(c)

        acct_str = str(auth_row.get("account_id") or 0)
        acct_int = int(auth_row.get("account_id") or 0)
        token = str(auth_row.get("token") or "")
        kid = int(auth_row.get("search_kingdom_id") or 0)

        def _variants(cont_id: int) -> list[tuple[str, dict]]:
            return [
                ("str_acct+startNumber0", {
                    "accountId": acct_str, "token": token, "kingdomId": kid,
                    "continentId": int(cont_id), "startNumber": 0,
                }),
                ("int_acct+startNumber0", {
                    "accountId": acct_int, "token": token, "kingdomId": kid,
                    "continentId": int(cont_id), "startNumber": 0,
                }),
                ("str_acct+no_startNumber", {
                    "accountId": acct_str, "token": token, "kingdomId": kid,
                    "continentId": int(cont_id),
                }),
                ("str_acct+startNumber1", {
                    "accountId": acct_str, "token": token, "kingdomId": kid,
                    "continentId": int(cont_id), "startNumber": 1,
                }),
                # Legacy fallback in case older servers still expect startingRank.
                ("str_acct+startingRank0", {
                    "accountId": acct_str, "token": token, "kingdomId": kid,
                    "continentId": int(cont_id), "startingRank": 0,
                }),
            ]

        page_size = max(1, int(KG_GAME_RANKINGS_PAGE_SIZE or 20))
        target_rows = max(page_size, int(KG_GAME_RANKINGS_TARGET_ROWS or 100))

        def _to_int_allow_zero(v):
            try:
                if v is None:
                    return None
                return int(str(v).strip())
            except Exception:
                return None

        for cont_id in ordered:
            for variant_name, payload in _variants(cont_id):
                start_seed = _to_int_allow_zero(payload.get("startNumber"))
                start_numbers = [None]
                if start_seed is not None:
                    pages = max(1, ceil(float(target_rows) / float(page_size)))
                    start_numbers = [int(start_seed + (i * page_size)) for i in range(pages)]

                merged_by_kingdom = {}
                ret_val = None
                ret_str = ""
                for start_number in start_numbers:
                    req_payload = dict(payload)
                    if start_number is None:
                        req_payload.pop("startNumber", None)
                    else:
                        req_payload["startNumber"] = int(start_number)

                    data, req_dbg = _kg_webservice_post_debug("Kingdoms", "GetKingdomRankings", req_payload)
                    data = data or {}
                    rows = _kg_extract_rankings_rows(data)
                    norm = _kg_normalize_rankings_rows(rows)
                    if ret_val is None:
                        ret_val = _safe_int_or_none(_dict_pick(data, "ReturnValue", "returnValue"))
                        ret_str = str(_dict_pick(data, "ReturnString", "returnString") or "").strip()
                    meta["attempts"].append({
                        "label": label,
                        "variant": variant_name,
                        "continent_id": int(cont_id),
                        "start_number": start_number,
                        "rows": int(len(norm or [])),
                        "http_status": req_dbg.get("status"),
                        "http_reason": req_dbg.get("reason"),
                        "body_preview": str(req_dbg.get("body_preview") or "")[:120],
                    })

                    for row in norm:
                        kid2 = int(row.get("kingdom_id") or 0)
                        if kid2 <= 0:
                            continue
                        prev = merged_by_kingdom.get(kid2)
                        if prev is None:
                            merged_by_kingdom[kid2] = row
                            continue
                        prev_rank = _safe_int_or_none(prev.get("rank"))
                        new_rank = _safe_int_or_none(row.get("rank"))
                        if prev_rank is None and new_rank is not None:
                            merged_by_kingdom[kid2] = row
                        elif prev_rank is not None and new_rank is not None and new_rank < prev_rank:
                            merged_by_kingdom[kid2] = row

                    if not norm:
                        break

                merged = _kg_sort_normalized_rankings_rows(list(merged_by_kingdom.values()))
                if merged:
                    merged = merged[:target_rows]
                    meta["auth_mode"] = str(auth_row.get("auth_mode") or "none")
                    meta["continent_id_used"] = int(cont_id)
                    meta["variant_used"] = variant_name
                    meta["return_value"] = ret_val
                    meta["return_string"] = ret_str
                    meta["rows_collected"] = int(len(merged))
                    return merged
        return []

    rows = _attempt_with_auth(auth, "cached_auth")
    if rows:
        return rows, meta

    # Static token likely expired/invalid (KG returns HTTP 500 on bad tokens).
    # Fall back to a fresh email/password login token if credentials are configured.
    login_auth = _kg_get_login_auth(force_refresh=True)
    if not login_auth:
        meta["error"] = "static_token_failed_and_no_login_credentials"
        meta["login_configured"] = bool(KG_GAME_EMAIL and KG_GAME_PASSWORD)
        meta["auth_mode"] = str(auth.get("auth_mode") or "none")
        return [], meta
    rows = _attempt_with_auth(login_auth, "login_auth")
    if rows:
        return rows, meta
    meta["auth_mode"] = str(login_auth.get("auth_mode") or "none")
    return [], meta


def fetch_world_kingdom_rankings() -> list[dict]:
    rows, _meta = fetch_world_kingdom_rankings_debug()
    return rows


def sync_upsert_nw_jump_subscription(guild_id: int, channel_id: int, min_jump: int, enabled: bool):
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO nw_jump_alert_subscriptions (guild_id, channel_id, min_jump, enabled, updated_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (guild_id) DO UPDATE
            SET channel_id=EXCLUDED.channel_id,
                min_jump=EXCLUDED.min_jump,
                enabled=EXCLUDED.enabled,
                updated_at=EXCLUDED.updated_at;
            """,
            (int(guild_id), int(channel_id), max(1, int(min_jump)), bool(enabled), now_utc()),
        )


def sync_add_nw_jump_channel(guild_id: int, channel_id: int):
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO nw_jump_alert_channels (guild_id, channel_id, enabled, updated_at)
            VALUES (%s, %s, TRUE, %s)
            ON CONFLICT (guild_id, channel_id) DO UPDATE
            SET enabled=TRUE, updated_at=EXCLUDED.updated_at;
            """,
            (int(guild_id), int(channel_id), now_utc()),
        )


def sync_remove_nw_jump_channel(guild_id: int, channel_id: int) -> int:
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE nw_jump_alert_channels
            SET enabled=FALSE, updated_at=%s
            WHERE guild_id=%s AND channel_id=%s;
            """,
            (now_utc(), int(guild_id), int(channel_id)),
        )
        return int(cur.rowcount or 0)


def sync_get_nw_jump_channels(guild_id: int) -> list[int]:
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT channel_id
            FROM nw_jump_alert_channels
            WHERE guild_id=%s AND enabled=TRUE
            ORDER BY channel_id ASC;
            """,
            (int(guild_id),),
        )
        return [int(r.get("channel_id") or 0) for r in (cur.fetchall() or []) if int(r.get("channel_id") or 0) > 0]


def sync_get_nw_jump_subscription(guild_id: int):
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT guild_id, channel_id, min_jump, enabled, updated_at
            FROM nw_jump_alert_subscriptions
            WHERE guild_id=%s
            LIMIT 1;
            """,
            (int(guild_id),),
        )
        return cur.fetchone()


def sync_disable_nw_jump_subscription(guild_id: int):
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE nw_jump_alert_subscriptions
            SET enabled=FALSE, updated_at=%s
            WHERE guild_id=%s;
            """,
            (now_utc(), int(guild_id)),
        )
        return int(cur.rowcount or 0)


def sync_get_enabled_nw_jump_subscriptions() -> list[dict]:
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                s.guild_id,
                s.channel_id,
                s.min_jump,
                s.enabled,
                COALESCE(array_agg(c.channel_id) FILTER (WHERE c.enabled=TRUE), '{}') AS extra_channel_ids
            FROM nw_jump_alert_subscriptions s
            LEFT JOIN nw_jump_alert_channels c ON c.guild_id=s.guild_id
            WHERE s.enabled=TRUE
            GROUP BY s.guild_id, s.channel_id, s.min_jump, s.enabled;
            """
        )
        return cur.fetchall() or []


def _normalize_phone_number(s: str) -> str:
    txt = str(s or "").strip()
    if not txt:
        return ""
    # Keep a leading '+' and digits; drop other characters.
    out = []
    for i, ch in enumerate(txt):
        if ch.isdigit():
            out.append(ch)
        elif ch == "+" and i == 0:
            out.append(ch)
    return "".join(out)


def _normalize_watch_target(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "").strip().casefold())


def _parse_alert_sms_watchlist() -> dict[str, set[str]]:
    """
    Env format:
      ALERT_SMS_WATCHLIST="+15551234567=Magic Dude|Northeast|Galileo; +15557654321=123|456"
    Targets can be kingdom names or kingdom IDs.
    """
    raw = str(ALERT_SMS_WATCHLIST or "").strip()
    if not raw:
        return {}

    out = {}
    for entry in raw.replace("\n", ";").split(";"):
        part = str(entry or "").strip()
        if not part or "=" not in part:
            continue
        phone_raw, targets_raw = part.split("=", 1)
        phone = _normalize_phone_number(phone_raw)
        if not phone:
            continue
        targets = set()
        for token in str(targets_raw or "").replace(",", "|").split("|"):
            t = _normalize_watch_target(token)
            if t:
                targets.add(t)
        if not targets:
            continue
        out[phone] = targets
    return out


def _get_alert_sms_recipients() -> list[str]:
    nums = []
    for piece in str(ALERT_SMS_TO or "").replace(";", ",").split(","):
        n = _normalize_phone_number(piece)
        if n:
            nums.append(n)
    for n in _parse_alert_sms_watchlist().keys():
        if n:
            nums.append(n)
    deduped = []
    seen = set()
    for n in nums:
        if n in seen:
            continue
        seen.add(n)
        deduped.append(n)
    lim = max(1, int(ALERT_SMS_MAX_PER_ALERT or 10))
    return deduped[:lim]


def _twilio_auth_credentials() -> tuple[str, str] | None:
    api_key_sid = str(ALERT_SMS_TWILIO_API_KEY_SID or "").strip()
    api_key_secret = str(ALERT_SMS_TWILIO_API_KEY_SECRET or "").strip()
    if api_key_sid and api_key_secret:
        return api_key_sid, api_key_secret

    sid = str(ALERT_SMS_TWILIO_ACCOUNT_SID or "").strip()
    token = str(ALERT_SMS_TWILIO_AUTH_TOKEN or "").strip()
    if sid and token:
        return sid, token
    return None


def _event_matches_sms_watch(event: dict, targets: set[str]) -> bool:
    if not targets:
        return False
    kid = _safe_int_or_none(event.get("kingdom_id"))
    name = _normalize_watch_target(event.get("kingdom_name"))
    for t in targets:
        if not t:
            continue
        if t.isdigit() and kid and int(t) == int(kid):
            return True
        if name and t == name:
            return True
    return False


def _build_nw_jump_sms_text(events: list[dict]) -> str:
    top = list(events or [])[:5]
    sms_lines = [f"KG2 NW jump alert ({len(events or [])} hits)"]
    for e in top:
        delta_v = int(e.get("delta") or 0)
        sign = "+" if delta_v >= 0 else "-"
        sms_lines.append(
            f"{e.get('kingdom_name')}: {sign}{fmt_int(abs(delta_v))} ({fmt_int(e.get('old_networth'))}->{fmt_int(e.get('new_networth'))})"
        )
    if len(events or []) > len(top):
        sms_lines.append(f"+{len(events) - len(top)} more")
    return " | ".join(sms_lines)


def _twilio_sms_configured() -> bool:
    return bool(
        ALERT_SMS_ENABLED
        and ALERT_SMS_TWILIO_ACCOUNT_SID
        and _twilio_auth_credentials()
        and ALERT_SMS_TWILIO_FROM
        and _get_alert_sms_recipients()
    )


def _twilio_send_sms_sync(message: str, recipients_override: list[str] | None = None) -> dict:
    """
    Sends SMS alerts via Twilio REST API (sync; call from thread).
    Returns summary counters and first error string.
    """
    recipients = recipients_override if recipients_override is not None else _get_alert_sms_recipients()
    if not _twilio_sms_configured():
        return {"ok": False, "sent": 0, "attempted": len(recipients), "error": "sms_not_configured"}

    account_sid = str(ALERT_SMS_TWILIO_ACCOUNT_SID or "").strip()
    creds = _twilio_auth_credentials()
    if not account_sid or not creds:
        return {"ok": False, "sent": 0, "attempted": len(recipients), "error": "missing_twilio_credentials"}
    user, secret = creds
    from_num = _normalize_phone_number(ALERT_SMS_TWILIO_FROM)
    if not from_num:
        return {"ok": False, "sent": 0, "attempted": len(recipients), "error": "invalid_from_number"}

    url = f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json"
    auth = base64.b64encode(f"{user}:{secret}".encode("utf-8")).decode("ascii")
    headers = {
        "Authorization": f"Basic {auth}",
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "kg2bot/1.0",
    }

    body_text = str(message or "").strip()
    if len(body_text) > 1500:
        body_text = body_text[:1497] + "..."

    sent = 0
    first_err = None
    for to_num in recipients:
        payload = urllib.parse.urlencode({"To": to_num, "From": from_num, "Body": body_text}).encode("utf-8")
        try:
            req = urllib.request.Request(url, data=payload, method="POST", headers=headers)
            with urllib.request.urlopen(req, timeout=max(2.0, float(KG_GAME_API_TIMEOUT))) as resp:
                if int(getattr(resp, "status", 0) or 0) in (200, 201):
                    sent += 1
                else:
                    if first_err is None:
                        first_err = f"twilio_http_{getattr(resp, 'status', 0)}"
        except urllib.error.HTTPError as e:
            if first_err is None:
                first_err = f"twilio_http_{int(getattr(e, 'code', 0) or 0)}"
        except Exception as e:
            if first_err is None:
                first_err = e.__class__.__name__

    return {
        "ok": sent > 0,
        "sent": int(sent),
        "attempted": int(len(recipients)),
        "error": first_err,
    }


def sync_get_nw_rankings_state_stats(world_id: int) -> dict:
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                (SELECT COUNT(*)::int FROM kingdom_rankings_state WHERE world_id=%s) AS state_count,
                (SELECT MAX(updated_at) FROM kingdom_rankings_state WHERE world_id=%s) AS state_last_updated,
                (SELECT COUNT(*)::int FROM kingdom_rankings_history WHERE world_id=%s) AS history_count,
                (SELECT MAX(snapshot_at) FROM kingdom_rankings_history WHERE world_id=%s) AS history_last_snapshot
            """,
            (int(world_id), int(world_id), int(world_id), int(world_id)),
        )
        row = cur.fetchone() or {}
        return {
            "state_count": int(row.get("state_count") or 0),
            "state_last_updated": row.get("state_last_updated"),
            "history_count": int(row.get("history_count") or 0),
            "history_last_snapshot": row.get("history_last_snapshot"),
        }


def sync_detect_rankings_alerts(world_id: int, rows: list[dict], default_threshold: int) -> dict:
    if not rows:
        return {"nw_events": [], "pie_events": []}

    threshold = max(1, int(default_threshold or 5000))
    now_ts = now_utc()
    row_by_id = {int(r["kingdom_id"]): r for r in rows if isinstance(r, dict) and r.get("kingdom_id")}
    kingdom_ids = list(row_by_id.keys())
    if not kingdom_ids:
        return {"nw_events": [], "pie_events": []}

    nw_events = []
    pie_events = []
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT kingdom_id, kingdom_name, rank_pos, networth, pie_active, pie_signature, pie_label
            FROM kingdom_rankings_state
            WHERE world_id=%s AND kingdom_id = ANY(%s);
            """,
            (int(world_id), kingdom_ids),
        )
        prev = {int(r["kingdom_id"]): r for r in (cur.fetchall() or [])}

        for kid, current in row_by_id.items():
            old = prev.get(kid)
            if not old:
                continue
            old_nw = _safe_int_or_none(old.get("networth"))
            new_nw = _safe_int_or_none(current.get("networth"))
            if old_nw is None or new_nw is None:
                continue
            delta = int(new_nw) - int(old_nw)
            if abs(delta) >= threshold:
                nw_events.append({
                    "kingdom_id": kid,
                    "kingdom_name": str(current.get("kingdom_name") or old.get("kingdom_name") or f"Kingdom #{kid}"),
                    "old_networth": int(old_nw),
                    "new_networth": int(new_nw),
                    "delta": int(delta),
                    "old_rank": _safe_int_or_none(old.get("rank_pos")),
                    "new_rank": _safe_int_or_none(current.get("rank")),
                    "detected_at": now_ts,
                })

            old_pie_active = bool(old.get("pie_active"))
            new_pie_active = bool(current.get("pie_active"))
            old_pie_signature = str(old.get("pie_signature") or "")
            new_pie_signature = str(current.get("pie_signature") or "")
            if KG_GAME_PIE_ALERTS_ENABLED and new_pie_active and new_pie_signature and new_pie_signature != old_pie_signature:
                pie_events.append({
                    "kingdom_id": kid,
                    "kingdom_name": str(current.get("kingdom_name") or old.get("kingdom_name") or f"Kingdom #{kid}"),
                    "rank": _safe_int_or_none(current.get("rank")) or _safe_int_or_none(old.get("rank_pos")),
                    "networth": int(new_nw),
                    "detected_at": now_ts,
                    "pie_label": str(current.get("pie_label") or old.get("pie_label") or "").strip(),
                    "previous_pie_label": str(old.get("pie_label") or "").strip(),
                    "event_kind": "first_seen" if not old_pie_active else "changed",
                })

        upsert_rows = [
            (
                int(world_id),
                int(r["kingdom_id"]),
                str(r.get("kingdom_name") or "").strip() or None,
                int(r.get("rank") or 0) if r.get("rank") else None,
                int(r.get("networth") or 0),
                bool(r.get("pie_active")),
                str(r.get("pie_signature") or "") or None,
                str(r.get("pie_label") or "") or None,
                now_ts,
            )
            for r in row_by_id.values()
        ]
        history_rows = [
            (
                int(world_id),
                int(r["kingdom_id"]),
                str(r.get("kingdom_name") or "").strip() or None,
                normalize_kingdom_lookup_key(r.get("kingdom_name")),
                int(r.get("rank") or 0) if r.get("rank") else None,
                int(r.get("networth") or 0),
                bool(r.get("pie_active")),
                str(r.get("pie_signature") or "") or None,
                str(r.get("pie_label") or "") or None,
                now_ts,
            )
            for r in row_by_id.values()
        ]
        cur.executemany(
            """
            INSERT INTO kingdom_rankings_state (
                world_id, kingdom_id, kingdom_name, rank_pos, networth,
                pie_active, pie_signature, pie_label, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (world_id, kingdom_id) DO UPDATE
            SET kingdom_name=EXCLUDED.kingdom_name,
                rank_pos=EXCLUDED.rank_pos,
                networth=EXCLUDED.networth,
                pie_active=EXCLUDED.pie_active,
                pie_signature=EXCLUDED.pie_signature,
                pie_label=EXCLUDED.pie_label,
                updated_at=EXCLUDED.updated_at;
            """,
            upsert_rows,
        )
        cur.executemany(
            """
            INSERT INTO kingdom_rankings_history (
                world_id, kingdom_id, kingdom_name, lookup_key, rank_pos, networth,
                pie_active, pie_signature, pie_label, snapshot_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            history_rows,
        )

    nw_events.sort(key=lambda e: (-int(e.get("delta") or 0), str(e.get("kingdom_name") or "")))
    pie_events.sort(key=lambda e: (0 if str(e.get("event_kind") or "") == "first_seen" else 1, str(e.get("kingdom_name") or "")))
    return {"nw_events": nw_events, "pie_events": pie_events}


def sync_detect_rankings_nw_jumps(world_id: int, rows: list[dict], default_threshold: int) -> list[dict]:
    return list((sync_detect_rankings_alerts(world_id, rows, default_threshold) or {}).get("nw_events") or [])


async def send_nw_jump_alerts(events: list[dict]):
    if not events:
        return

    subs = await run_db(sync_get_enabled_nw_jump_subscriptions)
    if not subs:
        if not NW_JUMP_ALERT_SILENT_NO_SUBS:
            logging.info("NW jump detected but no enabled subscriptions: events=%s", len(events))
        return

    by_guild = {}
    for s in subs:
        gid = int(s.get("guild_id") or 0)
        if gid > 0:
            by_guild[gid] = s

    for guild in bot.guilds:
        sub = by_guild.get(int(guild.id))
        if not sub:
            continue
        min_jump = max(1, int(sub.get("min_jump") or NW_JUMP_ALERT_DEFAULT_THRESHOLD))
        hits = [e for e in events if abs(int(e.get("delta") or 0)) >= min_jump]
        if not hits:
            continue

        lines = [f"🚨 **NW Jump Alert** (threshold: `{fmt_int(min_jump)}`)"]
        for e in hits[:12]:
            delta_v = int(e.get("delta") or 0)
            sign = "+" if delta_v >= 0 else "-"
            old_rank = _safe_int_or_none(e.get("old_rank"))
            new_rank = _safe_int_or_none(e.get("new_rank"))
            rank_part = ""
            if old_rank and new_rank:
                rank_part = f" • rank {old_rank} → {new_rank}"
            lines.append(
                f"- **{e.get('kingdom_name')}** {sign}`{fmt_int(abs(delta_v))}` NW ({fmt_int(e.get('old_networth'))} → {fmt_int(e.get('new_networth'))}){rank_part}"
            )
            if OVEN_ESTIMATOR_ENABLED and delta_v > 0:
                try:
                    oven_est = await run_db(
                        sync_build_oven_estimate,
                        str(e.get("kingdom_name") or ""),
                        int(delta_v),
                        e.get("detected_at") or now_utc(),
                    )
                    lines.extend(format_oven_summary_lines(oven_est, limit=int(OVEN_MAX_ALERT_LINES or 3), compact=True))
                except Exception:
                    logging.exception("Failed to build oven estimate for NW jump kingdom=%s", e.get("kingdom_name"))
        extra = len(hits) - 12
        if extra > 0:
            lines.append(f"... +{extra} more")

        target_ids = []
        primary_id = int(sub.get("channel_id") or 0)
        if primary_id > 0:
            target_ids.append(primary_id)
        for c in (sub.get("extra_channel_ids") or []):
            try:
                cid = int(c)
                if cid > 0:
                    target_ids.append(cid)
            except Exception:
                continue
        dedup_ids = []
        seen = set()
        for cid in target_ids:
            if cid in seen:
                continue
            seen.add(cid)
            dedup_ids.append(cid)

        sent_any = False
        for cid in dedup_ids:
            ch = guild.get_channel(int(cid))
            if not (ch and can_send(ch, guild)):
                continue
            try:
                await ch.send("\n".join(lines))
                sent_any = True
            except Exception:
                logging.exception("Failed to send NW jump alert to guild=%s channel=%s", guild.id, cid)

        if not sent_any:
            ch = get_live_battle_channel(guild, None)
            if ch and can_send(ch, guild):
                try:
                    await ch.send("\n".join(lines))
                except Exception:
                    logging.exception("Failed to send NW jump alert to fallback channel guild=%s", guild.id)

    if _twilio_sms_configured():
        watch = _parse_alert_sms_watchlist()
        if watch:
            total_sent = 0
            total_attempted = 0
            first_err = None
            for to_num, targets in watch.items():
                matched = [e for e in events if _event_matches_sms_watch(e, targets)]
                if not matched:
                    continue
                sms_result = await asyncio.to_thread(_twilio_send_sms_sync, _build_nw_jump_sms_text(matched), [to_num])
                total_sent += int(sms_result.get("sent") or 0)
                total_attempted += int(sms_result.get("attempted") or 0)
                if not sms_result.get("ok") and first_err is None:
                    first_err = sms_result.get("error")
            if first_err:
                logging.warning(
                    "NW jump SMS watchlist dispatch had issues: sent=%s attempted=%s err=%s",
                    total_sent,
                    total_attempted,
                    first_err,
                )
        else:
            sms_result = await asyncio.to_thread(_twilio_send_sms_sync, _build_nw_jump_sms_text(events))
            if not sms_result.get("ok"):
                logging.warning(
                    "NW jump SMS dispatch had issues: sent=%s attempted=%s err=%s",
                    sms_result.get("sent"),
                    sms_result.get("attempted"),
                    sms_result.get("error"),
                )


async def send_rankings_pie_alerts(events: list[dict]):
    if not events:
        return

    subs = await run_db(sync_get_enabled_nw_jump_subscriptions)
    if not subs:
        return

    by_guild = {}
    for s in subs:
        gid = int(s.get("guild_id") or 0)
        if gid > 0:
            by_guild[gid] = s

    for guild in bot.guilds:
        sub = by_guild.get(int(guild.id))
        if not sub:
            continue

        lines = ["🥧 **Pie Alert**"]
        for e in events[:12]:
            rank_part = f"rank #{int(e.get('rank') or 0)} • " if e.get("rank") else ""
            nw_part = f"NW {fmt_int(e.get('networth'))}" if e.get("networth") is not None else "NW unknown"
            reason = "pie appeared" if str(e.get("event_kind") or "") == "first_seen" else "pie changed"
            lines.append(f"- **{e.get('kingdom_name')}** {rank_part}{nw_part} • {reason}")
            pie_label = str(e.get("pie_label") or "").strip()
            if pie_label:
                lines.append(f"  {pie_label}")

        extra = len(events) - 12
        if extra > 0:
            lines.append(f"... +{extra} more")

        target_ids = []
        primary_id = int(sub.get("channel_id") or 0)
        if primary_id > 0:
            target_ids.append(primary_id)
        for c in (sub.get("extra_channel_ids") or []):
            try:
                cid = int(c)
                if cid > 0:
                    target_ids.append(cid)
            except Exception:
                continue

        dedup_ids = []
        seen = set()
        for cid in target_ids:
            if cid in seen:
                continue
            seen.add(cid)
            dedup_ids.append(cid)

        sent_any = False
        for cid in dedup_ids:
            ch = guild.get_channel(int(cid))
            if not (ch and can_send(ch, guild)):
                continue
            try:
                await ch.send("\n".join(lines))
                sent_any = True
            except Exception:
                logging.exception("Failed to send pie alert to guild=%s channel=%s", guild.id, cid)

        if not sent_any:
            ch = get_live_battle_channel(guild, None)
            if ch and can_send(ch, guild):
                try:
                    await ch.send("\n".join(lines))
                except Exception:
                    logging.exception("Failed to send pie alert to fallback channel guild=%s", guild.id)


async def run_rankings_refresh_cycle(dispatch_alerts: bool = True) -> dict:
    rows, dbg = await asyncio.to_thread(fetch_world_kingdom_rankings_debug)
    await run_db(sync_meta_set, "nw_jump_last_poll_ts", str(int(time.time())))
    await run_db(sync_meta_set, "nw_jump_last_poll_rows", str(len(rows or [])))
    await run_db(sync_meta_set, "nw_jump_last_auth_mode", str(dbg.get("auth_mode") or ""))
    await run_db(sync_meta_set, "nw_jump_last_return_value", str(dbg.get("return_value") or ""))
    await run_db(sync_meta_set, "nw_jump_last_return_string", str(dbg.get("return_string") or ""))
    await run_db(sync_meta_set, "nw_jump_last_attempts", json.dumps(dbg.get("attempts") or []))

    alerts = {"nw_events": [], "pie_events": []}
    if rows:
        alerts = await run_db(
            sync_detect_rankings_alerts,
            int(KG_GAME_WORLD_ID or 1),
            rows,
            int(NW_JUMP_ALERT_DEFAULT_THRESHOLD or 5000),
        ) or {"nw_events": [], "pie_events": []}
        nw_events = list((alerts or {}).get("nw_events") or [])
        pie_events = list((alerts or {}).get("pie_events") or [])
        await run_db(sync_meta_set, "nw_jump_last_events", str(len(nw_events or [])))
        await run_db(sync_meta_set, "nw_jump_last_pie_events", str(len(pie_events or [])))
        await run_db(sync_meta_set, "nw_jump_last_ok_ts", str(int(time.time())))
        if dispatch_alerts:
            if nw_events:
                await send_nw_jump_alerts(nw_events)

    return {
        "rows": rows,
        "debug": dbg,
        "nw_events": list((alerts or {}).get("nw_events") or []),
        "pie_events": list((alerts or {}).get("pie_events") or []),
    }


async def nw_jump_alerts_loop():
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            await run_rankings_refresh_cycle(dispatch_alerts=True)
        except Exception:
            logging.exception("nw_jump_alerts_loop failed")
            try:
                await run_db(sync_meta_set, "nw_jump_last_error_ts", str(int(time.time())))
            except Exception:
                pass
        await asyncio.sleep(max(15, int(NW_JUMP_ALERT_POLL_SECONDS or 60)))


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


def sync_meta_set(key: str, value: str):
    with db_conn() as conn, conn.cursor() as cur:
        _meta_set(cur, str(key), str(value))


def sync_meta_get(key: str):
    with db_conn() as conn, conn.cursor() as cur:
        return _meta_get(cur, str(key))


def sync_fuzzy_kingdom(query: str):
    if not query:
        return None
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT DISTINCT kingdom FROM spy_reports WHERE kingdom IS NOT NULL;")
        names = [str(r["kingdom"]).strip() for r in cur.fetchall() if r.get("kingdom")]
    if not names:
        return None

    q_key = normalize_kingdom_lookup_key(query)
    if not q_key:
        return None
    by_key = {}
    for name in names:
        key = normalize_kingdom_lookup_key(name)
        if key and key not in by_key:
            by_key[key] = name

    # Exact normalized hit first so separators like _, -, and spaces all match.
    if q_key in by_key:
        return by_key[q_key]

    # Keep fuzzy fallback available for small typos, but avoid unrelated matches.
    match = difflib.get_close_matches(q_key, list(by_key.keys()), 1, 0.8)
    if not match:
        return None
    return by_key.get(match[0])


def sync_fuzzy_live_kingdom(query: str):
    if not query:
        return None
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            WITH names AS (
                SELECT kingdom AS name FROM spy_reports WHERE kingdom IS NOT NULL
                UNION
                SELECT kingdom_name AS name FROM kingdom_rankings_state WHERE kingdom_name IS NOT NULL
                UNION
                SELECT kingdom_name AS name FROM kingdom_rankings_history WHERE kingdom_name IS NOT NULL
            )
            SELECT DISTINCT name FROM names WHERE name IS NOT NULL;
            """
        )
        names = [str(r["name"]).strip() for r in cur.fetchall() if r.get("name")]
    if not names:
        return None

    q_key = normalize_kingdom_lookup_key(query)
    if not q_key:
        return None
    by_key = {}
    for name in names:
        key = normalize_kingdom_lookup_key(name)
        if key and key not in by_key:
            by_key[key] = name

    if q_key in by_key:
        return by_key[q_key]

    match = difflib.get_close_matches(q_key, list(by_key.keys()), 1, 0.8)
    if not match:
        return None
    return by_key.get(match[0])


def sync_get_live_kingdom_profile(kingdom_query: str, lookback_hours: int | None = None) -> dict:
    requested = str(kingdom_query or "").strip()
    if not requested:
        return {"ok": False, "reason": "missing_kingdom"}

    lookback_hours = max(1, min(168, int(lookback_hours or KINGDOM_LIVE_DEFAULT_LOOKBACK_HOURS or 1)))
    resolved = sync_fuzzy_live_kingdom(requested) or requested
    lookup_key = normalize_kingdom_lookup_key(resolved)
    now_ts = now_utc()
    cutoff = now_ts - timedelta(hours=lookback_hours)
    attack_hours = max(1, min(168, int(KINGDOM_LIVE_ATTACK_WINDOW_HOURS or 24)))

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT kingdom_id, kingdom_name, rank_pos, networth, pie_active, pie_signature, pie_label, updated_at
            FROM kingdom_rankings_state
            WHERE REGEXP_REPLACE(LOWER(BTRIM(COALESCE(kingdom_name, ''))), '[^a-z0-9]+', ' ', 'g')=%s
            ORDER BY updated_at DESC NULLS LAST, kingdom_id DESC
            LIMIT 1;
            """,
            (lookup_key,),
        )
        current = cur.fetchone()
        if not current:
            cur.execute(
                """
                SELECT kingdom_id, kingdom_name, rank_pos, networth, pie_active, pie_signature, pie_label,
                       snapshot_at AS updated_at
                FROM kingdom_rankings_history
                WHERE lookup_key=%s
                ORDER BY snapshot_at DESC, id DESC
                LIMIT 1;
                """,
                (lookup_key,),
            )
            current = cur.fetchone()
        if not current:
            return {
                "ok": False,
                "reason": "no_rankings_data",
                "kingdom": resolved,
                "lookback_hours": lookback_hours,
            }

        kingdom_name = str(current.get("kingdom_name") or resolved).strip() or resolved
        lookup_key = normalize_kingdom_lookup_key(kingdom_name)

        cur.execute(
            """
            SELECT kingdom_id, kingdom_name, rank_pos, networth, pie_active, pie_signature, pie_label,
                   snapshot_at AS updated_at
            FROM kingdom_rankings_history
            WHERE lookup_key=%s
              AND snapshot_at <= %s
            ORDER BY snapshot_at DESC, id DESC
            LIMIT 1;
            """,
            (lookup_key, normalize_to_utc(cutoff)),
        )
        baseline = cur.fetchone()

        cur.execute(
            """
            SELECT
                COUNT(*)::int AS poll_count,
                MIN(networth) AS min_networth,
                MAX(networth) AS max_networth,
                MIN(rank_pos) AS best_rank,
                MAX(rank_pos) AS worst_rank,
                MAX(snapshot_at) AS last_snapshot_at,
                SUM(CASE WHEN pie_active THEN 1 ELSE 0 END)::int AS pie_polls
            FROM kingdom_rankings_history
            WHERE lookup_key=%s
              AND snapshot_at >= %s;
            """,
            (lookup_key, normalize_to_utc(cutoff)),
        )
        window = cur.fetchone() or {}

        since_attacks = now_ts - timedelta(hours=attack_hours)
        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE LOWER(COALESCE(attacker, '')) = LOWER(%s))::int AS outgoing_hits,
                COUNT(*) FILTER (WHERE LOWER(COALESCE(defender, '')) = LOWER(%s))::int AS incoming_hits,
                COALESCE(SUM(CASE WHEN LOWER(COALESCE(attacker, '')) = LOWER(%s) THEN COALESCE(land_taken, 0) ELSE 0 END), 0)::bigint AS land_gained,
                COALESCE(SUM(CASE WHEN LOWER(COALESCE(defender, '')) = LOWER(%s) THEN COALESCE(land_taken, 0) ELSE 0 END), 0)::bigint AS land_lost
            FROM attack_reports
            WHERE COALESCE(reported_at, created_at) >= %s;
            """,
            (kingdom_name, kingdom_name, kingdom_name, kingdom_name, normalize_to_utc(since_attacks)),
        )
        attacks = cur.fetchone() or {}

    current_rank = _safe_int_or_none(current.get("rank_pos"))
    baseline_rank = _safe_int_or_none(baseline.get("rank_pos")) if baseline else None
    current_nw = _safe_int_or_none(current.get("networth"))
    baseline_nw = _safe_int_or_none(baseline.get("networth")) if baseline else None
    current_pie = bool(current.get("pie_active"))
    baseline_pie = bool(baseline.get("pie_active")) if baseline else None

    pie_change = None
    if baseline is not None:
        if current_pie and not baseline_pie:
            pie_change = "appeared"
        elif (not current_pie) and baseline_pie:
            pie_change = "cleared"
        elif current.get("pie_signature") != baseline.get("pie_signature"):
            pie_change = "changed"
        else:
            pie_change = "unchanged"

    spy = sync_get_latest_spy_for_kingdom(kingdom_name)
    battle = sync_build_battle_estimate(kingdom_name, now_ts) if KG_TROOP_TRACKING_ENABLED else None
    oven = sync_build_oven_estimate(kingdom_name, None, None) if OVEN_ESTIMATOR_ENABLED else None

    return {
        "ok": True,
        "kingdom": kingdom_name,
        "lookback_hours": lookback_hours,
        "current": {
            "kingdom_id": _safe_int_or_none(current.get("kingdom_id")),
            "rank": current_rank,
            "networth": current_nw,
            "pie_active": current_pie,
            "pie_label": str(current.get("pie_label") or "").strip(),
            "updated_at": current.get("updated_at"),
        },
        "baseline": {
            "rank": baseline_rank,
            "networth": baseline_nw,
            "pie_active": baseline_pie,
            "pie_label": str((baseline or {}).get("pie_label") or "").strip() if baseline else "",
            "updated_at": (baseline or {}).get("updated_at") if baseline else None,
        } if baseline else None,
        "window": {
            "poll_count": int(window.get("poll_count") or 0),
            "min_networth": _safe_int_or_none(window.get("min_networth")),
            "max_networth": _safe_int_or_none(window.get("max_networth")),
            "best_rank": _safe_int_or_none(window.get("best_rank")),
            "worst_rank": _safe_int_or_none(window.get("worst_rank")),
            "last_snapshot_at": window.get("last_snapshot_at"),
            "pie_polls": int(window.get("pie_polls") or 0),
        },
        "delta": {
            "networth": (int(current_nw) - int(baseline_nw)) if current_nw is not None and baseline_nw is not None else None,
            "rank": (int(current_rank) - int(baseline_rank)) if current_rank is not None and baseline_rank is not None else None,
            "pie_change": pie_change,
        },
        "attacks": {
            "hours": attack_hours,
            "outgoing_hits": int(attacks.get("outgoing_hits") or 0),
            "incoming_hits": int(attacks.get("incoming_hits") or 0),
            "land_gained": int(attacks.get("land_gained") or 0),
            "land_lost": int(attacks.get("land_lost") or 0),
        },
        "latest_spy": spy,
        "battle": battle if battle and battle.get("ok") else None,
        "oven": oven if oven and oven.get("ok") else None,
    }


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
    lookup_key = normalize_kingdom_lookup_key(kingdom)
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, kingdom, defense_power, castles, created_at, raw, raw_gz
            FROM spy_reports
            WHERE REGEXP_REPLACE(LOWER(BTRIM(COALESCE(kingdom, ''))), '[^a-z0-9]+', ' ', 'g')=%s
            ORDER BY created_at DESC NULLS LAST, id DESC
            LIMIT 1;
        """, (lookup_key,))
        return cur.fetchone()


def sync_get_latest_dp_spy_for_kingdom(kingdom: str):
    lookup_key = normalize_kingdom_lookup_key(kingdom)
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, kingdom, defense_power, castles, created_at, raw, raw_gz
            FROM spy_reports
            WHERE REGEXP_REPLACE(LOWER(BTRIM(COALESCE(kingdom, ''))), '[^a-z0-9]+', ' ', 'g')=%s AND defense_power IS NOT NULL AND defense_power > 0
            ORDER BY created_at DESC NULLS LAST, id DESC
            LIMIT 1;
        """, (lookup_key,))
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
    lookup_key = normalize_kingdom_lookup_key(kingdom)
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, kingdom, defense_power, castles, created_at
            FROM spy_reports
            WHERE REGEXP_REPLACE(LOWER(BTRIM(COALESCE(kingdom, ''))), '[^a-z0-9]+', ' ', 'g')=%s
            ORDER BY created_at DESC NULLS LAST, id DESC
            LIMIT %s;
        """, (lookup_key, int(limit)))
        return cur.fetchall()


def sync_get_spy_history_with_raw(kingdom: str, limit: int = 10):
    lookup_key = normalize_kingdom_lookup_key(kingdom)
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, kingdom, created_at, raw, raw_gz
            FROM spy_reports
            WHERE REGEXP_REPLACE(LOWER(BTRIM(COALESCE(kingdom, ''))), '[^a-z0-9]+', ' ', 'g')=%s
            ORDER BY created_at DESC NULLS LAST, id DESC
            LIMIT %s;
        """, (lookup_key, int(limit)))
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
        inserted += int(cur.rowcount or 0)
    return inserted


def sync_upsert_market_transactions(cur, report_id: int, captured_at, txs: list[dict]) -> int:
    if not report_id or not txs:
        return 0
    inserted = 0
    ts = captured_at or now_utc()
    for tx in txs:
        cur.execute(
            """
            INSERT INTO market_transactions (
                report_id, captured_at, line_no, tx_type, buyer_kingdom, seller_kingdom,
                partner_kingdom, resource, quantity, gold_amount, tx_time_text, raw_line
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (report_id, line_no) DO NOTHING;
            """,
            (
                int(report_id),
                ts,
                int(tx.get("line_no") or 0),
                (str(tx.get("tx_type") or "").lower() or None),
                (str(tx.get("buyer_kingdom") or "").strip() or None),
                (str(tx.get("seller_kingdom") or "").strip() or None),
                (str(tx.get("partner_kingdom") or "").strip() or None),
                (str(tx.get("resource") or "").strip() or None),
                int(tx.get("quantity") or 0),
                int(tx.get("gold_amount") or 0),
                (str(tx.get("tx_time_text") or "").strip() or None),
                (str(tx.get("raw_line") or "").strip() or None),
            ),
        )
        inserted += int(cur.rowcount or 0)
    return inserted


def sync_get_supply_summary(kingdom: str, since_utc: datetime, detail_limit: int = 120):
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              COALESCE(NULLIF(BTRIM(seller_kingdom), ''), 'Unknown') AS seller,
              COUNT(*)::int AS tx_count,
              COALESCE(SUM(quantity), 0)::bigint AS qty_sum,
              COALESCE(SUM(gold_amount), 0)::bigint AS gold_sum,
              COALESCE(SUM(CASE WHEN COALESCE(gold_amount, 0) = 0 THEN 1 ELSE 0 END), 0)::int AS zero_gold_count,
              MAX(captured_at) AS last_seen
            FROM market_transactions
            WHERE LOWER(BTRIM(COALESCE(buyer_kingdom, ''))) = LOWER(BTRIM(%s))
              AND captured_at >= %s
              AND BTRIM(COALESCE(seller_kingdom, '')) <> ''
            GROUP BY COALESCE(NULLIF(BTRIM(seller_kingdom), ''), 'Unknown')
            ORDER BY qty_sum DESC, tx_count DESC, seller ASC;
            """,
            (kingdom, since_utc),
        )
        summary = cur.fetchall() or []

        cur.execute(
            """
            SELECT
              report_id, captured_at, buyer_kingdom, seller_kingdom, resource,
              quantity, gold_amount, tx_time_text, raw_line
            FROM market_transactions
            WHERE LOWER(BTRIM(COALESCE(buyer_kingdom, ''))) = LOWER(BTRIM(%s))
              AND captured_at >= %s
              AND BTRIM(COALESCE(seller_kingdom, '')) <> ''
            ORDER BY captured_at DESC, report_id DESC, line_no ASC
            LIMIT %s;
            """,
            (kingdom, since_utc, int(detail_limit)),
        )
        details = cur.fetchall() or []

        return {"summary": summary, "details": details}


def sync_get_spy_reports_raw_since(kingdom: str, since_utc: datetime, limit: int = 300):
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, kingdom, created_at, raw, raw_gz
            FROM spy_reports
            WHERE LOWER(BTRIM(COALESCE(kingdom, ''))) = LOWER(BTRIM(%s))
              AND created_at >= %s
            ORDER BY created_at DESC NULLS LAST, id DESC
            LIMIT %s;
            """,
            (kingdom, since_utc, int(limit)),
        )
        return cur.fetchall() or []


def sync_is_premium_discord_user(discord_user_id: int | str) -> bool:
    uid = str(discord_user_id or "").strip()
    if not uid:
        return False
    try:
        with db_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.app_users') AS t;")
            reg = cur.fetchone() or {}
            if not reg.get("t"):
                return False
            cur.execute(
                """
                SELECT is_premium
                FROM public.app_users
                WHERE discord_user_id = %s
                LIMIT 1;
                """,
                (uid,),
            )
            row = cur.fetchone() or {}
            return bool(row.get("is_premium") or False)
    except Exception:
        return False


def get_live_battle_channel(guild: discord.Guild, fallback: discord.abc.GuildChannel | None = None):
    """
    Preferred channel for battle-tracker live updates.
    Falls back to the provided channel if configured channel is unavailable.
    """
    ch = None
    try:
        if LIVE_BATTLE_CHANNEL_ID > 0:
            ch = guild.get_channel(int(LIVE_BATTLE_CHANNEL_ID))
    except Exception:
        ch = None
    if ch and can_send(ch, guild):
        return ch
    if fallback and can_send(fallback, guild):
        return fallback
    return None


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
    market_txs = parse_market_transactions(msg_content, kingdom)

    should_save = bool(kingdom) and (
        (dp is not None and dp >= 1000) or
        (techs and len(techs) >= 1) or
        (sr_troops and len(sr_troops) >= 1) or
        (market_txs and len(market_txs) >= 1)
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
            if market_txs:
                sync_upsert_market_transactions(cur, int(row["id"]), row.get("created_at") or created_at_utc, market_txs)

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
            if market_txs:
                sync_upsert_market_transactions(cur, rep_id, created_at_utc, market_txs)

        return {"saved": True, "duplicate": True, "row": None}


def sync_store_attack_report(
    msg_content: str,
    created_at_utc: datetime,
    source_message_id: int | None = None,
    source_channel_id: int | None = None,
):
    """
    Stores attack report deduped by hash.
    Tracks attacker/defender/result/land/settlement-loss signals for !track.
    """
    d = parse_attack_details(msg_content)
    reported_at = coerce_report_time(
        d.get("reported_at"),
        created_at_utc,
        bool(d.get("reported_at_has_tz")),
    )
    d["reported_at"] = reported_at
    # Guard against non-attack content even if detection is permissive.
    ll = (msg_content or "").lower()
    has_attack_shape = bool(
        "attack report" in ll
        or "attack result:" in ll
        or ("attacked" in ll and ("land taken" in ll or "acres" in ll))
    )
    if not has_attack_shape:
        return {"saved": False}
    if not d.get("defender") or not d.get("result"):
        # Avoid storing partial/non-standard fragments as attack rows.
        return {"saved": False}
    if d.get("land_taken") is None:
        d["land_taken"] = 0

    h = hash_report(msg_content)
    raw_gz = psycopg2.Binary(compress_report(msg_content))
    raw_text = msg_content if KEEP_RAW_TEXT else None
    raw_text_compat = msg_content or ""

    settlements = d.get("settlements_lost") or []
    settlements_txt = " | ".join([str(x).strip() for x in settlements if str(x).strip()]) or None

    with db_conn() as conn, conn.cursor() as cur:
        if source_message_id:
            cur.execute(
                "SELECT id FROM attack_reports WHERE source_message_id=%s LIMIT 1;",
                (int(source_message_id),),
            )
            exists_msg = cur.fetchone()
            if exists_msg:
                return {"saved": True, "duplicate": True, "row": None}

        cur.execute("SELECT id FROM attack_reports WHERE report_hash=%s LIMIT 1;", (h,))
        exists = cur.fetchone()
        if exists:
            return {"saved": True, "duplicate": True, "row": None}

        cur.execute(
            """
            INSERT INTO attack_reports (
                attacker, defender, attack_result, land_taken,
                settlements_lost_count, settlements_lost, reported_at, created_at,
                raw, raw_text, raw_gz, report_hash, source_message_id, source_channel_id
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id, attacker, defender, attack_result, land_taken,
                      settlements_lost_count, settlements_lost, reported_at, created_at, source_message_id;
            """,
            (
                d.get("attacker"),
                d.get("defender"),
                d.get("result"),
                d.get("land_taken"),
                int(d.get("settlements_lost_count") or 0),
                settlements_txt,
                reported_at,
                created_at_utc,
                raw_text,
                raw_text_compat,
                raw_gz,
                h,
                int(source_message_id) if source_message_id else None,
                int(source_channel_id) if source_channel_id else None,
            ),
        )
        row = cur.fetchone()

        movement_rows = 0
        sent_units = d.get("sent_units") or {}
        if KG_TRACK_ATTACK_REPORT_MOVEMENTS and row and sent_units and d.get("attacker"):
            departed = reported_at or created_at_utc
            base_minutes = compute_base_return_minutes_cur(cur, d.get("attacker"), d.get("defender"), departed)
            expected = estimate_return_time_season_aware(departed, base_minutes)
            if KG_ROUND_TO_TICK:
                expected = _round_ts_to_tick(expected, KG_TICK_MINUTES, KG_TICK_ROUND_MODE)
            movement_rows = sync_add_troop_movements(
                owner_kingdom=str(d.get("attacker")).strip(),
                target_kingdom=(str(d.get("defender")).strip() if d.get("defender") else None),
                units_map=sent_units,
                departed_at=departed,
                expected_return_at=expected,
                source_attack_report_id=int(row.get("id") or 0),
                source_message_id=source_message_id,
                source_channel_id=source_channel_id,
                note=f"from attack report casualties sent count; return_model={'nw_ratio' if KG_RETURN_MODEL_ENABLED else 'legacy'}",
                cur=cur,
            )

        return {"saved": True, "duplicate": False, "row": row, "movement_rows": movement_rows}


def sync_get_attack_rows_for_day(day_start_utc: datetime, day_end_utc: datetime, kingdom: str | None = None):
    with db_conn() as conn, conn.cursor() as cur:
        if kingdom:
            cur.execute(
                """
                SELECT id, attacker, defender, attack_result, land_taken,
                       settlements_lost_count, settlements_lost, source_message_id,
                       COALESCE(reported_at, created_at) AS happened_at
                FROM attack_reports
                WHERE COALESCE(reported_at, created_at) >= %s
                  AND COALESCE(reported_at, created_at) < %s
                  AND (
                    LOWER(COALESCE(defender, '')) = LOWER(%s)
                    OR LOWER(COALESCE(attacker, '')) = LOWER(%s)
                  )
                ORDER BY COALESCE(reported_at, created_at) DESC, id DESC;
                """,
                (day_start_utc, day_end_utc, kingdom, kingdom),
            )
        else:
            cur.execute(
                """
                SELECT id, attacker, defender, attack_result, land_taken,
                       settlements_lost_count, settlements_lost, source_message_id,
                       COALESCE(reported_at, created_at) AS happened_at
                FROM attack_reports
                WHERE COALESCE(reported_at, created_at) >= %s
                  AND COALESCE(reported_at, created_at) < %s
                ORDER BY COALESCE(reported_at, created_at) DESC, id DESC;
                """,
                (day_start_utc, day_end_utc),
            )
        return cur.fetchall()


def sync_get_latest_networth_for_kingdom_before_cur(cur, kingdom: str, at_utc: datetime):
    """
    Returns latest known NW for a kingdom by parsing saved SR text.
    We intentionally do not query a dedicated DB column because legacy deployments
    may not have one on spy_reports.
    """
    cur.execute(
        """
        SELECT id, raw, raw_gz
        FROM spy_reports
        WHERE LOWER(kingdom) = LOWER(%s)
          AND created_at <= %s
        ORDER BY created_at DESC NULLS LAST, id DESC
        LIMIT 25;
        """,
        (kingdom, normalize_to_utc(at_utc)),
    )
    rows = cur.fetchall() or []
    for row in rows:
        try:
            text = extract_report_text_for_row(row)
            details = parse_spy_details(text)
            nw = details.get("net_worth")
            if nw is not None:
                return int(nw)
        except Exception:
            continue
    # Fallback to game API if configured.
    return fetch_kingdom_networth_from_game_api(kingdom)


def infer_hit_direction_from_nw_cur(cur, attacker: str | None, defender: str | None, at_utc: datetime) -> str | None:
    atk = str(attacker or "").strip()
    dfn = str(defender or "").strip()
    if not atk or not dfn:
        return None
    a_nw = sync_get_latest_networth_for_kingdom_before_cur(cur, atk, at_utc)
    d_nw = sync_get_latest_networth_for_kingdom_before_cur(cur, dfn, at_utc)
    if a_nw is None or d_nw is None:
        return None
    if a_nw < d_nw:
        return "up"
    if a_nw > d_nw:
        return "down"
    return "even"


def sync_compute_expected_return_for_pair(
    attacker: str | None,
    defender: str | None,
    departed_at: datetime,
    attacker_nw_hint: int | None = None,
    defender_nw_hint: int | None = None,
) -> datetime:
    with db_conn() as conn, conn.cursor() as cur:
        base_minutes = compute_base_return_minutes_cur(
            cur,
            attacker,
            defender,
            departed_at,
            attacker_nw_hint=attacker_nw_hint,
            defender_nw_hint=defender_nw_hint,
        )
    expected = estimate_return_time_season_aware(departed_at, base_minutes)
    if KG_ROUND_TO_TICK:
        expected = _round_ts_to_tick(expected, KG_TICK_MINUTES, KG_TICK_ROUND_MODE)
    return expected


def sync_add_troop_movements(
    owner_kingdom: str,
    target_kingdom: str | None,
    units_map: dict,
    departed_at: datetime,
    expected_return_at: datetime,
    source_attack_report_id: int | None = None,
    source_message_id: int | None = None,
    source_channel_id: int | None = None,
    note: str | None = None,
    cur=None,
):
    owner = str(owner_kingdom or "").strip()
    if not owner:
        return 0
    inserted = 0
    season = season_name_at(departed_at)
    owns_cursor = cur is None
    ctx = db_conn() if owns_cursor else None
    conn = ctx.__enter__() if owns_cursor else None
    cur = conn.cursor() if owns_cursor else cur
    try:
        for raw_unit, raw_count in (units_map or {}).items():
            unit = normalize_unit_name(raw_unit) or str(raw_unit or "").strip().lower()
            if not unit:
                continue
            count = int(raw_count or 0)
            if count <= 0:
                continue
            cur.execute(
                """
                INSERT INTO troop_movements (
                    owner_kingdom, target_kingdom, unit_name, units_sent, departed_at, expected_return_at,
                    status, source_attack_report_id, source_message_id, source_channel_id, season_at_departure, note
                )
                VALUES (%s,%s,%s,%s,%s,%s,'out',%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING;
                """,
                (
                    owner,
                    (str(target_kingdom).strip() if target_kingdom else None),
                    unit,
                    count,
                    normalize_to_utc(departed_at),
                    normalize_to_utc(expected_return_at),
                    (int(source_attack_report_id) if source_attack_report_id else None),
                    (int(source_message_id) if source_message_id else None),
                    (int(source_channel_id) if source_channel_id else None),
                    season,
                    (str(note).strip() if note else None),
                ),
            )
            inserted += int(cur.rowcount or 0)
    finally:
        if owns_cursor and ctx is not None:
            ctx.__exit__(None, None, None)
    return inserted


def sync_get_troops_out_for_kingdom_at(kingdom: str, at_utc: datetime):
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, owner_kingdom, target_kingdom, unit_name, units_sent, departed_at, expected_return_at, note
            FROM troop_movements
            WHERE LOWER(owner_kingdom) = LOWER(%s)
              AND status = 'out'
              AND departed_at <= %s
              AND expected_return_at > %s
            ORDER BY expected_return_at ASC, id ASC;
            """,
            (kingdom, normalize_to_utc(at_utc), normalize_to_utc(at_utc)),
        )
        return cur.fetchall()


def sync_mark_due_troop_returns(now_ts: datetime):
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE troop_movements
            SET status = 'returned'
            WHERE status = 'out'
              AND expected_return_at <= %s
            RETURNING id, owner_kingdom, target_kingdom, unit_name, units_sent, departed_at, expected_return_at, source_channel_id;
            """,
            (normalize_to_utc(now_ts),),
        )
        return cur.fetchall()


def sync_get_latest_spy_for_kingdom_before(kingdom: str, at_utc: datetime):
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, kingdom, defense_power, castles, created_at, raw, raw_gz
            FROM spy_reports
            WHERE LOWER(kingdom)=LOWER(%s)
              AND created_at <= %s
            ORDER BY created_at DESC NULLS LAST, id DESC
            LIMIT 1;
            """,
            (kingdom, normalize_to_utc(at_utc)),
        )
        return cur.fetchone()


def aggregate_out_rows(rows: list[dict]) -> tuple[dict, list[str]]:
    units = {}
    notes = []
    for r in rows or []:
        u = str(r.get("unit_name") or "").strip().lower()
        c = int(r.get("units_sent") or 0)
        if u and c > 0:
            units[u] = int(units.get(u, 0) or 0) + c
        tgt = str(r.get("target_kingdom") or "unknown")
        due = r.get("expected_return_at")
        due_txt = str(due).split(".")[0] if due else "unknown"
        label = UNIT_DISPLAY.get(u, u.title() if u else "Units")
        notes.append(f"{label} {fmt_int(c)} -> {tgt} (returns {due_txt} UTC)")
    return units, notes


def aggregate_out_rows_grouped(rows: list[dict]) -> list[str]:
    """
    Group outgoing rows by (target_kingdom, expected_return_at) so one line shows
    the combined troops returning together.
    """
    grouped = {}
    for r in rows or []:
        tgt = str(r.get("target_kingdom") or "unknown").strip() or "unknown"
        due = normalize_to_utc(r.get("expected_return_at")) if r.get("expected_return_at") else None
        key = (tgt.lower(), int(due.timestamp()) if due else 0)
        g = grouped.get(key)
        if not g:
            g = {"target": tgt, "due": due, "units": {}, "total": 0}
            grouped[key] = g
        u = str(r.get("unit_name") or "").strip().lower()
        c = int(r.get("units_sent") or 0)
        if u and c > 0:
            g["units"][u] = int(g["units"].get(u, 0) or 0) + c
            g["total"] = int(g["total"] or 0) + c

    out = []
    ordered = sorted(
        grouped.values(),
        key=lambda x: (
            int(x["due"].timestamp()) if x.get("due") else 0,
            str(x.get("target") or "").lower(),
        ),
    )
    for g in ordered:
        due = g.get("due")
        due_txt = str(due).split(".")[0] if due else "unknown"
        units_txt = fmt_units_short(g.get("units") or {}, limit=6)
        out.append(
            f"{fmt_int(int(g.get('total') or 0))} troops ({units_txt}) -> {g.get('target')} (returns {due_txt} UTC)"
        )
    return out


def format_out_annotation(rows: list[dict]) -> str:
    if not rows:
        return ""
    _, notes = aggregate_out_rows(rows)
    if not notes:
        return ""


def sync_build_oven_estimate(kingdom: str, nw_delta: int | None = None, event_time=None) -> dict:
    if not OVEN_ESTIMATOR_ENABLED:
        return {"ok": False, "reason": "disabled"}
    lookback_since = now_utc() - timedelta(hours=max(1, int(OVEN_LOOKBACK_HOURS or 36)))
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, kingdom, created_at, raw, raw_gz
            FROM spy_reports
            WHERE LOWER(BTRIM(COALESCE(kingdom, ''))) = LOWER(BTRIM(%s))
              AND created_at >= %s
            ORDER BY created_at DESC NULLS LAST, id DESC
            LIMIT 25;
            """,
            (kingdom, lookback_since),
        )
        report_rows = cur.fetchall() or []

    snapshots = []
    for row in report_rows:
        raw_text = extract_report_text_for_row(row)
        troops = parse_sr_troops(raw_text)
        if not troops:
            continue
        signal_value, _signal_name = _snapshot_peasant_signal(troops)
        if signal_value is None:
            continue
        report_ts, has_explicit_tz = parse_report_datetime_from_line(raw_text)
        effective_at = coerce_report_time(report_ts, row.get("created_at"), has_explicit_tz) or row.get("created_at")
        if effective_at and normalize_to_utc(effective_at) < lookback_since:
            continue
        snapshots.append({
            "report_id": int(row["id"]),
            "captured_at": effective_at or row.get("created_at"),
            "troops": troops,
            "raw_text": raw_text,
        })

    snapshots.sort(key=lambda s: (normalize_to_utc(s.get("captured_at")) if s.get("captured_at") else datetime.min.replace(tzinfo=timezone.utc), int(s.get("report_id") or 0)), reverse=True)

    if len(snapshots) < 2:
        return {"ok": False, "reason": "need_two_recent_sr", "kingdom": kingdom}

    newest = snapshots[0]
    prev = snapshots[1]

    old_troops = prev["troops"]
    new_troops = newest["troops"]

    old_peas, old_signal = _snapshot_peasant_signal(old_troops)
    new_peas, new_signal = _snapshot_peasant_signal(new_troops)
    if old_peas is None or new_peas is None:
        return {"ok": False, "reason": "missing_peasant_signal", "kingdom": kingdom}

    peasant_delta = int(old_peas) - int(new_peas)
    if peasant_delta <= 0:
        return {
            "ok": False,
            "reason": "no_missing_peasants",
            "kingdom": kingdom,
            "old_peasants": int(old_peas),
            "new_peasants": int(new_peas),
        }

    inferred_nw_delta = None
    old_nw = None
    new_nw = None
    try:
        old_nw = parse_spy_details(prev.get("raw_text") or "").get("net_worth")
        new_nw = parse_spy_details(newest.get("raw_text") or "").get("net_worth")
        if old_nw is not None and new_nw is not None and int(new_nw) > int(old_nw):
            inferred_nw_delta = int(new_nw) - int(old_nw)
    except Exception:
        inferred_nw_delta = None

    use_nw_delta = int(nw_delta) if nw_delta is not None and int(nw_delta or 0) > 0 else inferred_nw_delta
    candidates = build_oven_candidates(
        peasant_delta,
        use_nw_delta,
        old_ts=prev.get("captured_at"),
        new_ts=newest.get("captured_at"),
        event_time=event_time,
    )

    return {
        "ok": bool(candidates),
        "reason": "ok" if candidates else "no_candidates",
        "kingdom": kingdom,
        "old_report_id": int(prev["report_id"]),
        "new_report_id": int(newest["report_id"]),
        "old_captured_at": prev.get("captured_at"),
        "new_captured_at": newest.get("captured_at"),
        "old_peasants": int(old_peas),
        "new_peasants": int(new_peas),
        "peasant_delta": int(peasant_delta),
        "peasant_signal": old_signal if old_signal == new_signal else f"{old_signal}/{new_signal}",
        "nw_delta": use_nw_delta,
        "old_networth": old_nw,
        "new_networth": new_nw,
        "candidates": candidates,
    }


def format_oven_summary_lines(est: dict, limit: int = 3, compact: bool = False) -> list[str]:
    if not est or not est.get("ok"):
        return []
    missing = int(est.get("peasant_delta") or 0)
    nw_delta = est.get("nw_delta")
    signal = str(est.get("peasant_signal") or "Peasants")
    prefix = "Oven guess" if compact else f"🧁 **Oven Estimate • {est.get('kingdom')}**"
    header = f"{prefix}: {signal} -`{fmt_int(missing)}`"
    if nw_delta is not None:
        header += f" | NW +`{fmt_int(nw_delta)}`"
    lines = [header]
    for c in (est.get("candidates") or [])[: max(1, int(limit or 3))]:
        unit = str(c.get("unit") or "Unit")
        count = int(c.get("count") or 0)
        conf = str(c.get("confidence") or "Low")
        expected_nw = int(c.get("expected_nw") or 0)
        time_txt = str(c.get("time_text") or "pop time unknown")
        if compact:
            lines.append(f"  • {conf}: {fmt_int(count)} {unit} (NW ~{fmt_int(expected_nw)})")
        else:
            lines.append(f"• **{conf}**: `{fmt_int(count)}` {unit} | NW ~`{fmt_int(expected_nw)}` | {time_txt}")
            counter = str(c.get("counter") or "").strip()
            if counter:
                lines.append(f"  Counter note: {counter}")
    if not compact:
        lines.append(
            f"Based on SR `#{est.get('old_report_id')}` ({_format_dt_short(est.get('old_captured_at'))}) "
            f"→ `#{est.get('new_report_id')}` ({_format_dt_short(est.get('new_captured_at'))})."
        )
    return lines

    lines = [
        "",
        "THIS sr likely did not contain the following troops expected to be out at SR time:",
    ]
    for n in notes[:8]:
        lines.append(f"- {n}")
    extra = len(notes) - 8
    if extra > 0:
        lines.append(f"... +{extra} more")
    return "\n".join(lines)


def fmt_units_short(units_map: dict, limit: int = 8) -> str:
    if not units_map:
        return "None"
    items = sorted((units_map or {}).items(), key=lambda x: int(x[1] or 0), reverse=True)
    parts = []
    for n, c in items[:limit]:
        label = UNIT_DISPLAY.get(str(n).lower(), str(n).replace("_", " ").title())
        parts.append(f"{label} {fmt_int(c)}")
    if len(items) > limit:
        parts.append(f"+{len(items) - limit} more")
    return " | ".join(parts)


def build_live_battle_update_text(kingdom: str, est: dict, header: str | None = None) -> str:
    spy = est.get("spy") or {}
    spy_id = spy.get("id")
    spy_at = spy.get("created_at")
    spy_at_txt = str(spy_at).split(".")[0] if spy_at else "Unknown"
    out_units = est.get("out_units") or {}
    out_notes = est.get("out_notes") or []
    out_rows = est.get("out_rows") or []
    out_grouped = aggregate_out_rows_grouped(out_rows) if out_rows else out_notes
    home_est = est.get("estimated_home") or {}

    lines = []
    if header:
        lines.append(header)
    lines.extend(
        [
            f"Battle Tracker | {kingdom}",
            f"Baseline SR: #{spy_id} at {spy_at_txt} UTC",
            f"Estimated home now: {fmt_units_short(home_est)}",
            f"Tracked out now: {fmt_units_short(out_units)}",
        ]
    )
    if out_grouped:
        lines.append("Returns in-flight:")
        for n in out_grouped[:6]:
            lines.append(f"- {n}")
        extra = len(out_grouped) - 6
        if extra > 0:
            lines.append(f"... +{extra} more")
    return "\n".join(lines)


def sync_build_battle_estimate(kingdom: str, at_utc: datetime):
    spy = sync_get_latest_spy_for_kingdom_before(kingdom, at_utc)
    if not spy:
        return {"ok": False, "reason": "no_spy"}
    raw = extract_report_text_for_row(spy)
    home = {}
    for name, c in (parse_sr_troops(raw) or {}).items():
        n = normalize_unit_name(name)
        if n:
            home[n] = int(home.get(n, 0) or 0) + int(c or 0)
    out_rows = sync_get_troops_out_for_kingdom_at(kingdom, at_utc)
    out_units, out_notes = aggregate_out_rows(out_rows)
    est_home = dict(home)
    for u, c in out_units.items():
        est_home[u] = max(0, int(est_home.get(u, 0) or 0) - int(c or 0))
    return {
        "ok": True,
        "spy": spy,
        "out_rows": out_rows,
        "home_from_spy": home,
        "out_units": out_units,
        "out_notes": out_notes,
        "estimated_home": est_home,
    }


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


def sync_get_research_export_rows():
    """
    CSV export rows: one row per kingdom + best indexed research line.
    Includes kingdoms that have spy reports but currently no indexed battle tech.
    """
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            WITH kingdoms AS (
                SELECT DISTINCT kingdom
                FROM spy_reports
                WHERE kingdom IS NOT NULL
            ),
            latest_spy AS (
                SELECT DISTINCT ON (kingdom)
                    kingdom,
                    id AS latest_report_id,
                    created_at AS latest_report_at
                FROM spy_reports
                WHERE kingdom IS NOT NULL
                ORDER BY kingdom, created_at DESC NULLS LAST, id DESC
            )
            SELECT
                k.kingdom,
                ls.latest_report_id,
                ls.latest_report_at,
                kt.tech_name,
                kt.best_level,
                kt.updated_at AS tech_updated_at,
                kt.source_report_id,
                COALESCE(ti.hits, 0) AS indexed_hits
            FROM kingdoms k
            LEFT JOIN latest_spy ls
              ON ls.kingdom = k.kingdom
            LEFT JOIN kingdom_tech kt
              ON kt.kingdom = k.kingdom
            LEFT JOIN LATERAL (
                SELECT COUNT(*) AS hits
                FROM tech_index t
                WHERE t.kingdom = k.kingdom
                  AND kt.tech_name IS NOT NULL
                  AND t.tech_name = kt.tech_name
            ) ti ON TRUE
            ORDER BY
                k.kingdom ASC,
                kt.best_level DESC NULLS LAST,
                kt.tech_name ASC NULLS LAST;
        """)
        return cur.fetchall()


def sync_get_all_spy_report_export_rows(limit: int = 200000):
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                id,
                kingdom,
                defense_power,
                castles,
                created_at,
                report_hash
            FROM spy_reports
            ORDER BY created_at DESC NULLS LAST, id DESC
            LIMIT %s;
            """,
            (int(limit),),
        )
        return cur.fetchall() or []


def sync_get_all_attack_report_export_rows(limit: int = 200000):
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                id,
                attacker,
                defender,
                attack_result,
                land_taken,
                settlements_lost_count,
                settlements_lost,
                reported_at,
                created_at,
                report_hash,
                source_message_id,
                source_channel_id
            FROM attack_reports
            ORDER BY COALESCE(reported_at, created_at) DESC NULLS LAST, id DESC
            LIMIT %s;
            """,
            (int(limit),),
        )
        return cur.fetchall() or []


def sync_backfill(days: int | None = None, progress_id: str | None = None):
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
        "market_reports": 0,
        "market_rows": 0,
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
        total_rows = len(rows)

        if progress_id:
            BACKFILL_PROGRESS[progress_id] = {
                "phase": "db_reprocess",
                "done": 0,
                "total": int(total_rows),
                "updated_at": time.time(),
                "complete": False,
            }

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

            # market transactions / supplier traces
            txs = parse_market_transactions(text, k)
            if txs:
                stats["market_reports"] += 1
                inserted = sync_upsert_market_transactions(cur, int(row["id"]), row.get("created_at") or now_utc(), txs)
                stats["market_rows"] += int(inserted)

            if progress_id and ((stats["reports_scanned"] % 100) == 0 or stats["reports_scanned"] == total_rows):
                BACKFILL_PROGRESS[progress_id] = {
                    "phase": "db_reprocess",
                    "done": int(stats["reports_scanned"]),
                    "total": int(total_rows),
                    "updated_at": time.time(),
                    "complete": False,
                }

        if progress_id:
            BACKFILL_PROGRESS[progress_id] = {
                "phase": "db_reprocess",
                "done": int(stats["reports_scanned"]),
                "total": int(total_rows),
                "updated_at": time.time(),
                "complete": True,
            }

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
    embed.add_field(name="Footmen Needed (est.)", value=f"{ceil(adj / FOOTMEN_AP):,}", inline=True)

    for label, red in AP_REDUCTIONS:
        rem = ceil(adj * (1 - red))
        embed.add_field(
            name=f"{label} (-{int(red*100)}%)",
            value=(
                f"Remaining DP: {rem:,}\n"
                f"Remaining HC: {ceil(rem/HEAVY_CAVALRY_AP):,}\n"
                f"Remaining Footmen: {ceil(rem/FOOTMEN_AP):,}"
            ),
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
    embed.add_field(name="Footmen Needed (est.)", value=f"{ceil(current_dp / FOOTMEN_AP):,}", inline=True)
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
        uid = int(getattr(ctx.author, "id", 0) or 0)
        if uid in ADMIN_USER_IDS:
            return True
        if ctx.guild and ctx.author and getattr(ctx.author, "guild_permissions", None):
            return bool(ctx.author.guild_permissions.administrator)
    except Exception:
        pass
    return False


async def _require_premium(ctx: commands.Context) -> bool:
    """
    Premium gate for premium-only commands.
    Only explicit free-user IDs bypass (owner/dev accounts).
    """
    try:
        uid = int(getattr(ctx.author, "id", 0) or 0)
    except Exception:
        uid = 0
    if uid in PREMIUM_FREE_USER_IDS:
        return True
    if not PREMIUM_GATE_ENABLED:
        return True
    ok = await run_db(sync_is_premium_discord_user, uid)
    if ok:
        return True
    await ctx.send("🔒 This command is a premium feature. Upgrade on recon-hub to use it.")
    return False


async def send_due_return_alerts(returned_rows: list[dict]):
    async def _resolve_channel(cid: int):
        if cid <= 0:
            return None
        ch = bot.get_channel(cid)
        if ch:
            return ch
        for g in bot.guilds:
            ch = g.get_channel(cid)
            if ch:
                return ch
        try:
            fetched = await bot.fetch_channel(cid)
            return fetched
        except Exception:
            return None

    by_channel_owner = {}
    for r in returned_rows or []:
        channel_id = int(r.get("source_channel_id") or 0)
        owner = str(r.get("owner_kingdom") or "Unknown").strip() or "Unknown"
        by_channel_owner.setdefault(channel_id, {}).setdefault(owner, []).append(r)

    for channel_id, owner_map in by_channel_owner.items():
        sent_to_specific_channel = False
        if channel_id > 0:
            ch = await _resolve_channel(channel_id)
            guild = getattr(ch, "guild", None)
            if ch and guild and can_send(ch, guild):
                sent_to_specific_channel = True
                for owner, rows in owner_map.items():
                    lines = [f"Troops return alert: **{owner}** may have troops back home."]
                    for r in rows[:6]:
                        unit = UNIT_DISPLAY.get(str(r.get("unit_name") or "").lower(), str(r.get("unit_name") or "Unit"))
                        cnt = int(r.get("units_sent") or 0)
                        tgt = str(r.get("target_kingdom") or "unknown")
                        departed = r.get("departed_at")
                        d_txt = str(departed).split(".")[0] if departed else "unknown"
                        lines.append(f"- {unit} `{fmt_int(cnt)}` from hit on `{tgt}` (sent {d_txt} UTC)")
                    extra = len(rows) - 6
                    if extra > 0:
                        lines.append(f"... +{extra} more")
                    try:
                        await ch.send("\n".join(lines))
                    except Exception:
                        pass
            else:
                logging.warning(
                    "return alert channel unresolved or unsendable for source_channel_id=%s; skipping fallback for strict routing",
                    channel_id,
                )

        if sent_to_specific_channel:
            continue

        # Fallback only when source channel is unknown/missing.
        if channel_id > 0:
            continue

        for guild in bot.guilds:
            ch = get_live_battle_channel(guild, None)
            if not (ch and can_send(ch, guild)):
                continue
            for owner, rows in owner_map.items():
                lines = [f"Troops return alert: **{owner}** may have troops back home."]
                for r in rows[:6]:
                    unit = UNIT_DISPLAY.get(str(r.get("unit_name") or "").lower(), str(r.get("unit_name") or "Unit"))
                    cnt = int(r.get("units_sent") or 0)
                    tgt = str(r.get("target_kingdom") or "unknown")
                    departed = r.get("departed_at")
                    d_txt = str(departed).split(".")[0] if departed else "unknown"
                    lines.append(f"- {unit} `{fmt_int(cnt)}` from hit on `{tgt}` (sent {d_txt} UTC)")
                extra = len(rows) - 6
                if extra > 0:
                    lines.append(f"... +{extra} more")
                try:
                    await ch.send("\n".join(lines))
                except Exception:
                    pass


async def battle_returns_loop():
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            returned_rows = await run_db(sync_mark_due_troop_returns, now_utc())
            if returned_rows:
                await send_due_return_alerts(returned_rows)
        except Exception:
            logging.exception("battle_returns_loop failed")
        await asyncio.sleep(max(10, RETURN_ALERT_POLL_SECONDS))


# ---------- Events ----------
@bot.event
async def on_ready():
    global ANNOUNCED_READY_THIS_PROCESS
    global BATTLE_RETURNS_LOOP_STARTED
    global NW_JUMP_ALERTS_LOOP_STARTED

    try:
        await ensure_db_ready()
    except Exception:
        logging.exception("DB init failed")

    if KG_TROOP_TRACKING_ENABLED and not BATTLE_RETURNS_LOOP_STARTED:
        BATTLE_RETURNS_LOOP_STARTED = True
        asyncio.create_task(battle_returns_loop())
    if NW_JUMP_ALERTS_ENABLED and not NW_JUMP_ALERTS_LOOP_STARTED:
        NW_JUMP_ALERTS_LOOP_STARTED = True
        asyncio.create_task(nw_jump_alerts_loop())

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
        if int(TARGET_GUILD_ID or 0) > 0 and int(guild.id) != int(TARGET_GUILD_ID):
            continue
        ch = get_updates_channel(guild, None)
        if not ch:
            logging.warning(
                "Startup announcement skipped: no sendable updates channel in guild=%s (wanted name=%s)",
                guild.id,
                ERROR_CHANNEL_NAME,
            )
            continue
        try:
            await ch.send(
                f"✅ **KG2 Recon Bot merged + restarted**\n"
                f"Version: `{BOT_VERSION}`\n"
                f"Patch:\n{patch_lines}"
            )
        except Exception as e:
            logging.warning(
                "Startup announcement send failed for guild=%s channel=%s (%s)",
                guild.id,
                getattr(ch, "id", "?"),
                e.__class__.__name__,
            )


@bot.event
async def on_message(msg: discord.Message):
    if msg.author.bot or not msg.guild:
        return

    try:
        live_ch = msg.channel if can_send(msg.channel, msg.guild) else get_live_battle_channel(msg.guild, msg.channel)
        ts = normalize_to_utc(msg.created_at)
        result = await run_db(sync_store_report, msg.content, ts)
        attack_result = await run_db(
            sync_store_attack_report,
            msg.content,
            ts,
            int(msg.id),
            int(msg.channel.id) if getattr(msg, "channel", None) else None,
        )
        alert_inserted = 0
        incoming_alert = parse_incoming_attack_alert(msg.content)
        if KG_TRACK_INCOMING_ALERT_MOVEMENTS and incoming_alert:
            alert_target = str(incoming_alert.get("defender") or "").strip() or None
            if not alert_target and attack_result.get("saved") and attack_result.get("row"):
                alert_target = str((attack_result.get("row") or {}).get("defender") or "").strip() or None
            alert_departed_at = coerce_report_time(
                incoming_alert.get("occurred_at"),
                ts,
                bool(incoming_alert.get("occurred_at_has_tz")),
            ) or ts
            expected = await run_db(
                sync_compute_expected_return_for_pair,
                incoming_alert.get("attacker"),
                alert_target,
                alert_departed_at,
                incoming_alert.get("attacker_nw"),
                None,
            )
            alert_inserted = await run_db(
                sync_add_troop_movements,
                incoming_alert.get("attacker"),
                alert_target,
                incoming_alert.get("units") or {},
                alert_departed_at,
                expected,
                None,
                int(msg.id),
                int(msg.channel.id) if getattr(msg, "channel", None) else None,
                f"from incoming attacked-by alert; target={alert_target or 'unknown'}",
            )
        forwarded = None

        if looks_like_recon_report(msg.content):
            forwarded = await run_db(sync_recon_ingest_report, msg.content)
            if not forwarded.get("ok"):
                logging.warning("recon ingest failed (live): %s", forwarded)

        if result.get("saved") and not result.get("duplicate") and result.get("row"):
            row = result["row"]
            if can_send(msg.channel, msg.guild):
                dp = row.get("defense_power")
                castles = int(row.get("castles") or 0)
                kingdom = row.get("kingdom") or "Unknown"
                dp_txt = f"{int(dp):,}" if dp else "N/A"
                await msg.channel.send(
                    f"✅ Spy report saved: ID `{row.get('id')}` • **{kingdom}** • DP `{dp_txt}` • Castles `{castles}`"
                )
        elif result.get("saved") and result.get("duplicate"):
            if can_send(msg.channel, msg.guild):
                await msg.channel.send("✅ Duplicate spy report detected (repair mode applied).")

        if attack_result.get("saved") and not attack_result.get("duplicate") and attack_result.get("row"):
            row = attack_result["row"]
            if can_send(msg.channel, msg.guild):
                await msg.channel.send(
                    f"Attack report saved: ID `{row.get('id')}` | "
                    f"Defender `{row.get('defender') or 'Unknown'}` | "
                    f"Land `{fmt_int(row.get('land_taken'))}` | "
                    f"Settlement losses `{int(row.get('settlements_lost_count') or 0)}`"
                )
                mr = int(attack_result.get("movement_rows") or 0)
                if mr > 0:
                    if live_ch and can_send(live_ch, msg.guild):
                        await live_ch.send(f"Battle tracker: `{mr}` outgoing troop movement row(s) tracked.")
                atk = str(row.get("attacker") or "").strip()
                if atk:
                    live_est = await run_db(sync_build_battle_estimate, atk, ts)
                    if live_est.get("ok"):
                        if live_ch and can_send(live_ch, msg.guild):
                            await live_ch.send(
                            build_live_battle_update_text(
                                atk,
                                live_est,
                                header="Live update from attack report:",
                            )
                        )

        if KG_TRACK_INCOMING_ALERT_MOVEMENTS and alert_inserted > 0 and live_ch and can_send(live_ch, msg.guild):
            await live_ch.send(f"Battle tracker: tracked `{int(alert_inserted)}` outgoing troop movement row(s) from alert text.")
            alert_atk = str((incoming_alert or {}).get("attacker") or "").strip()
            if alert_atk:
                live_est = await run_db(sync_build_battle_estimate, alert_atk, ts)
                if live_est.get("ok"):
                    await live_ch.send(
                        build_live_battle_update_text(
                            alert_atk,
                            live_est,
                            header="Live update from attacked-by alert:",
                        )
                    )

        if forwarded and forwarded.get("ok"):
            data = forwarded.get("data") or {}
            if data.get("report_type") == "spy" and can_send(msg.channel, msg.guild):
                calc_link = build_calc_link_from_ingest_data(data)
                if calc_link:
                    await msg.channel.send(f"🧮 Open in calc: {calc_link}")
            if data.get("report_type") == "attack" and can_send(msg.channel, msg.guild):
                auto_hit = bool(data.get("auto_known_hit_inserted"))
                await msg.channel.send(
                    f"✅ Attack report synced to recon-hub"
                    f"{' • auto known-hit added' if auto_hit else ''}"
                )

    except Exception as e:
        tb = traceback.format_exc()
        logging.exception("on_message error")
        await send_error(msg.guild, f"on_message error: {e}", tb=tb)

    await bot.process_commands(msg)


async def sync_ingest_history(days: int | None = None):
    """
    Pull spy/attack reports from readable Discord channel history into DB.
    This is safe to rerun because storage is deduped by report hash.
    """
    await ensure_db_ready()

    since = None
    if days and int(days) > 0:
        since = now_utc() - timedelta(days=int(days))

    stats = {
        "guilds": 0,
        "channels_scanned": 0,
        "messages_scanned": 0,
        "messages_matched": 0,
        "reports_saved": 0,
        "duplicates": 0,
        "attack_reports_saved": 0,
        "attack_duplicates": 0,
        "reports_forwarded": 0,
        "forward_skipped": 0,
        "forward_failures": 0,
        "forward_failure_reasons": {},
        "ingest_errors": 0,
    }

    async def _scan_channel(guild: discord.Guild, channel: discord.TextChannel):
        local = {
            "channels_scanned": 1,
            "messages_scanned": 0,
            "messages_matched": 0,
            "reports_saved": 0,
            "duplicates": 0,
            "attack_reports_saved": 0,
            "attack_duplicates": 0,
            "reports_forwarded": 0,
            "forward_skipped": 0,
            "forward_failures": 0,
            "forward_failure_reasons": {},
            "ingest_errors": 0,
        }

        history_kwargs = {"limit": None}
        if since:
            history_kwargs["after"] = since
        if MAX_HISTORY_SCAN_MESSAGES_PER_CHANNEL > 0:
            history_kwargs["limit"] = MAX_HISTORY_SCAN_MESSAGES_PER_CHANNEL

        try:
            async for m in channel.history(**history_kwargs):
                if not m or m.author.bot:
                    continue
                local["messages_scanned"] += 1

                if INGEST_PROGRESS_EVERY_MESSAGES > 0 and (local["messages_scanned"] % INGEST_PROGRESS_EVERY_MESSAGES) == 0:
                    logging.info(
                        "Backfill progress: guild=%s channel=%s scanned=%s",
                        guild.name,
                        channel.name,
                        local["messages_scanned"],
                    )

                candidates = extract_discord_message_texts(m)
                if not candidates:
                    continue

                created_at = normalize_to_utc(m.created_at)
                msg_id = int(m.id)
                ch_id = int(channel.id)

                for content in candidates:
                    try:
                        if INGEST_PREFILTER_ENABLED and not looks_like_history_candidate_fast(content):
                            continue

                        row = await run_db(sync_ingest_history_candidate, content, created_at, msg_id, ch_id)
                        local["messages_matched"] += int(row.get("matched") or 0)
                        local["reports_saved"] += int(row.get("reports_saved") or 0)
                        local["duplicates"] += int(row.get("duplicates") or 0)
                        local["attack_reports_saved"] += int(row.get("attack_reports_saved") or 0)
                        local["attack_duplicates"] += int(row.get("attack_duplicates") or 0)
                        local["reports_forwarded"] += int(row.get("reports_forwarded") or 0)
                        local["forward_skipped"] += int(row.get("forward_skipped") or 0)
                        local["forward_failures"] += int(row.get("forward_failures") or 0)
                        reason = str(row.get("forward_failure_reason") or "").strip().lower()
                        if reason:
                            bucket = local["forward_failure_reasons"]
                            bucket[reason] = int(bucket.get(reason) or 0) + 1
                    except Exception:
                        local["ingest_errors"] += 1
                        logging.exception("Backfill ingest failed for message %s in #%s", getattr(m, "id", "unknown"), channel.name)
        except Exception:
            logging.exception("History scan failed for %s (%s)", guild.name, channel.name)

        return local

    sem = asyncio.Semaphore(max(1, int(BACKFILL_CHANNEL_CONCURRENCY or 1)))

    async def _scan_bounded(guild: discord.Guild, channel: discord.TextChannel):
        async with sem:
            return await _scan_channel(guild, channel)

    jobs = []
    for guild in bot.guilds:
        stats["guilds"] += 1
        for channel in guild.text_channels:
            if not can_read_history(channel, guild):
                continue
            jobs.append(_scan_bounded(guild, channel))

    results = await asyncio.gather(*jobs, return_exceptions=True)
    for r in results:
        if isinstance(r, Exception):
            stats["ingest_errors"] += 1
            logging.exception("Backfill channel job failed", exc_info=r)
            continue
        stats["channels_scanned"] += int(r.get("channels_scanned") or 0)
        stats["messages_scanned"] += int(r.get("messages_scanned") or 0)
        stats["messages_matched"] += int(r.get("messages_matched") or 0)
        stats["reports_saved"] += int(r.get("reports_saved") or 0)
        stats["duplicates"] += int(r.get("duplicates") or 0)
        stats["attack_reports_saved"] += int(r.get("attack_reports_saved") or 0)
        stats["attack_duplicates"] += int(r.get("attack_duplicates") or 0)
        stats["reports_forwarded"] += int(r.get("reports_forwarded") or 0)
        stats["forward_skipped"] += int(r.get("forward_skipped") or 0)
        stats["forward_failures"] += int(r.get("forward_failures") or 0)
        for reason, ct in (r.get("forward_failure_reasons") or {}).items():
            stats["forward_failure_reasons"][reason] = int(stats["forward_failure_reasons"].get(reason) or 0) + int(ct or 0)
        stats["ingest_errors"] += int(r.get("ingest_errors") or 0)

    return stats


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
        content, raw = build_spy_text_report(row)
        out_rows = await run_db(sync_get_troops_out_for_kingdom_at, row.get("kingdom") or real, row.get("created_at"))
        out_note = format_out_annotation(out_rows)
        if out_note:
            content = f"{content}\n{out_note}"
        if raw:
            for part in split_for_discord(raw, 1900):
                await ctx.send(part)
        await ctx.send(content=content)
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
        content, raw = build_spy_text_report(row)
        out_rows = await run_db(
            sync_get_troops_out_for_kingdom_at,
            row.get("kingdom") or "Unknown",
            row.get("created_at"),
        )
        out_note = format_out_annotation(out_rows)
        if out_note:
            content = f"{content}\n{out_note}"
        if raw:
            for part in split_for_discord(raw, 1900):
                await ctx.send(part)
        await ctx.send(content=content)
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
async def spies(ctx, *, kingdom: str):
    """!spies <kingdom> -> last 10 reports with Date/Sent/Lost/Result + send recommendation."""
    try:
        real = await run_db(sync_fuzzy_kingdom, kingdom)
        real = real or kingdom
        rows = await run_db(sync_get_spy_history_with_raw, real, 10)
        if not rows:
            return await ctx.send(f"❌ No saved reports for **{real}**.")

        lines = []
        most_recent_complete_send = None
        most_recent_any_send = None

        for r in rows:
            text = extract_report_text_for_row(r)
            d = parse_spy_details(text)
            sent = d.get("spies_sent")
            lost = d.get("spies_lost")
            result = d.get("result") or "N/A"
            ts = r.get("created_at")
            ts_txt = str(ts).split(".")[0] if ts else "Unknown"

            if sent is not None:
                sent_int = int(sent)
                if most_recent_any_send is None:
                    most_recent_any_send = sent_int
                if "complete infiltration" in str(result).lower() and most_recent_complete_send is None:
                    # rows are newest -> oldest, so first complete is the most recent complete report
                    most_recent_complete_send = sent_int

            lines.append(
                f"• `{ts_txt}` | Sent `{fmt_int(sent)}` | Lost `{fmt_int(lost)}` | Result `{result}`"
            )

        if most_recent_complete_send is not None:
            recommended = most_recent_complete_send
            rec_basis = "based on the most recent `Complete Infiltration` report"
        elif most_recent_any_send is not None:
            recommended = most_recent_any_send
            rec_basis = "based on the most recent report with parsable sent value (no complete infiltration found)"
        else:
            recommended = None
            rec_basis = "no parsable sent values found"

        rec_line = (
            f"Recommended spies to send: `{fmt_int(recommended)}` ({rec_basis})"
            if recommended is not None else
            f"Recommended spies to send: `N/A` ({rec_basis})"
        )

        await ctx.send(
            f"🕵️ **Spies History • {real}**\n"
            f"{rec_line}\n\n"
            + "\n".join(lines)
        )

    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("⚠️ spies failed.")
        await send_error(ctx.guild, f"spies error: {e}", tb=tb)


@bot.command()
async def supply(ctx, *, arg: str):
    """
    !supply <kingdom> [days]
    Premium-only: shows who has been supplying the target kingdom from SR market transactions.
    """
    try:
        if not await _require_premium(ctx):
            return
        dm = await ctx.author.create_dm()

        raw = (arg or "").strip()
        if not raw:
            return await ctx.send("Usage: `!supply <kingdom> [days]`")

        days = 14
        kingdom_query = raw
        parts = raw.split()
        if parts and re.fullmatch(r"\d{1,3}", parts[-1]):
            days = max(1, min(365, int(parts[-1])))
            kingdom_query = " ".join(parts[:-1]).strip() or kingdom_query

        real = await run_db(sync_fuzzy_kingdom, kingdom_query)
        real = real or kingdom_query
        since = now_utc() - timedelta(days=int(days))
        res = await run_db(sync_get_supply_summary, real, since, 160)
        summary = res.get("summary") or []
        details = res.get("details") or []

        if not summary:
            # Fallback: parse directly from stored raw reports in case indexing was introduced after reports were saved.
            spy_rows = await run_db(sync_get_spy_reports_raw_since, real, since, 400)
            agg = {}
            parsed_details = []
            for sr in spy_rows:
                text = extract_report_text_for_row(sr)
                if not text:
                    continue
                d = parse_spy_details(text)
                buyer_name = d.get("target") or sr.get("kingdom") or real
                txs = parse_market_transactions(text, buyer_name)
                cap = sr.get("created_at")
                for tx in txs:
                    seller = str(tx.get("seller_kingdom") or "").strip()
                    buyer = str(tx.get("buyer_kingdom") or "").strip()
                    if not seller or not buyer:
                        continue
                    if buyer.lower().strip() != str(real).lower().strip():
                        continue
                    qty = int(tx.get("quantity") or 0)
                    gold = int(tx.get("gold_amount") or 0)
                    key = seller.lower()
                    if key not in agg:
                        agg[key] = {
                            "seller": seller,
                            "tx_count": 0,
                            "qty_sum": 0,
                            "gold_sum": 0,
                            "zero_gold_count": 0,
                            "last_seen": cap,
                        }
                    a = agg[key]
                    a["tx_count"] += 1
                    a["qty_sum"] += qty
                    a["gold_sum"] += gold
                    if gold == 0:
                        a["zero_gold_count"] += 1
                    if (a.get("last_seen") is None) or (cap and cap > a.get("last_seen")):
                        a["last_seen"] = cap
                    parsed_details.append(
                        {
                            "captured_at": cap,
                            "report_id": sr.get("id"),
                            "buyer_kingdom": buyer,
                            "seller_kingdom": seller,
                            "resource": tx.get("resource"),
                            "quantity": qty,
                            "gold_amount": gold,
                            "tx_time_text": tx.get("tx_time_text"),
                            "raw_line": tx.get("raw_line"),
                        }
                    )

            summary = sorted(
                agg.values(),
                key=lambda x: (int(x.get("qty_sum") or 0), int(x.get("tx_count") or 0), str(x.get("seller") or "")),
                reverse=True,
            )
            details = sorted(
                parsed_details,
                key=lambda x: (x.get("captured_at") or datetime(1970, 1, 1, tzinfo=timezone.utc), int(x.get("report_id") or 0)),
                reverse=True,
            )[:160]

            if not summary:
                return await dm.send(
                    f"No supplier transactions found for **{real}** in the last `{days}` days.\n"
                    "Tip: paste full SR reports that include market transactions, then run `!backfill`."
                )

        lines = []
        resource_map = _build_supply_resource_breakdown(details)
        for i, r in enumerate(summary[:12], start=1):
            seller = r.get("seller") or "Unknown"
            tx_count = int(r.get("tx_count") or 0)
            qty_sum = int(r.get("qty_sum") or 0)
            gold_sum = int(r.get("gold_sum") or 0)
            zero = int(r.get("zero_gold_count") or 0)
            last_seen = r.get("last_seen")
            last_txt = str(last_seen).split(".")[0] if last_seen else "Unknown"
            label = _supply_confidence_label(tx_count, qty_sum, gold_sum, zero)
            top_resource = _top_resource_text_for_seller(resource_map, str(seller))
            lines.append(
                f"{i}. **{seller}** | `{label}` | Tx `{tx_count}` | Qty `{fmt_int(qty_sum)}` | "
                f"Gold `{fmt_int(gold_sum)}` | Zero-gold `{zero}` | {top_resource} | Last `{last_txt}` UTC"
            )

        await dm.send(
            f"📦 **Supply Chain • {real}** (last `{days}` days)\n"
            "Top suppliers by total quantity sold into target:\n"
            + "\n".join(lines)
        )

        # TSV export for spreadsheet / deeper review
        out = io.StringIO()
        writer = csv.writer(out, delimiter="\t", lineterminator="\n")
        writer.writerow([
            "captured_at_utc",
            "report_id",
            "buyer_kingdom",
            "seller_kingdom",
            "resource",
            "quantity",
            "gold_amount",
            "tx_time_text",
            "raw_line",
        ])
        for d in details:
            cap = d.get("captured_at")
            writer.writerow([
                cap.isoformat() if cap else "",
                d.get("report_id") or "",
                d.get("buyer_kingdom") or "",
                d.get("seller_kingdom") or "",
                d.get("resource") or "",
                int(d.get("quantity") or 0),
                int(d.get("gold_amount") or 0),
                d.get("tx_time_text") or "",
                d.get("raw_line") or "",
            ])
        payload = out.getvalue().encode("utf-8")
        out.close()

        fname = f"kg2_supply_{str(real).strip().replace(' ', '_')}_{now_utc().strftime('%Y%m%d_%H%M%S')}.tsv"
        await dm.send(file=discord.File(fp=io.BytesIO(payload), filename=fname))
        await ctx.send("Supply report sent to your DMs.")

    except Exception as e:
        tb = traceback.format_exc()
        try:
            await ctx.send("supply failed. Make sure your DMs are open and try again.")
        except Exception:
            pass
        await send_error(ctx.guild, f"supply error: {e}", tb=tb)


@bot.command()
async def battle(ctx, *, kingdom: str):
    """!battle <kingdom> -> estimated now-troops using latest SR minus tracked outgoing troops."""
    try:
        if not KG_TROOP_TRACKING_ENABLED:
            return await ctx.send("Battle tracker is currently disabled (troop tracking off).")
        real = await run_db(sync_fuzzy_kingdom, kingdom)
        real = real or kingdom
        est = await run_db(sync_build_battle_estimate, real, now_utc())
        if not est.get("ok"):
            return await ctx.send(f"No saved SR baseline found for **{real}**.")

        spy = est.get("spy") or {}
        spy_id = spy.get("id")
        spy_at = spy.get("created_at")
        spy_at_txt = str(spy_at).split(".")[0] if spy_at else "Unknown"
        out_units = est.get("out_units") or {}
        out_notes = est.get("out_notes") or []
        home_sr = est.get("home_from_spy") or {}
        home_est = est.get("estimated_home") or {}

        lines = [
            f"Battle Tracker | {real}",
            f"Baseline SR: #{spy_id} at {spy_at_txt} UTC",
            f"Home from SR: {fmt_units_short(home_sr)}",
            f"Tracked out now: {fmt_units_short(out_units)}",
            f"Estimated home now: {fmt_units_short(home_est)}",
        ]
        if out_notes:
            lines.append("")
            lines.append("Outgoing movements:")
            for n in out_notes[:8]:
                lines.append(f"- {n}")
            extra = len(out_notes) - 8
            if extra > 0:
                lines.append(f"... +{extra} more")

        await ctx.send("\n".join(lines))
    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("battle failed.")
        await send_error(ctx.guild, f"battle error: {e}", tb=tb)


@bot.command(name="kingdomlive", aliases=["intel"])
async def kingdomlive(ctx, *, arg: str):
    """!kingdomlive <kingdom> [hours] -> live rankings profile with lookback deltas."""
    try:
        raw = str(arg or "").strip()
        if not raw:
            return await ctx.send("Usage: `!kingdomlive <kingdom> [hours]`")

        hours = int(KINGDOM_LIVE_DEFAULT_LOOKBACK_HOURS or 1)
        kingdom = raw
        parts = raw.rsplit(" ", 1)
        if len(parts) == 2 and re.match(r"^\d+$", parts[1].strip()):
            maybe_hours = int(parts[1].strip())
            if maybe_hours > 0:
                hours = maybe_hours
                kingdom = parts[0].strip()
        if not kingdom:
            return await ctx.send("Usage: `!kingdomlive <kingdom> [hours]`")

        profile = await run_db(sync_get_live_kingdom_profile, kingdom, hours)
        if not profile.get("ok"):
            reason = str(profile.get("reason") or "unknown")
            if reason == "no_rankings_data":
                return await ctx.send(
                    f"❌ No live rankings data found for **{profile.get('kingdom') or kingdom}** yet. Wait for the poller to collect snapshots first."
                )
            return await ctx.send(f"❌ Live intel unavailable for **{kingdom}** (`{reason}`).")

        real = str(profile.get("kingdom") or kingdom)
        current = profile.get("current") or {}
        baseline = profile.get("baseline") or {}
        window = profile.get("window") or {}
        delta = profile.get("delta") or {}
        attacks = profile.get("attacks") or {}
        latest_spy = profile.get("latest_spy") or {}
        battle_est = profile.get("battle") or {}
        oven_est = profile.get("oven") or {}

        rank_now = current.get("rank")
        nw_now = current.get("networth")
        pie_now = "active" if current.get("pie_active") else "clear"
        pie_label = str(current.get("pie_label") or "").strip()

        if rank_now:
            now_line = (
                f"Now: rank `#{int(rank_now)}` | NW `{fmt_int(nw_now)}` | pie `{pie_now}` | "
                f"updated `{_format_dt_short(current.get('updated_at'))}`"
            )
        else:
            now_line = (
                f"Now: rank `n/a` | NW `{fmt_int(nw_now)}` | pie `{pie_now}` | "
                f"updated `{_format_dt_short(current.get('updated_at'))}`"
            )

        lines = [f"📡 **Live Intel • {real}**", now_line]
        if pie_label:
            lines.append(f"Pie detail: {pie_label}")

        if baseline:
            nw_delta = delta.get("networth")
            rank_from = baseline.get("rank")
            rank_to = current.get("rank")
            sign = "+" if (nw_delta or 0) >= 0 else "-"
            rank_txt = "rank n/a"
            if rank_from and rank_to:
                rank_txt = f"rank #{int(rank_from)} -> #{int(rank_to)}"
            pie_change = str(delta.get("pie_change") or "unknown")
            nw_txt = "NW n/a"
            if nw_delta is not None and baseline.get("networth") is not None and current.get("networth") is not None:
                nw_txt = (
                    f"NW {sign}`{fmt_int(abs(int(nw_delta)))}` "
                    f"({fmt_int(baseline.get('networth'))} -> {fmt_int(current.get('networth'))})"
                )
            lines.append(
                f"{int(profile.get('lookback_hours') or hours)}h change: {nw_txt} | {rank_txt} | pie `{pie_change}`"
            )
        else:
            lines.append(f"{int(profile.get('lookback_hours') or hours)}h change: not enough rankings history yet.")

        if int(window.get("poll_count") or 0) > 0:
            best_rank = window.get("best_rank")
            worst_rank = window.get("worst_rank")
            if best_rank and worst_rank:
                rank_range = f"rank range `#{int(best_rank)}` - `#{int(worst_rank)}`"
            else:
                rank_range = "rank range `n/a`"
            lines.append(
                f"Window: `{int(window.get('poll_count') or 0)}` polls | "
                f"NW range `{fmt_int(window.get('min_networth'))}` - `{fmt_int(window.get('max_networth'))}` | "
                f"{rank_range}"
            )

        if latest_spy:
            lines.append(
                f"Latest SR: `#{int(latest_spy.get('id') or 0)}` at `{_format_dt_short(latest_spy.get('created_at'))}` | "
                f"DP `{fmt_int(latest_spy.get('defense_power'))}` | Castles `{fmt_int(latest_spy.get('castles'))}`"
            )
        else:
            lines.append("Latest SR: none saved yet.")

        lines.append(
            f"{int(attacks.get('hours') or KINGDOM_LIVE_ATTACK_WINDOW_HOURS or 24)}h attacks: "
            f"out `{int(attacks.get('outgoing_hits') or 0)}` | in `{int(attacks.get('incoming_hits') or 0)}` | "
            f"land gained `{fmt_int(attacks.get('land_gained'))}` | land lost `{fmt_int(attacks.get('land_lost'))}`"
        )

        if battle_est:
            lines.append(f"Estimated home now: {fmt_units_short(battle_est.get('estimated_home') or {}, limit=6)}")

        if oven_est:
            lines.extend(format_oven_summary_lines(oven_est, limit=2, compact=True))

        for chunk in split_for_discord("\n".join(lines), 1900):
            await ctx.send(chunk)
    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("⚠️ kingdomlive failed.")
        await send_error(ctx.guild, f"kingdomlive error: {e}", tb=tb)


@bot.command()
async def track(ctx, *, arg: str = None):
    """
    !track
    !track today
    !track yesterday
    !track 2026-02-13
    !track Galileo
    !track Galileo 2026-02-13
    """
    try:
        raw = (arg or "").strip()
        kingdom = None
        day_token = None

        if raw:
            parts = raw.split()
            if len(parts) == 1:
                p = parts[0].strip()
                if p.lower() in ("today", "yesterday") or re.match(r"^\d{4}-\d{2}-\d{2}$", p):
                    day_token = p
                else:
                    kingdom = p
            elif len(parts) >= 2:
                p1 = parts[0].strip()
                p2 = parts[-1].strip()
                if p1.lower() in ("today", "yesterday") or re.match(r"^\d{4}-\d{2}-\d{2}$", p1):
                    day_token = p1
                    kingdom = " ".join(parts[1:]).strip() or None
                elif p2.lower() in ("today", "yesterday") or re.match(r"^\d{4}-\d{2}-\d{2}$", p2):
                    day_token = p2
                    kingdom = " ".join(parts[:-1]).strip() or None
                else:
                    kingdom = raw

        day_start = parse_track_day_arg(day_token)
        day_end = day_start + timedelta(days=1)

        real_kingdom = None
        if kingdom:
            real_kingdom = await run_db(sync_fuzzy_kingdom, kingdom)
            real_kingdom = real_kingdom or kingdom

        rows = await run_db(sync_get_attack_rows_for_day, day_start, day_end, real_kingdom)
        if not rows:
            target_txt = f" for `{real_kingdom}`" if real_kingdom else ""
            return await ctx.send(f"No attack reports found{target_txt} on `{day_start.date()}` (UTC).")

        # Collapse likely duplicates from legacy ingests.
        deduped = {}
        for r in rows:
            smid = r.get("source_message_id")
            dt = r.get("happened_at")
            dt_key = dt.strftime("%Y-%m-%d %H:%M:%S") if dt else "none"
            key = (
                f"msg:{int(smid)}" if smid else
                f"legacy:{dt_key}|{str(r.get('attacker') or '').strip().lower()}|"
                f"{str(r.get('defender') or '').strip().lower()}|{str(r.get('attack_result') or '').strip().lower()}|"
                f"{str(r.get('settlements_lost') or '').strip().lower()}"
            )
            cur = deduped.get(key)
            if not cur:
                deduped[key] = r
                continue
            # Keep lower positive land if same apparent event, to avoid NW misparse inflation.
            new_land = int(r.get("land_taken") or 0)
            cur_land = int(cur.get("land_taken") or 0)
            if cur_land <= 0 and new_land > 0:
                deduped[key] = r
            elif new_land > 0 and cur_land > 0 and new_land < cur_land:
                deduped[key] = r

        rows = list(deduped.values())

        agg = {}
        for r in rows:
            defender = (r.get("defender") or "Unknown").strip() or "Unknown"
            key = defender.lower()
            if key not in agg:
                agg[key] = {
                    "defender": defender,
                    "hits": 0,
                    "land_lost": 0,
                    "setty_lost": 0,
                    "setty_notes": set(),
                }
            agg[key]["hits"] += 1
            agg[key]["land_lost"] += int(r.get("land_taken") or 0)
            agg[key]["setty_lost"] += int(r.get("settlements_lost_count") or 0)
            note = (r.get("settlements_lost") or "").strip()
            if note:
                agg[key]["setty_notes"].add(note)

        summary_rows = sorted(
            agg.values(),
            key=lambda x: (int(x["land_lost"]), int(x["setty_lost"]), int(x["hits"])),
            reverse=True,
        )
        lines = []
        for s in summary_rows[:25]:
            extra = ""
            if s["setty_notes"]:
                sample = sorted(list(s["setty_notes"]))[0]
                extra = f" | Setty: {sample}"
            lines.append(
                f"- {s['defender']} | Hits `{s['hits']}` | Land Lost `{fmt_int(s['land_lost'])}` | "
                f"Setty Lost `{s['setty_lost']}`{extra}"
            )

        out = io.StringIO()
        writer = csv.writer(out, delimiter="\t", lineterminator="\n")
        writer.writerow([
            "date_utc",
            "report_id",
            "attacker",
            "defender",
            "result",
            "land_taken",
            "settlements_lost_count",
            "settlements_lost",
        ])
        for r in rows:
            dt = r.get("happened_at")
            writer.writerow([
                dt.isoformat() if dt else "",
                r.get("id") or "",
                r.get("attacker") or "",
                r.get("defender") or "",
                r.get("attack_result") or "",
                int(r.get("land_taken") or 0),
                int(r.get("settlements_lost_count") or 0),
                r.get("settlements_lost") or "",
            ])
        payload = out.getvalue()
        out.close()

        head = (
            f"Daily Attack Track - {day_start.date()} (UTC)"
            + (f" | Filter: `{real_kingdom}`" if real_kingdom else "")
            + f"\nReports: `{len(rows)}` | Defenders hit: `{len(summary_rows)}`"
        )
        await ctx.send(head + "\n" + "\n".join(lines))

        preview = payload.strip()
        if len(preview) <= 1500:
            await ctx.send(f"```tsv\n{preview}\n```")
        else:
            await ctx.send(
                "```tsv\n"
                + "\n".join(payload.splitlines()[:15])
                + "\n... (truncated in chat; full TSV attached)\n```"
            )

        fname = f"kg2_track_{day_start.strftime('%Y%m%d')}.tsv"
        await ctx.send(file=discord.File(fp=io.BytesIO(payload.encode("utf-8")), filename=fname))

    except ValueError:
        await ctx.send("Invalid date. Use `YYYY-MM-DD`, `today`, or `yesterday`.")
    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("track failed.")
        await send_error(ctx.guild, f"track error: {e}", tb=tb)


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
async def techindex(ctx, days: int = None):
    """!techindex [days] -> ingest Discord history into DB, then index battle tech from saved reports."""
    if not _is_admin(ctx):
        return await ctx.send("❌ Admin only.")
    try:
        if days and int(days) > 0:
            await ctx.send(f"🔎 Pulling reports from Discord history (last `{int(days)}` days), then rebuilding tech index…")
        else:
            await ctx.send("🔎 Pulling reports from Discord history (all readable channels), then rebuilding tech index…")

        ingest = await sync_ingest_history(int(days) if days else None)
        stats = await run_db(sync_techindex_all, int(days) if days else None)
        await ctx.send(
            "✅ **Tech index complete**\n"
            f"Guilds scanned: `{ingest['guilds']}` • Channels scanned: `{ingest['channels_scanned']}`\n"
            f"Messages scanned: `{ingest['messages_scanned']}` • Matched reports: `{ingest['messages_matched']}`\n"
            f"New reports saved: `{ingest['reports_saved']}` • Duplicates seen: `{ingest['duplicates']}`\n"
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
async def techcsv(ctx):
    """!techcsv -> export indexed kingdom research to CSV and upload file."""
    if not _is_admin(ctx):
        return await ctx.send("❌ Admin only.")
    try:
        rows = await run_db(sync_get_research_export_rows)
        if not rows:
            return await ctx.send("❌ No spy reports found in DB yet.")

        out = io.StringIO()
        writer = csv.writer(out)
        writer.writerow([
            "kingdom",
            "latest_spy_report_id",
            "latest_spy_at_utc",
            "tech_name",
            "best_level",
            "tech_updated_at_utc",
            "source_report_id",
            "indexed_hits",
        ])

        for r in rows:
            writer.writerow([
                r.get("kingdom") or "",
                r.get("latest_report_id") or "",
                (r.get("latest_report_at").isoformat() if r.get("latest_report_at") else ""),
                r.get("tech_name") or "",
                r.get("best_level") if r.get("best_level") is not None else "",
                (r.get("tech_updated_at").isoformat() if r.get("tech_updated_at") else ""),
                r.get("source_report_id") or "",
                int(r.get("indexed_hits") or 0),
            ])

        payload = out.getvalue().encode("utf-8")
        out.close()

        kingdoms = len({(r.get("kingdom") or "").strip().lower() for r in rows if r.get("kingdom")})
        ts = now_utc().strftime("%Y%m%d_%H%M%S")
        filename = f"kg2_research_export_{ts}.csv"

        await ctx.send(
            f"📄 Export ready: `{filename}`\n"
            f"Kingdoms: `{kingdoms}` • Rows: `{len(rows)}`",
            file=discord.File(fp=io.BytesIO(payload), filename=filename)
        )
    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("⚠️ techcsv failed.")
        await send_error(ctx.guild, f"techcsv error: {e}", tb=tb)


@bot.command(name="reportscsv")
async def reportscsv(ctx):
    """!reportscsv -> admin export of all saved spy + attack report metadata as CSV files."""
    if not _is_admin(ctx):
        return await ctx.send("❌ Admin only.")
    try:
        spy_rows = await run_db(sync_get_all_spy_report_export_rows, 200000)
        atk_rows = await run_db(sync_get_all_attack_report_export_rows, 200000)

        if not spy_rows and not atk_rows:
            return await ctx.send("❌ No saved spy/attack reports found in DB yet.")

        ts = now_utc().strftime("%Y%m%d_%H%M%S")

        spy_buf = io.StringIO()
        sw = csv.writer(spy_buf)
        sw.writerow(["id", "kingdom", "defense_power", "castles", "created_at_utc", "report_hash"])
        for r in spy_rows:
            sw.writerow([
                r.get("id") or "",
                r.get("kingdom") or "",
                r.get("defense_power") if r.get("defense_power") is not None else "",
                r.get("castles") if r.get("castles") is not None else "",
                r.get("created_at").isoformat() if r.get("created_at") else "",
                r.get("report_hash") or "",
            ])
        spy_payload = spy_buf.getvalue().encode("utf-8")
        spy_buf.close()

        atk_buf = io.StringIO()
        aw = csv.writer(atk_buf)
        aw.writerow([
            "id", "attacker", "defender", "attack_result", "land_taken", "settlements_lost_count",
            "settlements_lost", "reported_at_utc", "created_at_utc", "report_hash", "source_message_id", "source_channel_id"
        ])
        for r in atk_rows:
            aw.writerow([
                r.get("id") or "",
                r.get("attacker") or "",
                r.get("defender") or "",
                r.get("attack_result") or "",
                r.get("land_taken") if r.get("land_taken") is not None else "",
                r.get("settlements_lost_count") if r.get("settlements_lost_count") is not None else "",
                r.get("settlements_lost") or "",
                r.get("reported_at").isoformat() if r.get("reported_at") else "",
                r.get("created_at").isoformat() if r.get("created_at") else "",
                r.get("report_hash") or "",
                r.get("source_message_id") if r.get("source_message_id") is not None else "",
                r.get("source_channel_id") if r.get("source_channel_id") is not None else "",
            ])
        atk_payload = atk_buf.getvalue().encode("utf-8")
        atk_buf.close()

        spy_name = f"kg2_spy_reports_{ts}.csv"
        atk_name = f"kg2_attack_reports_{ts}.csv"
        await ctx.send(
            f"📄 Export ready • Spy rows: `{len(spy_rows)}` • Attack rows: `{len(atk_rows)}`",
            files=[
                discord.File(fp=io.BytesIO(spy_payload), filename=spy_name),
                discord.File(fp=io.BytesIO(atk_payload), filename=atk_name),
            ],
        )
    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("⚠️ reportscsv failed.")
        await send_error(ctx.guild, f"reportscsv error: {e}", tb=tb)


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
    Pulls old Discord history into DB, then ensures research + troops are accounted for.
    - If days provided: only last N days of history/reports
    - Else: full readable history + whole DB
    """
    if not _is_admin(ctx):
        return await ctx.send("❌ Admin only.")

    progress_id = None
    try:
        def _bar(pct: float, width: int = 20) -> str:
            p = max(0.0, min(100.0, float(pct or 0.0)))
            filled = int(round((p / 100.0) * width))
            return "[" + ("#" * filled) + ("-" * max(0, width - filled)) + "]"

        started = time.monotonic()
        spinner = ["|", "/", "-", "\\"]

        if days and int(days) > 0:
            status = await ctx.send(f"🧱 Backfilling last **{int(days)}** days from Discord history, then reprocessing reports (tech + troops)…")
        else:
            status = await ctx.send("🧱 Backfilling **ALL** readable Discord history, then reprocessing reports (tech + troops)…")

        # Phase 1: Discord history ingest (indeterminate progress with live heartbeat).
        ingest_task = asyncio.create_task(sync_ingest_history(int(days) if days else None))
        spin_i = 0
        while not ingest_task.done():
            elapsed = int(time.monotonic() - started)
            try:
                await status.edit(
                    content=(
                        "🧱 **Backfill in progress**\n"
                        f"Phase 1/2: Scanning Discord history {spinner[spin_i % len(spinner)]}\n"
                        f"Elapsed: `{elapsed}s`"
                    )
                )
            except Exception:
                pass
            spin_i += 1
            await asyncio.sleep(4)

        ingest = await ingest_task

        # Phase 2: DB reprocess with real percentage bar.
        progress_id = f"bf:{int(time.time() * 1000)}:{int(getattr(ctx.guild, 'id', 0) or 0)}:{int(getattr(ctx.channel, 'id', 0) or 0)}"
        db_task = asyncio.create_task(run_db(sync_backfill, int(days) if days else None, progress_id))

        while not db_task.done():
            p = BACKFILL_PROGRESS.get(progress_id) or {}
            done = int(p.get("done") or 0)
            total = int(p.get("total") or 0)
            pct = (100.0 * done / total) if total > 0 else 0.0
            elapsed = int(time.monotonic() - started)
            try:
                await status.edit(
                    content=(
                        "🧱 **Backfill in progress**\n"
                        "Phase 1/2: Discord history scan complete\n"
                        f"Phase 2/2: Reprocessing saved reports `{done}`/`{total}` { _bar(pct) } `{pct:.1f}%`\n"
                        f"Elapsed: `{elapsed}s`"
                    )
                )
            except Exception:
                pass
            await asyncio.sleep(3)

        stats = await db_task
        BACKFILL_PROGRESS.pop(progress_id, None)

        reason_items = sorted((ingest.get("forward_failure_reasons") or {}).items(), key=lambda kv: int(kv[1]), reverse=True)
        reason_txt = ", ".join([f"{k}:{int(v)}" for k, v in reason_items[:3]]) if reason_items else "n/a"

        await status.edit(content=(
            "✅ **Backfill complete**\n"
            f"Guilds scanned: `{ingest['guilds']}` • Channels scanned: `{ingest['channels_scanned']}`\n"
            f"Messages scanned: `{ingest['messages_scanned']}` • Matched reports: `{ingest['messages_matched']}`\n"
            f"New spy reports saved: `{ingest['reports_saved']}` • Spy duplicates: `{ingest['duplicates']}`\n"
            f"New attack reports saved: `{ingest['attack_reports_saved']}` • Attack duplicates: `{ingest['attack_duplicates']}`\n"
            f"Forwarded to recon-hub: `{ingest['reports_forwarded']}` • Forward skipped: `{ingest.get('forward_skipped', 0)}` • Forward failures: `{ingest['forward_failures']}`\n"
            f"Forward failure reasons (top): `{reason_txt}`\n"
            f"Ingest errors: `{ingest['ingest_errors']}`\n"
            f"Reports scanned: `{stats['reports_scanned']}`\n"
            f"Tech reports: `{stats['tech_reports']}` • Tech lines indexed: `{stats['tech_history_rows']}` • Best updates: `{stats['best_updates']}`\n"
            f"Troop reports: `{stats['troop_reports']}` • Troop rows inserted: `{stats['troop_rows']}`\n"
            f"Market reports: `{stats['market_reports']}` • Market rows inserted: `{stats['market_rows']}`"
        ))

    except Exception as e:
        # Best-effort cleanup of progress entries on command failure.
        if progress_id:
            BACKFILL_PROGRESS.pop(progress_id, None)
        tb = traceback.format_exc()
        await ctx.send("⚠️ backfill failed.")
        await send_error(ctx.guild, f"backfill error: {e}", tb=tb)


@bot.command()
async def attackbackfill(ctx, days: int = None):
    """
    !attackbackfill [days]
    Pulls Discord history and backfills attack reports used by !track.
    - If days provided: only last N days
    - Else: full readable history
    """
    if not _is_admin(ctx):
        return await ctx.send("❌ Admin only.")

    try:
        if days and int(days) > 0:
            await ctx.send(f"🧱 Backfilling attack reports from the last **{int(days)}** days...")
        else:
            await ctx.send("🧱 Backfilling attack reports from **ALL** readable Discord history...")

        stats = await sync_ingest_history(int(days) if days else None)
        reason_items = sorted((stats.get("forward_failure_reasons") or {}).items(), key=lambda kv: int(kv[1]), reverse=True)
        reason_txt = ", ".join([f"{k}:{int(v)}" for k, v in reason_items[:3]]) if reason_items else "n/a"
        await ctx.send(
            "✅ **Attack backfill complete**\n"
            f"Guilds scanned: `{stats['guilds']}` • Channels scanned: `{stats['channels_scanned']}`\n"
            f"Messages scanned: `{stats['messages_scanned']}` • Matched reports: `{stats['messages_matched']}`\n"
            f"Attack reports saved: `{stats['attack_reports_saved']}` • Attack duplicates: `{stats['attack_duplicates']}`\n"
            f"Forwarded to recon-hub: `{stats['reports_forwarded']}` • Forward skipped: `{stats.get('forward_skipped', 0)}` • Forward failures: `{stats['forward_failures']}`\n"
            f"Forward failure reasons (top): `{reason_txt}`\n"
            f"Ingest errors: `{stats['ingest_errors']}`"
        )
    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("⚠️ attackbackfill failed.")
        await send_error(ctx.guild, f"attackbackfill error: {e}", tb=tb)


@bot.command(name="oven")
async def oven(ctx, *, kingdom: str):
    """!oven <kingdom> -> infer likely troops in training from last two SR snapshots."""
    try:
        real = await run_db(sync_fuzzy_kingdom, kingdom)
        real = real or kingdom
        est = await run_db(sync_build_oven_estimate, real, None, None)
        if not est.get("ok"):
            reason = str(est.get("reason") or "unknown")
            if reason == "need_two_recent_sr":
                return await ctx.send(
                    f"❌ Need at least **2 recent SR reports** for **{real}** within `{OVEN_LOOKBACK_HOURS}` hours. Paste fresh SRs first."
                )
            if reason == "missing_peasant_signal":
                return await ctx.send(f"❌ Could not find Peasants or Population lines in the last two recent SR reports for **{real}**.")
            if reason == "no_missing_peasants":
                return await ctx.send(
                    f"ℹ️ No missing peasants/population detected for **{real}** between the last two SRs "
                    f"({fmt_int(est.get('old_peasants'))} → {fmt_int(est.get('new_peasants'))})."
                )
            return await ctx.send(f"❌ Oven estimate unavailable for **{real}** (`{reason}`).")

        lines = format_oven_summary_lines(est, limit=int(OVEN_MAX_RESULTS or 6), compact=False)
        lines.append("Tune constants with Railway env vars like `OVEN_HEAVY_CAVALRY_NW` and `OVEN_HEAVY_CAVALRY_MINUTES_PER_1000` as game values are verified.")
        for chunk in split_for_discord("\n".join(lines), 1900):
            await ctx.send(chunk)
    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("⚠️ oven failed.")
        await send_error(ctx.guild, f"oven error: {e}", tb=tb)


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


@bot.command(name="help")
async def help_cmd(ctx):
    """!help -> list all available bot commands."""
    try:
        lines = [
            "**KG2 Recon Bot Commands**",
            "`!help` - Show this command list",
            "`!calc` - Paste mode calculator (prompts for report)",
            "`!calc <kingdom>` - Run calc from latest saved DP report for a kingdom",
            "`!calc db` - Run calc from latest saved DP report overall",
            "`!spy <kingdom>` - Show latest saved spy report summary",
            "`!spyid <id>` - Show saved spy report by DB ID",
            "`!spyhistory <kingdom>` - Show last 5 saved spy reports",
            "`!spies <kingdom>` - Show last 10 spy reports + send recommendation",
            "`!kingdomlive <kingdom> [hours]` - Live rankings profile with lookback deltas, pie, SR, and attack summary",
            "`!supply <kingdom> [days]` - Premium: show supplier kingdoms from market transactions + TSV",
            "`!nwjumpalerts status` - Show NW jump alert config for this server",
            "`!nwjumpalerts on [threshold]` - Admin: enable rankings alerts in this channel (NW jumps + pie changes)",
            "`!nwjumpalerts addhere` - Admin: add current room to NW jump alert fanout",
            "`!nwjumpalerts removehere` - Admin: remove current room from NW jump alert fanout",
            "`!nwjumpalerts off` - Admin: disable NW jump alerts for this server",
            "`!nwjumpcheck` - Admin: run live rankings check + show alert pipeline status",
            "`!rankingsrefresh` - Admin: force a live top-100 rankings refresh into DB/history now",
            "`!nwjumptestalert` - Admin: send a marked test NW jump alert to all configured rooms",
            "`!nwjumppulltest` - Admin: pull rankings now and print top sample rows",
            "`!whereupdates` - Show configured target server/channel + bot send permissions",
            "`!battle <kingdom>` - Estimate current home troops (available when troop tracking is enabled)",
            "`!track` - Daily attack tracker for today (UTC) + TSV export",
            "`!track yesterday` - Daily attack tracker for yesterday (UTC)",
            "`!track YYYY-MM-DD` - Daily attack tracker for a specific date (UTC)",
            "`!track <kingdom>` - Daily attack tracker filtered to a kingdom (UTC today)",
            "`!track <kingdom> YYYY-MM-DD` - Daily attack tracker for one kingdom on a specific date (UTC)",
            "`!ap <kingdom>` - AP planner with hit buttons",
            "`!apstatus <kingdom>` - Read-only AP planner status",
            "`!techindex [days]` - Admin: pull history + rebuild indexed battle tech",
            "`!tech <kingdom>` - Show indexed battle tech for a kingdom",
            "`!techtop` - Show most common indexed training names",
            "`!techcsv` - Admin: export indexed tech CSV",
            "`!reportscsv` - Admin: export all saved spy + attack report metadata CSVs",
            "`!techpull <kingdom>` - Rebuild indexed tech for one kingdom",
            "`!backfill [days]` - Admin: reprocess saved reports for indexing",
            "`!attackbackfill [days]` - Admin: pull attack/spy reports from Discord history for track data",
            "`!oven <kingdom>` - Estimate likely troops in training from SR peasant loss + NW delta",
            "`!troops <kingdom>` - Latest saved troop snapshot",
            "`!troopsdelta <kingdom>` - Troop delta from last two snapshots",
            "`!troopdelta <kingdom>` - Alias of !troopsdelta",
            "`!announcepatch` - Admin: force-post current patch notes to update channel",
            "`!refresh` - Admin: restart bot process",
        ]
        for chunk in split_for_discord("\n".join(lines), 1900):
            await ctx.send(chunk)
    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("help failed.")
        if ctx.guild:
            await send_error(ctx.guild, f"help error: {e}", tb=tb)


@bot.command(name="nwjumpalerts")
async def nwjumpalerts(ctx, action: str = "status", *, arg: str = ""):
    """
    Configure per-server NW jump alerts sourced from GetKingdomRankings.
    Usage:
    !nwjumpalerts status
    !nwjumpalerts on [threshold]
    !nwjumpalerts off
    """
    try:
        if not ctx.guild:
            return await ctx.send("❌ This command can only be used in a server channel.")
        if not is_target_guild(ctx.guild):
            return await ctx.send(f"❌ This bot is configured for server `{TARGET_GUILD_ID}` only.")

        action_norm = str(action or "status").strip().lower()
        guild_id = int(ctx.guild.id)

        if action_norm in ("status", "show"):
            row = await run_db(sync_get_nw_jump_subscription, guild_id)
            if not row:
                return await ctx.send(
                    f"ℹ️ Rankings alerts are **not configured** for this server. "
                    f"Use `!nwjumpalerts on {NW_JUMP_ALERT_DEFAULT_THRESHOLD}` in the channel that should receive alerts."
                )
            enabled = bool(row.get("enabled"))
            cid = int(row.get("channel_id") or 0)
            threshold_v = max(1, int(row.get("min_jump") or NW_JUMP_ALERT_DEFAULT_THRESHOLD))
            extra_channels = await run_db(sync_get_nw_jump_channels, guild_id)
            ch_text = f"<#{cid}>" if cid > 0 else "(unknown channel)"
            state = "enabled" if enabled else "disabled"
            extra_txt = ", ".join([f"<#{int(x)}>" for x in extra_channels]) if extra_channels else "(none)"
            return await ctx.send(
                f"📡 Rankings alerts are **{state}** for this server.\n"
                f"Channel: {ch_text}\n"
                f"NW threshold: `{fmt_int(threshold_v)}`\n"
                f"Pie alerts: `{'enabled' if KG_GAME_PIE_ALERTS_ENABLED else 'disabled'}`\n"
                f"Fanout rooms: {extra_txt}"
            )

        if action_norm == "addhere":
            if not _is_admin(ctx):
                return await ctx.send("❌ You don’t have permission to use this command.")
            await run_db(sync_add_nw_jump_channel, guild_id, int(ctx.channel.id))
            return await ctx.send(f"✅ Added {ctx.channel.mention} to NW jump alert fanout rooms.")

        if action_norm == "removehere":
            if not _is_admin(ctx):
                return await ctx.send("❌ You don’t have permission to use this command.")
            changed = await run_db(sync_remove_nw_jump_channel, guild_id, int(ctx.channel.id))
            if changed <= 0:
                return await ctx.send("ℹ️ This room was not in the NW jump fanout list.")
            return await ctx.send(f"🛑 Removed {ctx.channel.mention} from NW jump alert fanout rooms.")

        if action_norm == "off":
            if not _is_admin(ctx):
                return await ctx.send("❌ You don’t have permission to use this command.")
            changed = await run_db(sync_disable_nw_jump_subscription, guild_id)
            if changed <= 0:
                return await ctx.send("ℹ️ NW jump alerts were already off (or not configured) for this server.")
            return await ctx.send("🛑 NW jump alerts disabled for this server.")

        if action_norm == "on":
            if not _is_admin(ctx):
                return await ctx.send("❌ You don’t have permission to use this command.")
            min_jump = _safe_int_or_none(arg)
            if min_jump is None:
                min_jump = int(NW_JUMP_ALERT_DEFAULT_THRESHOLD or 5000)
            min_jump = max(1, int(min_jump))
            await run_db(
                sync_upsert_nw_jump_subscription,
                guild_id,
                int(ctx.channel.id),
                min_jump,
                True,
            )
            await run_db(sync_add_nw_jump_channel, guild_id, int(ctx.channel.id))
            return await ctx.send(
                f"✅ Rankings alerts enabled in {ctx.channel.mention} with NW threshold `{fmt_int(min_jump)}`.\n"
                "Alerts trigger when a kingdom gains at least this much net worth between ranking polls, and also when pie status appears or changes in rankings."
            )

        return await ctx.send(
            "Usage: `!nwjumpalerts status` | `!nwjumpalerts on [threshold]` | "
            "`!nwjumpalerts addhere` | `!nwjumpalerts removehere` | `!nwjumpalerts off`"
        )

    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("⚠️ nwjumpalerts failed.")
        if ctx.guild:
            await send_error(ctx.guild, f"nwjumpalerts error: {e}", tb=tb)


@bot.command(name="nwjumpcheck")
async def nwjumpcheck(ctx):
    """Admin-only: run a live rankings pull and summarize NW jump pipeline health."""
    try:
        async def _send_with_fallback(text: str):
            chunks = split_for_discord(str(text or ""), 1800)
            sent = False
            for chunk in chunks:
                try:
                    await ctx.send(chunk)
                    sent = True
                except Exception:
                    sent = False
                    break
            if sent:
                return True

            if ctx.guild:
                ch = get_updates_channel(ctx.guild, ctx.channel)
                if ch and can_send(ch, ctx.guild):
                    for chunk in chunks:
                        await ch.send(chunk)
                    return True
            return False

        if not _is_admin(ctx):
            return await _send_with_fallback("❌ You don’t have permission to use this command.")
        if ctx.guild and not is_target_guild(ctx.guild):
            return await _send_with_fallback(f"❌ This bot is configured for server `{TARGET_GUILD_ID}` only.")

        await _send_with_fallback("⏳ Running NW jump diagnostics now...")

        rows, dbg = await asyncio.to_thread(fetch_world_kingdom_rankings_debug)
        world_id = int(KG_GAME_WORLD_ID or 1)
        stats = await run_db(sync_get_nw_rankings_state_stats, world_id)

        poll_ts = await run_db(sync_meta_get, "nw_jump_last_poll_ts")
        poll_rows = await run_db(sync_meta_get, "nw_jump_last_poll_rows")
        ev_last = await run_db(sync_meta_get, "nw_jump_last_events")
        pie_last = await run_db(sync_meta_get, "nw_jump_last_pie_events")
        ok_ts = await run_db(sync_meta_get, "nw_jump_last_ok_ts")
        err_ts = await run_db(sync_meta_get, "nw_jump_last_error_ts")
        last_auth_mode = await run_db(sync_meta_get, "nw_jump_last_auth_mode")
        last_return_value = await run_db(sync_meta_get, "nw_jump_last_return_value")
        last_return_string = await run_db(sync_meta_get, "nw_jump_last_return_string")
        last_attempts = await run_db(sync_meta_get, "nw_jump_last_attempts")

        sub = await run_db(sync_get_nw_jump_subscription, int(ctx.guild.id)) if ctx.guild else None
        fanout = await run_db(sync_get_nw_jump_channels, int(ctx.guild.id)) if ctx.guild else []
        sms_recipients = _get_alert_sms_recipients()

        auth = await asyncio.to_thread(_kg_get_auth, False)
        auth_mode = str((auth or {}).get("auth_mode") or "none")
        has_auth = bool(auth and auth.get("account_id") and auth.get("token"))
        live_pie_count = sum(1 for r in (rows or []) if bool(r.get("pie_active")) or str(r.get("pie_label") or "").strip())

        lines = [
            "🧪 **NW Jump Check**",
            f"World: `{world_id}` | Rankings pulled now: `{len(rows or [])}` | Live pie detected: `{live_pie_count}`",
            f"Auth: `{auth_mode}` ({'ok' if has_auth else 'missing'})",
            f"Current state rows: `{fmt_int(stats.get('state_count'))}` | Last state update: `{stats.get('state_last_updated') or 'never'}`",
            f"Rankings history rows: `{fmt_int(stats.get('history_count'))}` | Last history snapshot: `{stats.get('history_last_snapshot') or 'never'}`",
            f"Loop last poll ts: `{poll_ts or 'n/a'}` | rows: `{poll_rows or 'n/a'}` | last NW events: `{ev_last or 'n/a'}` | last pie events: `{pie_last or 'n/a'}`",
            f"Loop last ok ts: `{ok_ts or 'n/a'}` | last error ts: `{err_ts or 'n/a'}`",
            f"Last API mode: `{last_auth_mode or 'n/a'}` | ReturnValue: `{last_return_value or 'n/a'}` | ReturnString: `{last_return_string or 'n/a'}`",
            f"This check attempts: `{json.dumps(dbg.get('attempts') or [])[:900]}`",
        ]

        if sub:
            lines.append(
                f"Guild subscription: `enabled={bool(sub.get('enabled'))}` channel=<#{int(sub.get('channel_id') or 0)}> threshold=`{fmt_int(sub.get('min_jump'))}`"
            )
            if fanout:
                lines.append("Fanout rooms: " + ", ".join([f"<#{int(x)}>" for x in fanout]))
            else:
                lines.append("Fanout rooms: (none)")
        else:
            lines.append("Guild subscription: `not configured` (run `!nwjumpalerts on 5000`)")

        if ALERT_SMS_ENABLED:
            lines.append(
                f"SMS: `enabled` provider=`twilio` recipients=`{len(sms_recipients)}` configured=`{_twilio_sms_configured()}`"
            )
        else:
            lines.append("SMS: `disabled`")

        preview = rows[:5]
        if preview:
            lines.append("Top rankings sample:")
            for r in preview:
                pie_text = f" | pie: {r.get('pie_label')}" if r.get("pie_label") else ""
                lines.append(f"- #{int(r.get('rank') or 0)} {r.get('kingdom_name')} (NW {fmt_int(r.get('networth'))}){pie_text}")

        await _send_with_fallback("\n".join(lines))
    except Exception as e:
        tb = traceback.format_exc()
        try:
            await ctx.send("⚠️ nwjumpcheck failed.")
        except Exception:
            if ctx.guild:
                ch = get_updates_channel(ctx.guild, ctx.channel)
                if ch and can_send(ch, ctx.guild):
                    await ch.send("⚠️ nwjumpcheck failed.")
        if ctx.guild:
            await send_error(ctx.guild, f"nwjumpcheck error: {e}", tb=tb)


@bot.command(name="kgauthtest")
async def kgauthtest(ctx):
    """Admin-only: probe KG login + a known-good authenticated endpoint to isolate auth vs endpoint issues."""
    try:
        if not _is_admin(ctx):
            return await ctx.send("❌ You don’t have permission to use this command.")

        def _probe() -> dict:
            out = {}
            # 1) Login with email/password
            login_data, login_dbg = _kg_webservice_post_debug(
                "User", "Login", {"email": KG_GAME_EMAIL, "password": KG_GAME_PASSWORD}
            )
            login_data = login_data or {}
            acct, tok = _kg_extract_auth_credentials(login_data)
            out["login_status"] = login_dbg.get("status")
            out["login_reason"] = login_dbg.get("reason")
            out["login_body"] = str(login_dbg.get("body_preview") or "")[:160]
            out["login_account_id"] = acct
            out["login_token_len"] = len(tok or "")

            # Decide which auth to use for endpoint probes
            account_id = acct or _safe_int_or_none(KG_GAME_ACCOUNT_ID)
            token = tok or str(KG_GAME_TOKEN or "")
            kid = _safe_int_or_none(KG_GAME_TOKEN_KINGDOM_ID) or 0
            out["used_account_id"] = account_id
            out["used_token_len"] = len(token or "")
            out["used_kingdom_id"] = kid

            # 2) SearchByName (known-good authenticated endpoint)
            search_data, search_dbg = _kg_webservice_post_debug(
                "Kingdoms", "SearchByName",
                {"accountId": str(account_id or 0), "token": token, "kingdomId": int(kid or 0), "searchTerm": "a"},
            )
            out["search_status"] = search_dbg.get("status")
            out["search_reason"] = search_dbg.get("reason")
            out["search_body"] = str(search_dbg.get("body_preview") or "")[:160]

            # 3) GetKingdomRankings with same auth (probe multiple request body shapes)
            rank_attempts = []
            rank_variants = [
                ("startNumber0", {"accountId": str(account_id or 0), "token": token, "kingdomId": int(kid or 0), "continentId": -1, "startNumber": 0}),
                ("no_startNumber", {"accountId": str(account_id or 0), "token": token, "kingdomId": int(kid or 0), "continentId": -1}),
                ("startNumber1", {"accountId": str(account_id or 0), "token": token, "kingdomId": int(kid or 0), "continentId": -1, "startNumber": 1}),
                ("startingRank0_legacy", {"accountId": str(account_id or 0), "token": token, "kingdomId": int(kid or 0), "continentId": -1, "startingRank": 0}),
            ]
            for variant_name, payload in rank_variants:
                rank_data, rank_dbg = _kg_webservice_post_debug("Kingdoms", "GetKingdomRankings", payload)
                _ = rank_data
                rank_attempts.append({
                    "variant": variant_name,
                    "status": rank_dbg.get("status"),
                    "reason": rank_dbg.get("reason"),
                    "body": str(rank_dbg.get("body_preview") or "")[:120],
                })

            best = next((a for a in rank_attempts if int(a.get("status") or 0) == 200), rank_attempts[0] if rank_attempts else {})
            out["rank_status"] = best.get("status")
            out["rank_reason"] = best.get("reason")
            out["rank_body"] = best.get("body")
            out["rank_attempts"] = rank_attempts
            return out

        r = await asyncio.to_thread(_probe)
        lines = [
            "🔐 **KG Auth Test**",
            f"Login: status=`{r.get('login_status')}` reason=`{r.get('login_reason')}` accountId=`{r.get('login_account_id')}` tokenLen=`{r.get('login_token_len')}`",
            f"Login body: `{r.get('login_body')}`",
            f"Using: accountId=`{r.get('used_account_id')}` tokenLen=`{r.get('used_token_len')}` kingdomId=`{r.get('used_kingdom_id')}`",
            f"SearchByName: status=`{r.get('search_status')}` reason=`{r.get('search_reason')}`",
            f"Search body: `{r.get('search_body')}`",
            f"GetKingdomRankings: status=`{r.get('rank_status')}` reason=`{r.get('rank_reason')}`",
            f"Rank body: `{r.get('rank_body')}`",
        ]
        attempts = r.get("rank_attempts") or []
        if attempts:
            lines.append("Rank attempts:")
            for a in attempts[:6]:
                lines.append(
                    f"- `{a.get('variant')}` status=`{a.get('status')}` reason=`{a.get('reason')}` body=`{a.get('body')}`"
                )
        await ctx.send("\n".join(lines))
    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("⚠️ kgauthtest failed.")
        if ctx.guild:
            await send_error(ctx.guild, f"kgauthtest error: {e}", tb=tb)


@bot.command(name="nwjumptestalert")
async def nwjumptestalert(ctx):
    """Admin-only: dispatch a test NW jump alert to verify channel/fanout delivery."""
    try:
        if not _is_admin(ctx):
            return await ctx.send("❌ You don’t have permission to use this command.")
        if not ctx.guild:
            return await ctx.send("❌ This command can only be used in a server channel.")
        if not is_target_guild(ctx.guild):
            return await ctx.send(f"❌ This bot is configured for server `{TARGET_GUILD_ID}` only.")

        sub = await run_db(sync_get_nw_jump_subscription, int(ctx.guild.id))
        if not sub or not bool(sub.get("enabled")):
            return await ctx.send("ℹ️ NW jump alerts are not enabled for this server. Run `!nwjumpalerts on 5000` first.")

        await ctx.send("🧪 Sending test NW jump alert to configured rooms now...")
        fake = {
            "kingdom_id": -1,
            "kingdom_name": "[TEST] Training Jump",
            "old_networth": 50000,
            "new_networth": 65000,
            "delta": 15000,
            "old_rank": 12,
            "new_rank": 10,
        }
        await send_nw_jump_alerts([fake])
        await ctx.send("✅ Test alert dispatched. Check your primary + fanout NW jump channels.")
    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("⚠️ nwjumptestalert failed.")
        if ctx.guild:
            await send_error(ctx.guild, f"nwjumptestalert error: {e}", tb=tb)


@bot.command(name="nwjumppulltest")
async def nwjumppulltest(ctx):
    """Admin-only: perform a live rankings pull and print top rows + API attempts."""
    try:
        async def _send_with_fallback(text: str):
            chunks = split_for_discord(str(text or ""), 1800)
            sent = False
            for chunk in chunks:
                try:
                    await ctx.send(chunk)
                    sent = True
                except Exception:
                    sent = False
                    break
            if sent:
                return True

            if ctx.guild:
                ch = get_updates_channel(ctx.guild, ctx.channel)
                if ch and can_send(ch, ctx.guild):
                    for chunk in chunks:
                        await ch.send(chunk)
                    return True
            return False

        if not _is_admin(ctx):
            return await _send_with_fallback("❌ You don’t have permission to use this command.")
        if ctx.guild and not is_target_guild(ctx.guild):
            return await _send_with_fallback(f"❌ This bot is configured for server `{TARGET_GUILD_ID}` only.")

        await _send_with_fallback("⏳ Running live rankings pull test now...")

        rows, dbg = await asyncio.to_thread(fetch_world_kingdom_rankings_debug)
        attempts = dbg.get("attempts") or []
        top = rows[:10]

        lines = [
            "🧪 **NW Jump Pull Test**",
            f"Bot version: `{BOT_VERSION}`",
            f"Rows pulled: `{len(rows)}`",
            f"Auth mode: `{dbg.get('auth_mode') or 'n/a'}`",
            f"Configured continent: `{dbg.get('configured_continent_id')}` | used: `{dbg.get('continent_id_used', 'n/a')}`",
            f"ReturnValue: `{dbg.get('return_value', 'n/a')}` | ReturnString: `{dbg.get('return_string', 'n/a')}`",
        ]

        if top:
            lines.append("Top sample rows:")
            for r in top:
                lines.append(
                    f"- #{int(r.get('rank') or 0)} {r.get('kingdom_name')} (id {int(r.get('kingdom_id') or 0)}) NW {fmt_int(r.get('networth'))}"
                )
        else:
            lines.append("Top sample rows: (none)")

        if attempts:
            lines.append(f"Attempts: `{json.dumps(attempts)[:1200]}`")

        await _send_with_fallback("\n".join(lines))
    except Exception as e:
        tb = traceback.format_exc()
        try:
            await ctx.send("⚠️ nwjumppulltest failed.")
        except Exception:
            if ctx.guild:
                ch = get_updates_channel(ctx.guild, ctx.channel)
                if ch and can_send(ch, ctx.guild):
                    await ch.send("⚠️ nwjumppulltest failed.")
        if ctx.guild:
            await send_error(ctx.guild, f"nwjumppulltest error: {e}", tb=tb)


@bot.command(name="rankingsrefresh")
async def rankingsrefresh(ctx):
    """Admin-only: force a live top-100 rankings refresh into DB/history now."""
    try:
        if not _is_admin(ctx):
            return await ctx.send("❌ You don’t have permission to use this command.")
        if ctx.guild and not is_target_guild(ctx.guild):
            return await ctx.send(f"❌ This bot is configured for server `{TARGET_GUILD_ID}` only.")

        await ctx.send("⏳ Refreshing live top-100 rankings now...")
        result = await run_rankings_refresh_cycle(dispatch_alerts=True)
        rows = list(result.get("rows") or [])
        dbg = result.get("debug") or {}
        nw_events = list(result.get("nw_events") or [])
        pie_events = list(result.get("pie_events") or [])
        pie_live = sum(1 for r in rows if bool(r.get("pie_active")) or str(r.get("pie_label") or "").strip())

        await ctx.send(
            "✅ **Rankings refresh complete**\n"
            f"Rows stored this pull: `{len(rows)}`\n"
            f"Live pie rows this pull: `{pie_live}`\n"
            f"NW alerts triggered: `{len(nw_events)}`\n"
            f"Pie changes detected: `{len(pie_events)}`\n"
            f"Auth mode: `{dbg.get('auth_mode') or 'n/a'}` | ReturnValue: `{dbg.get('return_value') or 'n/a'}`"
        )
    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("⚠️ rankingsrefresh failed.")
        if ctx.guild:
            await send_error(ctx.guild, f"rankingsrefresh error: {e}", tb=tb)


@bot.command(name="whereupdates")
async def whereupdates(ctx):
    """Show update-routing target and send permissions for current server."""
    try:
        if not ctx.guild:
            return await ctx.send("❌ This command can only be used in a server channel.")

        is_target = is_target_guild(ctx.guild)
        target_txt = f"{TARGET_GUILD_ID}" if int(TARGET_GUILD_ID or 0) > 0 else "(all guilds)"
        ch = get_updates_channel(ctx.guild, ctx.channel)
        can_here = can_send(ctx.channel, ctx.guild)
        ch_name = getattr(ch, "name", "none") if ch else "none"
        ch_id = int(getattr(ch, "id", 0) or 0) if ch else 0
        can_target = can_send(ch, ctx.guild) if ch else False

        await ctx.send(
            "📍 **Update Routing**\n"
            f"Current guild: `{ctx.guild.id}`\n"
            f"Target guild: `{target_txt}`\n"
            f"This guild is target: `{is_target}`\n"
            f"Configured updates name: `{UPDATES_CHANNEL_NAME}`\n"
            f"Configured updates id: `{int(UPDATES_CHANNEL_ID or 0)}`\n"
            f"Resolved updates channel: `<#{ch_id}>` ({ch_name})\n"
            f"Can send in resolved channel: `{can_target}`\n"
            f"Can send in current channel: `{can_here}`"
        )
    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("⚠️ whereupdates failed.")
        if ctx.guild:
            await send_error(ctx.guild, f"whereupdates error: {e}", tb=tb)


@bot.command(name="refresh")
async def refresh(ctx):
    """Admin-only manual restart; the hosting platform will restart the worker."""
    try:
        if not _is_admin(ctx):
            return await ctx.send("❌ You don’t have permission to use this command.")

        try:
            await ctx.send("🔄 Refreshing bot now… (manual restart)")
        except Exception:
            pass

        if ctx.guild:
            try:
                ch = get_updates_channel(ctx.guild, ctx.channel)
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


@bot.command(name="announcepatch")
async def announcepatch(ctx):
    """Admin-only: force post current version patch notes to updates channel."""
    try:
        if not _is_admin(ctx):
            return await ctx.send("❌ You don’t have permission to use this command.")
        if not ctx.guild:
            return await ctx.send("❌ This command can only be used in a server channel.")
        if not is_target_guild(ctx.guild):
            return await ctx.send(f"❌ This bot is configured for server `{TARGET_GUILD_ID}` only.")

        ch = get_updates_channel(ctx.guild, ctx.channel)
        if not ch:
            return await ctx.send(
                f"❌ Could not find a sendable updates channel. Expected name: `{ERROR_CHANNEL_NAME}`."
            )

        patch_lines = "\n".join([f"• {x}" for x in PATCH_NOTES])
        await ch.send(
            f"📢 **KG2 Recon Bot patch announcement**\n"
            f"Version: `{BOT_VERSION}`\n"
            f"Patch:\n{patch_lines}"
        )
        if int(getattr(ch, "id", 0) or 0) != int(getattr(ctx.channel, "id", 0) or 0):
            await ctx.send(f"✅ Posted patch announcement in {ch.mention}.")
        else:
            await ctx.send("✅ Posted patch announcement.")
    except Exception as e:
        tb = traceback.format_exc()
        await ctx.send("⚠️ announcepatch failed.")
        if ctx.guild:
            await send_error(ctx.guild, f"announcepatch error: {e}", tb=tb)


# ---------- START ----------
if __name__ == "__main__":
    bot.run(TOKEN)
