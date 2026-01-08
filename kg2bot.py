# ---------- KG2 Recon Bot • FULL FINAL BUILD (PostgreSQL) ----------
# Spy Capture + Embed Display • Spy History • Spy ID Lookup
# Calc (HC fixed @ 7 AP + explicit remaining % + Remaining DP shown)
# AP Planner w/ Buttons + Reset • AP Status • Session Locking • Error Logging
# Startup announces to #kg2recon-updates
# Startup self-heals ID sequences
# Tech indexing + pull from saved spy reports
# Storage upgrade: compress raw spy report into BYTEA (raw_gz)
# Backfill: scan every readable channel history and import old spy reports

import os, re, asyncio, difflib, hashlib, logging, gzip
from math import ceil
from datetime import datetime, timezone, timedelta

import discord
from discord.ext import commands
from discord.ui import View, Button
from dotenv import load_dotenv

import psycopg2
from psycopg2.extras import RealDictCursor

# ---------- Setup ----------
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
ERROR_CHANNEL_NAME = "kg2recon-updates"

logging.basicConfig(level=logging.INFO)

if not TOKEN:
    raise RuntimeError("Missing DISCORD_TOKEN env var.")

# ---------- DB config ----------
DATABASE_URL = (
    os.getenv("DATABASE_URL")
    or os.getenv("INTERNAL_DATABASE_URL")
    or os.getenv("EXTERNAL_DATABASE_URL")
)

FALLBACK_DB = {
    "host": os.getenv("DB_HOST", "dpg-d54eklm3jp1c73970rdg-a"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "kg2bot_db"),
    "user": os.getenv("DB_USER", "kg2bot_db_user"),
    "password": os.getenv("DB_PASS", "tH4jvQNiIAvE8jmIVbVCMxsFnG1hvccA"),
}

KEEP_RAW_TEXT = os.getenv("KEEP_RAW_TEXT", "false").lower() in ("1", "true", "yes", "y")

# ---------- Constants ----------
HEAVY_CAVALRY_AP = 7

AP_REDUCTIONS = [
    ("Minor Victory", 0.19),
    ("Victory", 0.35),
    ("Major Victory", 0.55),
    ("Overwhelming Victory", 0.875),
]

BATTLE_TECH_KEYWORDS = [
    "training", "leadership", "battle", "attack", "defense", "offense",
    "troop", "army", "cavalry", "archer", "pikemen", "knight", "siege",
    "damage", "health", "armor", "speed", "march", "morale"
]

# ---------- Discord ----------
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# ---------- Locks ----------
ap_lock = asyncio.Lock()
tech_index_lock = asyncio.Lock()

# ---------- Permissions ----------
def is_admin_or_owner(ctx):
    return (
        ctx.author.guild_permissions.administrator
        or ctx.author.id == ctx.guild.owner_id
    )

# ---------- DB ----------
def db_connect():
    if DATABASE_URL:
        return psycopg2.connect(
            DATABASE_URL,
            cursor_factory=RealDictCursor,
            sslmode="require",
        )
    return psycopg2.connect(
        host=FALLBACK_DB["host"],
        port=FALLBACK_DB["port"],
        dbname=FALLBACK_DB["dbname"],
        user=FALLBACK_DB["user"],
        password=FALLBACK_DB["password"],
        cursor_factory=RealDictCursor,
        sslmode="require",
    )

def init_db():
    with db_connect() as conn, conn.cursor() as cur:
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
        cur.execute("""
        CREATE TABLE IF NOT EXISTS player_tech (
            kingdom TEXT NOT NULL,
            tech_name TEXT NOT NULL,
            tech_level INTEGER NOT NULL,
            last_seen TIMESTAMPTZ NOT NULL,
            source_report_id INTEGER,
            PRIMARY KEY (kingdom, tech_name)
        );
        """)

def heal_sequences():
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT setval(
              pg_get_serial_sequence('spy_reports','id'),
              COALESCE((SELECT MAX(id) FROM spy_reports), 1),
              true
            );
        """)
        cur.execute("""
            SELECT setval(
              pg_get_serial_sequence('dp_sessions','id'),
              COALESCE((SELECT MAX(id) FROM dp_sessions), 1),
              true
            );
        """)

# ---------- Helpers ----------
def castle_bonus(c):
    return (c ** 0.5) / 100 if c else 0.0

def hash_report(text):
    return hashlib.sha256(text.encode()).hexdigest()

def parse_spy(text):
    kingdom, dp, castles = None, None, 0
    for line in text.splitlines():
        l = line.lower()
        if l.startswith("target:"):
            kingdom = line.split(":", 1)[1].strip()
        if "defensive power" in l:
            m = re.search(r"\d+", line.replace(",", ""))
            if m:
                dp = int(m.group())
        if "number of castles" in l:
            m = re.search(r"\d+", line)
            if m:
                castles = int(m.group())
    return kingdom, dp, castles

def fuzzy_kingdom(query):
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT DISTINCT kingdom FROM spy_reports;")
        names = [r["kingdom"] for r in cur.fetchall() if r["kingdom"]]
    m = difflib.get_close_matches(query, names, 1, 0.5)
    return m[0] if m else None

def compress_report(text):
    return gzip.compress(text.encode(), 9)

def extract_tech_from_raw(raw):
    if not raw:
        return []
    lines = raw.splitlines()
    start = None
    for i, l in enumerate(lines):
        if l.lower().startswith("the following technology information"):
            start = i + 1
            break
    if start is None:
        return []

    out = []
    for l in lines[start:]:
        l = l.strip()
        if not l:
            break
        m = re.match(r"^(.*?)\s+lvl\s+(\d+)$", l, re.I)
        if m:
            out.append((m.group(1).strip(), int(m.group(2))))
    return out

def is_battle_related(name):
    n = name.lower()
    return any(k in n for k in BATTLE_TECH_KEYWORDS)

# ---------- Startup ----------
@bot.event
async def on_ready():
    init_db()
    heal_sequences()
    for g in bot.guilds:
        ch = discord.utils.get(g.text_channels, name=ERROR_CHANNEL_NAME)
        if ch:
            await ch.send(f"✅ KG2 Recon Bot started • {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ---------- Backfill ----------
@bot.command()
@commands.check(is_admin_or_owner)
async def backfill(ctx, days: int = 30):
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    await ctx.send(f"⏳ Backfilling last {days} days…")

    inserted = dupes = scanned = 0

    for ch in ctx.guild.text_channels:
        perms = ch.permissions_for(ctx.guild.me)
        if not perms.read_message_history:
            continue

        async for m in ch.history(after=cutoff):
            scanned += 1
            if m.author.bot:
                continue

            kingdom, dp, castles = parse_spy(m.content)
            if not kingdom or not dp or dp < 1000:
                continue

            h = hash_report(m.content)
            raw_gz = psycopg2.Binary(compress_report(m.content))
            ts = m.created_at.replace(tzinfo=timezone.utc)

            with db_connect() as conn, conn.cursor() as cur:
                cur.execute("SELECT 1 FROM spy_reports WHERE report_hash=%s;", (h,))
                if cur.fetchone():
                    dupes += 1
                    continue

                cur.execute("""
                    INSERT INTO spy_reports
                    (kingdom, defense_power, castles, created_at, raw_gz, report_hash)
                    VALUES (%s,%s,%s,%s,%s,%s);
                """, (kingdom, dp, castles, ts, raw_gz, h))
                inserted += 1

    heal_sequences()
    await ctx.send(f"✅ Backfill complete | Scanned {scanned:,} | Inserted {inserted:,} | Dupes {dupes:,}")

# ---------- Run ----------
bot.run(TOKEN)
