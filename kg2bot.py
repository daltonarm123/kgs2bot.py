# ---------- KG2 Recon Bot • FULL FINAL BUILD (PostgreSQL) ----------
# Spy Capture + Embed Display • Spy History • Spy ID Lookup
# Calc: DB-linked (uses last saved spy report) + explicit remaining % + Remaining DP shown
# AP Planner w/ Buttons + Reset • AP Status • Session Locking
# Startup announces to #kg2recon-updates + patch notes + self-heals sequences

import os, re, asyncio, difflib, hashlib, logging, gzip
from math import ceil
from datetime import datetime, timezone

import discord
from discord.ext import commands
from discord.ui import View, Button
from dotenv import load_dotenv

import psycopg2
from psycopg2.extras import RealDictCursor

# ------------------- PATCH INFO (edit this each deploy) -------------------
BOT_VERSION = "2026-01-09.3"
PATCH_NOTES = [
    "Fixed: added !apfix command this should be ran if shows 0 during ap plan.",
    
]
# ------------------------------------------------------------------------

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

# ---------- Discord ----------
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# ---------- Locks ----------
ap_lock = asyncio.Lock()

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
        # schema self-heal
        cur.execute("ALTER TABLE spy_reports ADD COLUMN IF NOT EXISTS defense_power INTEGER;")
        cur.execute("ALTER TABLE spy_reports ADD COLUMN IF NOT EXISTS castles INTEGER;")
        cur.execute("ALTER TABLE spy_reports ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ;")
        cur.execute("ALTER TABLE spy_reports ADD COLUMN IF NOT EXISTS raw TEXT;")
        cur.execute("ALTER TABLE spy_reports ADD COLUMN IF NOT EXISTS raw_gz BYTEA;")
        cur.execute("ALTER TABLE spy_reports ADD COLUMN IF NOT EXISTS report_hash TEXT;")
        try:
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS spy_reports_report_hash_uq ON spy_reports(report_hash);")
        except Exception:
            pass

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
def castle_bonus(c: int) -> float:
    return (c ** 0.5) / 100 if c else 0.0

def hash_report(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

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

def compress_report(text: str) -> bytes:
    return gzip.compress(text.encode("utf-8"), compresslevel=9)

def fuzzy_kingdom(query: str):
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT DISTINCT kingdom FROM spy_reports WHERE kingdom IS NOT NULL;")
        names = [r["kingdom"] for r in cur.fetchall() if r.get("kingdom")]
    match = difflib.get_close_matches(query, names, 1, 0.5)
    return match[0] if match else None

def get_latest_spy_report_for_kingdom(kingdom: str):
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, kingdom, defense_power, castles, created_at
            FROM spy_reports
            WHERE kingdom=%s
              AND defense_power IS NOT NULL
              AND defense_power > 0
            ORDER BY created_at DESC
            LIMIT 1;
        """, (kingdom,))
        return cur.fetchone()

def get_latest_spy_report_any():
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, kingdom, defense_power, castles, created_at
            FROM spy_reports
            WHERE defense_power IS NOT NULL
              AND defense_power > 0
            ORDER BY created_at DESC
            LIMIT 1;
        """)
        return cur.fetchone()

def rebuild_ap_session(kingdom: str) -> bool:
    """Deletes latest session for kingdom and rebuilds from latest valid spy report."""
    spy = get_latest_spy_report_for_kingdom(kingdom)
    if not spy:
        return False

    with db_connect() as conn, conn.cursor() as cur:
        # delete only the most recent session row (so history isn't huge, but keeps it clean)
        cur.execute("""
            DELETE FROM dp_sessions
            WHERE id IN (
                SELECT id FROM dp_sessions
                WHERE kingdom=%s
                ORDER BY captured_at DESC
                LIMIT 1
            );
        """, (kingdom,))

        cur.execute("""
            INSERT INTO dp_sessions (kingdom, base_dp, castles, current_dp, hits, last_hit, captured_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s);
        """, (
            kingdom,
            spy["defense_power"],
            spy["castles"] or 0,
            spy["defense_power"],
            0,
            None,
            spy["created_at"] or datetime.now(timezone.utc),
        ))
    return True

def ensure_ap_session(kingdom: str) -> bool:
    """
    If session exists but has 0 DP, auto-rebuild it from latest valid spy report.
    Otherwise create session if missing.
    """
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, base_dp, current_dp
            FROM dp_sessions
            WHERE kingdom=%s
            ORDER BY captured_at DESC
            LIMIT 1;
        """, (kingdom,))
        sess = cur.fetchone()

    if sess:
        # ✅ auto-heal broken sessions
        if (sess["base_dp"] or 0) <= 0 and (sess["current_dp"] or 0) <= 0:
            return rebuild_ap_session(kingdom)
        return True

    # create if missing
    spy = get_latest_spy_report_for_kingdom(kingdom)
    if not spy:
        return False

    with db_connect() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dp_sessions (kingdom, base_dp, castles, current_dp, hits, last_hit, captured_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s);
        """, (
            kingdom,
            spy["defense_power"],
            spy["castles"] or 0,
            spy["defense_power"],
            0,
            None,
            spy["created_at"] or datetime.now(timezone.utc),
        ))
    return True

def build_spy_embed(row):
    dp = row["defense_power"] or 0
    castles = row["castles"] or 0
    adjusted = ceil(dp * (1 + castle_bonus(castles)))

    embed = discord.Embed(title="🕵️ Spy Report", color=0x5865F2)
    embed.add_field(name="Kingdom", value=row["kingdom"] or "Unknown", inline=False)
    embed.add_field(name="Base DP", value=f"{dp:,}", inline=True)
    embed.add_field(name="Adjusted DP", value=f"{adjusted:,}", inline=True)
    embed.add_field(name="Castles", value=str(castles), inline=True)
    embed.set_footer(text=f"ID {row['id']} • Captured {row['created_at']}")
    return embed

def build_ap_embed(kingdom: str):
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT base_dp, current_dp, hits, last_hit
            FROM dp_sessions
            WHERE kingdom=%s
            ORDER BY captured_at DESC
            LIMIT 1;
        """, (kingdom,))
        row = cur.fetchone()
    if not row:
        return None

    embed = discord.Embed(title=f"⚔️ AP Planner • {kingdom}", color=0xE74C3C)
    embed.add_field(name="Base DP", value=f"{row['base_dp']:,}")
    embed.add_field(name="Current DP", value=f"{row['current_dp']:,}")
    embed.add_field(name="Hits Applied", value=str(row["hits"]))
    if row.get("last_hit"):
        embed.set_footer(text=f"Last hit by {row['last_hit']}")
    return embed

async def send_error(guild: discord.Guild, msg: str):
    try:
        ch = discord.utils.get(guild.text_channels, name=ERROR_CHANNEL_NAME)
        if ch and ch.permissions_for(guild.me).send_messages:
            await ch.send(f"⚠️ ERROR LOG:\n```py\n{msg}\n```")
            return
    except Exception:
        pass
    logging.error(msg)

# ---------- Startup ----------
@bot.event
async def on_ready():
    init_db()
    try:
        heal_sequences()
    except Exception as e:
        logging.error(f"heal_sequences failed: {e}")

    patch_lines = "\n".join([f"• {x}" for x in PATCH_NOTES])
    for guild in bot.guilds:
        ch = discord.utils.get(guild.text_channels, name=ERROR_CHANNEL_NAME)
        if ch:
            await ch.send(
                "✅ **KG2 Recon Bot restarted**\n"
                f"Version: `{BOT_VERSION}`\n"
                f"Patch:\n{patch_lines}"
            )

# ---------- Auto Capture ----------
@bot.event
async def on_message(msg: discord.Message):
    try:
        if msg.author.bot or not msg.guild:
            return

        kingdom, dp, castles = parse_spy(msg.content)
        if not kingdom or not dp or dp < 1000:
            return

        h = hash_report(msg.content)
        ts = msg.created_at.replace(tzinfo=timezone.utc) if msg.created_at else datetime.now(timezone.utc)

        raw_gz = psycopg2.Binary(compress_report(msg.content))
        raw_text = msg.content if KEEP_RAW_TEXT else None

        with db_connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT id FROM spy_reports WHERE report_hash=%s LIMIT 1;", (h,))
            if cur.fetchone():
                if msg.channel.permissions_for(msg.guild.me).send_messages:
                    await msg.channel.send("✅ Duplicate spy report detected (already saved).")
                return

            cur.execute("""
                INSERT INTO spy_reports (kingdom, defense_power, castles, created_at, raw, raw_gz, report_hash)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                RETURNING id, kingdom, defense_power, castles, created_at;
            """, (kingdom, dp, castles, ts, raw_text, raw_gz, h))
            inserted_row = cur.fetchone()

        # ensure AP session exists (or heal 0 session)
        ensure_ap_session(kingdom)

        # send embed
        if msg.channel.permissions_for(msg.guild.me).send_messages:
            await msg.channel.send(embed=build_spy_embed(inserted_row))

    except Exception as e:
        await send_error(msg.guild, f"on_message: {e}")
    finally:
        await bot.process_commands(msg)

# ---------- Calc ----------
@bot.command()
async def calc(ctx, *, kingdom: str = None):
    row = None
    used = ""

    if kingdom:
        real = fuzzy_kingdom(kingdom) or kingdom
        row = get_latest_spy_report_for_kingdom(real)
        used = f"(using latest saved report for **{real}**)"
    else:
        row = get_latest_spy_report_any()
        used = "(using latest saved report in DB)"

    if not row:
        await ctx.send("📄 No usable report found in DB. Paste a spy report:")
        try:
            msg = await bot.wait_for(
                "message",
                timeout=300,
                check=lambda m: m.author == ctx.author and m.channel == ctx.channel
            )
        except asyncio.TimeoutError:
            return await ctx.send("⏰ Timed out.")

        k2, dp2, c2 = parse_spy(msg.content)
        if not k2 or not dp2:
            return await ctx.send("❌ Could not parse spy report.")
        dp = dp2
        castles = c2
        target = k2
        used = "(using pasted report)"
    else:
        dp = row["defense_power"]
        castles = row["castles"] or 0
        target = row["kingdom"] or "Unknown"

    adjusted_dp = ceil(dp * (1 + castle_bonus(castles)))
    hc_no_ap = ceil(adjusted_dp / HEAVY_CAVALRY_AP)

    embed = discord.Embed(title="⚔️ Combat Calculator (KG2)", color=0x5865F2)
    embed.add_field(name="Target", value=f"{target} {used}", inline=False)
    embed.add_field(name="Base DP", value=f"{dp:,}", inline=True)
    embed.add_field(name="Adjusted DP", value=f"{adjusted_dp:,}", inline=True)
    embed.add_field(name="HC Needed (No AP)", value=f"{hc_no_ap:,} HC", inline=False)

    for label, reduction in AP_REDUCTIONS:
        remaining_dp = ceil(adjusted_dp * (1 - reduction))
        hc_req = ceil(remaining_dp / HEAVY_CAVALRY_AP)
        embed.add_field(
            name=f"After {label} (−{int(reduction*100)}%)",
            value=f"Remaining DP: {remaining_dp:,}\nHC Required: {hc_req:,}",
            inline=False
        )

    embed.set_footer(text=f"HC = {HEAVY_CAVALRY_AP} AP | Explicit KG2 remaining % math")
    await ctx.send(embed=embed)

# ---------- AP Planner ----------
class APView(View):
    def __init__(self, kingdom: str):
        super().__init__(timeout=None)
        self.kingdom = kingdom
        self.add_item(APButton("minor", kingdom))
        self.add_item(APButton("victory", kingdom))
        self.add_item(APButton("major", kingdom))
        self.add_item(APButton("overwhelming", kingdom))
        self.add_item(APResetButton(kingdom))

class APButton(Button):
    def __init__(self, key: str, kingdom: str):
        super().__init__(label=key.title(), style=discord.ButtonStyle.danger)
        self.key = key
        self.kingdom = kingdom

    async def callback(self, interaction: discord.Interaction):
        async with ap_lock:
            reduction = dict(minor=0.19, victory=0.35, major=0.55, overwhelming=0.875)[self.key]
            with db_connect() as conn, conn.cursor() as cur:
                cur.execute("""
                    SELECT id, current_dp
                    FROM dp_sessions
                    WHERE kingdom=%s
                    ORDER BY captured_at DESC
                    LIMIT 1;
                """, (self.kingdom,))
                row = cur.fetchone()
                if not row:
                    return await interaction.response.send_message("❌ No active AP session.", ephemeral=True)

                dp = row["current_dp"] or 0
                reduce_amt = ceil(dp * reduction)
                new_dp = max(0, dp - reduce_amt)

                cur.execute("""
                    UPDATE dp_sessions
                    SET current_dp=%s, hits=hits+1, last_hit=%s
                    WHERE id=%s;
                """, (new_dp, interaction.user.display_name, row["id"]))

        await interaction.response.edit_message(embed=build_ap_embed(self.kingdom), view=self.view)

class APResetButton(Button):
    def __init__(self, kingdom: str):
        super().__init__(label="Reset", style=discord.ButtonStyle.secondary)
        self.kingdom = kingdom

    async def callback(self, interaction: discord.Interaction):
        async with ap_lock:
            with db_connect() as conn, conn.cursor() as cur:
                cur.execute("""
                    SELECT id, base_dp
                    FROM dp_sessions
                    WHERE kingdom=%s
                    ORDER BY captured_at DESC
                    LIMIT 1;
                """, (self.kingdom,))
                row = cur.fetchone()
                if not row:
                    return await interaction.response.send_message("❌ No active AP session.", ephemeral=True)

                cur.execute("""
                    UPDATE dp_sessions
                    SET current_dp=%s, hits=0, last_hit=NULL
                    WHERE id=%s;
                """, (row["base_dp"], row["id"]))

        await interaction.response.edit_message(embed=build_ap_embed(self.kingdom), view=self.view)

@bot.command()
async def ap(ctx, *, kingdom: str):
    real = fuzzy_kingdom(kingdom) or kingdom
    if not ensure_ap_session(real):
        return await ctx.send("❌ No usable spy report found (DP missing/0). Send a fresh spy report first.")
    await ctx.send(embed=build_ap_embed(real), view=APView(real))

@bot.command()
async def apstatus(ctx, *, kingdom: str):
    real = fuzzy_kingdom(kingdom) or kingdom
    embed = build_ap_embed(real)
    if not embed:
        return await ctx.send("❌ No active AP session.")
    await ctx.send(embed=embed)

@bot.command()
async def apfix(ctx, *, kingdom: str):
    """Force rebuild AP session from latest valid spy report."""
    real = fuzzy_kingdom(kingdom) or kingdom
    ok = rebuild_ap_session(real)
    if not ok:
        return await ctx.send("❌ Could not rebuild AP session (no valid spy report found).")
    await ctx.send(f"✅ Rebuilt AP session for **{real}** from latest saved spy report.")
    await ctx.send(embed=build_ap_embed(real), view=APView(real))

# ---------- Run ----------
bot.run(TOKEN)
