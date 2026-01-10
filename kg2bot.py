# ---------- KG2 Recon Bot • FULL FINAL BUILD (PostgreSQL) ----------
# Spy Capture + Embed Display • Spy History • Spy ID Lookup
# Calc: DB-linked (uses last saved spy report) + explicit remaining % + Remaining DP shown
# AP Planner w/ Buttons + Reset • AP Status • Session Locking
# Tech system (ALL research): auto-index on every captured spy report
# Tech commands + CSV export
# Backfill: scans server history and stores spy reports + tech
# Diagnostics: !ping, !perms
# Startup announces to #kg2recon-updates + patch notes + self-heals sequences

import os, re, asyncio, difflib, hashlib, logging, gzip
from math import ceil
from datetime import datetime, timezone, timedelta

import discord
from discord.ext import commands
from discord.ui import View, Button
from dotenv import load_dotenv

import psycopg2
from psycopg2.extras import RealDictCursor

# ------------------- PATCH INFO (edit this each deploy) -------------------
BOT_VERSION = "2026-01-09.2"
PATCH_NOTES = [
    "Fixed: !calc now uses the latest saved spy report (or latest per-kingdom).",
    "Fixed: AP sessions will not initialize with 0 DP (prevents 0/0/0 planner bug).",
    "Added: Startup announcement includes patch notes + version.",
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

# Fallback (Render internal hostname is best for services)
FALLBACK_DB = {
    "host": os.getenv("DB_HOST", "dpg-d54eklm3jp1c73970rdg-a"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "kg2bot_db"),
    "user": os.getenv("DB_USER", "kg2bot_db_user"),
    "password": os.getenv("DB_PASS", "tH4jvQNiIAvE8jmIVbVCMxsFnG1hvccA"),
}

# Keep plaintext raw in DB? (uses more storage)
KEEP_RAW_TEXT = os.getenv("KEEP_RAW_TEXT", "false").lower() in ("1", "true", "yes", "y")

# ---------- Constants ----------
HEAVY_CAVALRY_AP = 7  # KG2: 1 HC = 7 AP

AP_REDUCTIONS = [
    ("Minor Victory", 0.19),
    ("Victory", 0.35),
    ("Major Victory", 0.55),
    ("Overwhelming Victory", 0.875),  # label shows -87% in embed (rounded)
]

# ---------- Discord ----------
intents = discord.Intents.default()
intents.message_content = True  # MUST also be enabled in Discord Dev Portal

bot = commands.Bot(command_prefix="!", intents=intents)

# ---------- Locks ----------
ap_lock = asyncio.Lock()
tech_lock = asyncio.Lock()
backfill_lock = asyncio.Lock()

# ---------- Permissions helpers ----------
def is_admin_or_owner(ctx: commands.Context) -> bool:
    try:
        return ctx.author.guild_permissions.administrator or ctx.author.id == ctx.guild.owner_id
    except Exception:
        return False

# ---------- DB ----------
def db_connect():
    """Connect using DATABASE_URL if available, else fallback. Render Postgres requires SSL."""
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
        cur.execute("CREATE INDEX IF NOT EXISTS idx_player_tech_kingdom ON player_tech(kingdom);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_player_tech_last_seen ON player_tech(last_seen DESC);")

        # Self-heal older schemas
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

def decompress_report(blob: bytes) -> str:
    return gzip.decompress(blob).decode("utf-8", errors="replace")

def get_raw_text(row) -> str:
    if not row:
        return ""
    if row.get("raw_gz"):
        try:
            return decompress_report(row["raw_gz"])
        except Exception:
            pass
    return row.get("raw") or ""

def fuzzy_kingdom(query: str):
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT DISTINCT kingdom FROM spy_reports WHERE kingdom IS NOT NULL;")
        names = [r["kingdom"] for r in cur.fetchall() if r.get("kingdom")]
    match = difflib.get_close_matches(query, names, 1, 0.5)
    return match[0] if match else None

def extract_tech_from_raw(raw: str):
    if not raw:
        return []
    lines = raw.splitlines()
    start_idx = None
    for i, line in enumerate(lines):
        if line.strip().lower().startswith("the following technology information was also discovered"):
            start_idx = i + 1
            break
    if start_idx is None:
        return []

    techs = []
    for j in range(start_idx, len(lines)):
        line = lines[j].strip()
        if not line:
            break

        low = line.lower()
        if low.startswith("our spies also found") or low.startswith("the following ") or low.startswith("sender:") or low.startswith("recipient"):
            break

        m = re.match(r"^(.*?)\s+lvl\s+(\d+)\s*$", line, flags=re.IGNORECASE)
        if m:
            techs.append((m.group(1).strip(), int(m.group(2))))
    return techs

def upsert_tech_list(kingdom: str, techs, seen_ts, source_report_id: int):
    if not kingdom or not techs:
        return 0
    upserts = 0
    with db_connect() as conn, conn.cursor() as cur:
        for tech_name, lvl in techs:
            cur.execute("""
                INSERT INTO player_tech (kingdom, tech_name, tech_level, last_seen, source_report_id)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (kingdom, tech_name)
                DO UPDATE SET
                    tech_level = CASE
                        WHEN EXCLUDED.tech_level > player_tech.tech_level THEN EXCLUDED.tech_level
                        WHEN EXCLUDED.tech_level = player_tech.tech_level AND EXCLUDED.last_seen > player_tech.last_seen THEN EXCLUDED.tech_level
                        ELSE player_tech.tech_level
                    END,
                    last_seen = CASE
                        WHEN EXCLUDED.tech_level > player_tech.tech_level THEN EXCLUDED.last_seen
                        WHEN EXCLUDED.tech_level = player_tech.tech_level AND EXCLUDED.last_seen > player_tech.last_seen THEN EXCLUDED.last_seen
                        ELSE player_tech.last_seen
                    END,
                    source_report_id = CASE
                        WHEN EXCLUDED.tech_level > player_tech.tech_level THEN EXCLUDED.source_report_id
                        WHEN EXCLUDED.tech_level = player_tech.tech_level AND EXCLUDED.last_seen > player_tech.last_seen THEN EXCLUDED.source_report_id
                        ELSE player_tech.source_report_id
                    END;
            """, (kingdom, tech_name, lvl, seen_ts, source_report_id))
            upserts += 1
    return upserts

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

def ensure_ap_session(kingdom: str) -> bool:
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM dp_sessions WHERE kingdom=%s ORDER BY captured_at DESC LIMIT 1;", (kingdom,))
        if cur.fetchone():
            return True

        # Latest valid report only (prevents 0/0/0 sessions)
        cur.execute("""
            SELECT defense_power, castles, created_at
            FROM spy_reports
            WHERE kingdom=%s
              AND defense_power IS NOT NULL
              AND defense_power > 0
            ORDER BY created_at DESC
            LIMIT 1;
        """, (kingdom,))
        spy = cur.fetchone()
        if not spy:
            return False

        dp = spy["defense_power"]
        castles = spy["castles"] or 0
        ts = spy["created_at"] or datetime.now(timezone.utc)

        cur.execute("""
            INSERT INTO dp_sessions (kingdom, base_dp, castles, current_dp, hits, last_hit, captured_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s);
        """, (kingdom, dp, castles, dp, 0, None, ts))
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

# ---------- Diagnostics ----------
@bot.command()
async def ping(ctx):
    await ctx.send("🏓 Pong. Commands are working here.")

@bot.command()
async def perms(ctx):
    me = ctx.guild.me
    p = ctx.channel.permissions_for(me)
    await ctx.send(
        "🔎 **Bot Permissions Here**\n"
        f"Send Messages: `{p.send_messages}`\n"
        f"Embed Links: `{p.embed_links}`\n"
        f"Read Message History: `{p.read_message_history}`\n"
        f"View Channel: `{p.view_channel}`\n"
        f"Message Content Intent (code): `{bot.intents.message_content}`\n"
        "If Send/Embed is False, the bot can’t respond.\n"
        "If Message Content intent is off in the Dev Portal, commands won’t parse."
    )

# ---------- Command error logging ----------
@bot.event
async def on_command_error(ctx, error):
    try:
        await ctx.send(f"❌ Command error: {type(error).__name__} — {error}")
    except Exception:
        pass
    try:
        if ctx.guild:
            await send_error(ctx.guild, f"{ctx.command} by {ctx.author}: {type(error).__name__} — {error}")
    except Exception:
        pass

# ---------- Startup ----------
@bot.event
async def on_ready():
    init_db()
    try:
        heal_sequences()
    except Exception as e:
        logging.error(f"heal_sequences failed: {e}")

    logging.info(f"KG2 Recon Bot logged in as {bot.user}")

    patch_lines = "\n".join([f"• {x}" for x in PATCH_NOTES])
    for guild in bot.guilds:
        ch = discord.utils.get(guild.text_channels, name=ERROR_CHANNEL_NAME)
        if ch:
            try:
                await ch.send(
                    "✅ **KG2 Recon Bot restarted**\n"
                    f"Version: `{BOT_VERSION}`\n"
                    f"Patch:\n{patch_lines}"
                )
            except Exception as e:
                logging.error(f"Startup announce failed in {guild.name}: {e}")

# ---------- Auto Capture (Spy + Tech auto-index) ----------
@bot.event
async def on_message(msg: discord.Message):
    try:
        if msg.author.bot or not msg.guild:
            return

        kingdom, dp, castles = parse_spy(msg.content)

        # Safety guard
        if not kingdom or not dp or dp < 1000:
            return

        h = hash_report(msg.content)
        ts = msg.created_at.replace(tzinfo=timezone.utc) if msg.created_at else datetime.now(timezone.utc)

        raw_gz = psycopg2.Binary(compress_report(msg.content))
        raw_text = msg.content if KEEP_RAW_TEXT else None

        inserted_row = None

        with db_connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT id FROM spy_reports WHERE report_hash=%s LIMIT 1;", (h,))
            if cur.fetchone():
                # optional: let you know it was already saved
                if msg.channel.permissions_for(msg.guild.me).send_messages:
                    await msg.channel.send("✅ Duplicate spy report detected (already saved).")
                return

            cur.execute("""
                INSERT INTO spy_reports (kingdom, defense_power, castles, created_at, raw, raw_gz, report_hash)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                RETURNING id, kingdom, defense_power, castles, created_at;
            """, (kingdom, dp, castles, ts, raw_text, raw_gz, h))
            inserted_row = cur.fetchone()

        # Create AP session baseline if none exists
        ensure_ap_session(kingdom)

        # Auto tech index (ALL research)
        techs = extract_tech_from_raw(msg.content)
        if techs:
            async with tech_lock:
                upsert_tech_list(kingdom, techs, ts, inserted_row["id"])

        # Send embed
        try:
            if msg.channel.permissions_for(msg.guild.me).send_messages:
                await msg.channel.send(embed=build_spy_embed(inserted_row))
        except Exception as e:
            await send_error(msg.guild, f"send embed failed: {e}")

    except Exception as e:
        if msg.guild:
            await send_error(msg.guild, f"on_message: {e}")
    finally:
        await bot.process_commands(msg)

# ---------- Spy Commands ----------
@bot.command()
async def spy(ctx, *, kingdom: str):
    real = fuzzy_kingdom(kingdom) or kingdom
    row = get_latest_spy_report_for_kingdom(real)
    if not row:
        return await ctx.send("❌ No spy report found.")
    await ctx.send(embed=build_spy_embed(row))

@bot.command()
async def spyid(ctx, sid: int):
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, kingdom, defense_power, castles, created_at
            FROM spy_reports
            WHERE id=%s;
        """, (sid,))
        row = cur.fetchone()
    if not row:
        return await ctx.send("❌ Invalid spy report ID.")
    await ctx.send(embed=build_spy_embed(row))

@bot.command()
async def spyhistory(ctx, *, kingdom: str):
    real = fuzzy_kingdom(kingdom) or kingdom
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, defense_power, created_at
            FROM spy_reports
            WHERE kingdom=%s
              AND defense_power IS NOT NULL
              AND defense_power > 0
            ORDER BY created_at DESC
            LIMIT 5;
        """, (real,))
        rows = cur.fetchall()

    if not rows:
        return await ctx.send("❌ No spy history found.")

    out = [f"🗂 **Spy History — {real}**"]
    for r in rows:
        out.append(f"ID `{r['id']}` • DP `{r['defense_power']:,}` • {r['created_at']}")
    await ctx.send("\n".join(out))

# ---------- Calc (DB-linked) ----------
@bot.command()
async def calc(ctx, *, kingdom: str = None):
    """
    Usage:
      !calc             -> uses most recent saved spy report (any kingdom)
      !calc <kingdom>   -> uses most recent saved spy report for that kingdom
      If DB has none, falls back to paste mode.
    """
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
        # fallback: paste mode
        await ctx.send("📄 No usable report found in DB. Paste a spy report:")
        try:
            msg = await bot.wait_for(
                "message",
                timeout=300,
                check=lambda m: m.author == ctx.author and m.channel == ctx.channel
            )
        except asyncio.TimeoutError:
            return await ctx.send("⏰ Timed out.")

        kingdom2, dp2, castles2 = parse_spy(msg.content)
        if not kingdom2 or not dp2:
            return await ctx.send("❌ Could not parse spy report.")
        dp = dp2
        castles = castles2
        target = kingdom2
        used = "(using pasted report)"
    else:
        dp = row["defense_power"] or 0
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

# ---------- Run ----------
bot.run(TOKEN)
