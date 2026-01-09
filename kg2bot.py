# ---------- KG2 Recon Bot • FULL FINAL BUILD (PostgreSQL) ----------
# Spy Capture + Embed Display • Spy History • Spy ID Lookup
# Calc (HC fixed @ 7 AP + explicit remaining % + Remaining DP shown)
# AP Planner w/ Buttons + Reset • AP Status • Session Locking • Error Logging
# Startup announces to #kg2recon-updates
# Startup self-heals ID sequences
# Tech indexing + pull from saved spy reports
# Storage upgrade: compress raw spy report into BYTEA (raw_gz)
# Backfill: scan every readable channel history and import old spy reports
# Export: !techallcsv (clean spreadsheet import)

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
# Prefer DATABASE_URL if you add it on Render; otherwise use these fallbacks.
DATABASE_URL = (
    os.getenv("DATABASE_URL")
    or os.getenv("INTERNAL_DATABASE_URL")
    or os.getenv("EXTERNAL_DATABASE_URL")
)

# Fallback to your Render Postgres details (internal host works best from Render services)
FALLBACK_DB = {
    "host": os.getenv("DB_HOST", "dpg-d54eklm3jp1c73970rdg-a"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "kg2bot_db"),
    "user": os.getenv("DB_USER", "kg2bot_db_user"),
    "password": os.getenv("DB_PASS", "tH4jvQNiIAvE8jmIVbVCMxsFnG1hvccA"),
}

# If you want to also store plaintext raw again (uses more DB space), set KEEP_RAW_TEXT=true
KEEP_RAW_TEXT = os.getenv("KEEP_RAW_TEXT", "false").lower() in ("1", "true", "yes", "y")

# ---------- Constants ----------
HEAVY_CAVALRY_AP = 7  # KG2: 1 HC = 7 AP

# Explicit one-hit remaining% math (matches your desired output)
AP_REDUCTIONS = [
    ("Minor Victory", 0.19),
    ("Victory", 0.35),
    ("Major Victory", 0.55),
    ("Overwhelming Victory", 0.875),
]

# Battle-ish keyword filter (for techindex/techpull)
BATTLE_TECH_KEYWORDS = [
    "training", "leadership", "battle", "attack", "defense", "defensive", "offense", "offensive",
    "troop", "army", "cavalry", "archer", "pikemen", "knight", "siege",
    "damage", "health", "hp", "armor", "speed", "march", "morale", "accuracy",
]

# ---------- Discord ----------
intents = discord.Intents.default()
intents.message_content = True  # MUST also be enabled in Discord Developer Portal
bot = commands.Bot(command_prefix="!", intents=intents)

# ---------- Locks ----------
ap_lock = asyncio.Lock()
tech_index_lock = asyncio.Lock()
backfill_lock = asyncio.Lock()

# ---------- Permissions ----------
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

        # Self-heal older schemas if you created tables earlier differently
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
    """Prefer compressed raw_gz; fallback to raw for older records."""
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

def is_battle_related_tech(name: str) -> bool:
    n = name.lower()
    return any(k in n for k in BATTLE_TECH_KEYWORDS)

def extract_tech_from_raw(raw: str):
    """Extract lines like 'Better Training Methods lvl 6' from tech section."""
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

def ensure_ap_session(kingdom: str) -> bool:
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM dp_sessions WHERE kingdom=%s ORDER BY captured_at DESC LIMIT 1;", (kingdom,))
        if cur.fetchone():
            return True

        cur.execute(
            "SELECT defense_power, castles, created_at FROM spy_reports WHERE kingdom=%s ORDER BY created_at DESC LIMIT 1;",
            (kingdom,)
        )
        spy = cur.fetchone()
        if not spy:
            return False

        dp = spy["defense_power"] or 0
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
        cur.execute(
            "SELECT base_dp, current_dp, hits, last_hit FROM dp_sessions WHERE kingdom=%s ORDER BY captured_at DESC LIMIT 1;",
            (kingdom,)
        )
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
        if ch:
            await ch.send(f"⚠️ ERROR LOG:\n```py\n{msg}\n```")
    except Exception:
        pass

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
    heal_sequences()
    logging.info(f"KG2 Recon Bot logged in as {bot.user}")

    for guild in bot.guilds:
        ch = discord.utils.get(guild.text_channels, name=ERROR_CHANNEL_NAME)
        if ch:
            await ch.send(f"✅ KG2 Recon Bot started • {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} • DB ok")

# ---------- Auto Capture ----------
@bot.event
async def on_message(msg: discord.Message):
    if msg.author.bot or not msg.guild:
        return

    try:
        kingdom, dp, castles = parse_spy(msg.content)

        # Safety guard: avoid false positives / junk
        if not kingdom or not dp or dp < 1000:
            await bot.process_commands(msg)
            return

        h = hash_report(msg.content)
        ts = msg.created_at.replace(tzinfo=timezone.utc) if msg.created_at else datetime.now(timezone.utc)

        raw_gz = psycopg2.Binary(compress_report(msg.content))
        raw_text = msg.content if KEEP_RAW_TEXT else None

        with db_connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT id FROM spy_reports WHERE report_hash=%s LIMIT 1;", (h,))
            if cur.fetchone():
                await bot.process_commands(msg)
                return

            cur.execute("""
                INSERT INTO spy_reports (kingdom, defense_power, castles, created_at, raw, raw_gz, report_hash)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                RETURNING id, kingdom, defense_power, castles, created_at;
            """, (kingdom, dp, castles, ts, raw_text, raw_gz, h))
            row = cur.fetchone()

        ensure_ap_session(kingdom)
        await msg.channel.send(embed=build_spy_embed(row))

    except Exception as e:
        await send_error(msg.guild, f"on_message: {e}")

    await bot.process_commands(msg)

# ---------- Spy Commands ----------
@bot.command()
async def spy(ctx, *, kingdom: str):
    real = fuzzy_kingdom(kingdom) or kingdom
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, kingdom, defense_power, castles, created_at
            FROM spy_reports
            WHERE kingdom=%s
            ORDER BY created_at DESC
            LIMIT 1;
        """, (real,))
        row = cur.fetchone()
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

# ---------- Calc ----------
@bot.command()
async def calc(ctx):
    await ctx.send("📄 Paste spy report:")
    try:
        msg = await bot.wait_for(
            "message",
            timeout=300,
            check=lambda m: m.author == ctx.author and m.channel == ctx.channel
        )
    except asyncio.TimeoutError:
        return await ctx.send("⏰ Timed out.")

    kingdom, dp, castles = parse_spy(msg.content)
    if not kingdom or not dp:
        return await ctx.send("❌ Could not parse spy report.")

    adjusted_dp = ceil(dp * (1 + castle_bonus(castles)))
    hc_no_ap = ceil(adjusted_dp / HEAVY_CAVALRY_AP)

    embed = discord.Embed(title="⚔️ Combat Calculator (KG2)", color=0x5865F2)
    embed.add_field(name="Target", value=kingdom, inline=False)
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
        return await ctx.send("❌ No spy report found.")
    await ctx.send(embed=build_ap_embed(real), view=APView(real))

@bot.command()
async def apstatus(ctx, *, kingdom: str):
    real = fuzzy_kingdom(kingdom) or kingdom
    embed = build_ap_embed(real)
    if not embed:
        return await ctx.send("❌ No active AP session.")
    await ctx.send(embed=embed)

# ---------- Tech Commands ----------
@bot.command()
async def techindex(ctx):
    async with tech_index_lock:
        await ctx.send("🔎 Scanning saved spy reports for battle-related trainings...")

        scanned = 0
        reports_with_section = 0
        kept_lines = 0
        upserts = 0

        with db_connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT id, kingdom, created_at, raw, raw_gz FROM spy_reports ORDER BY created_at DESC;")
            reports = cur.fetchall()

            for r in reports:
                scanned += 1
                kingdom = r.get("kingdom")
                if not kingdom:
                    continue

                raw_text = get_raw_text(r)
                techs = extract_tech_from_raw(raw_text)
                if not techs:
                    continue

                reports_with_section += 1

                techs = [(name, lvl) for (name, lvl) in techs if is_battle_related_tech(name)]
                if not techs:
                    continue

                kept_lines += len(techs)

                for name, lvl in techs:
                    cur.execute("""
                        INSERT INTO player_tech (kingdom, tech_name, tech_level, last_seen, source_report_id)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (kingdom, tech_name)
                        DO UPDATE SET
                            tech_level = EXCLUDED.tech_level,
                            last_seen = EXCLUDED.last_seen,
                            source_report_id = EXCLUDED.source_report_id
                        WHERE player_tech.last_seen <= EXCLUDED.last_seen;
                    """, (kingdom, name, lvl, r["created_at"] or datetime.now(timezone.utc), r["id"]))
                    upserts += 1

        await ctx.send(
            "✅ Tech index updated.\n"
            f"Reports scanned: {scanned:,}\n"
            f"Reports w/ tech section: {reports_with_section:,}\n"
            f"Battle-tech lines kept: {kept_lines:,}\n"
            f"Rows upserted: {upserts:,}"
        )

@bot.command()
async def tech(ctx, *, kingdom: str):
    real = fuzzy_kingdom(kingdom) or kingdom
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT tech_name, tech_level, last_seen
            FROM player_tech
            WHERE kingdom=%s
            ORDER BY tech_name ASC;
        """, (real,))
        rows = cur.fetchall()

    if not rows:
        return await ctx.send(f"❌ No indexed battle-related tech for **{real}**.\nRun `!techindex` first (or use `!techpull {real}`).")

    embed = discord.Embed(title=f"📚 Battle Trainings • {real}", color=0x2ECC71)
    lines = [f"• {r['tech_name']} — lvl {r['tech_level']} (last: {r['last_seen']})" for r in rows[:25]]
    embed.description = "\n".join(lines)
    if len(rows) > 25:
        embed.set_footer(text=f"Showing 25 of {len(rows)}")
    await ctx.send(embed=embed)

@bot.command()
async def techtop(ctx):
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT tech_name, COUNT(*) AS cnt
            FROM player_tech
            GROUP BY tech_name
            ORDER BY cnt DESC, tech_name ASC
            LIMIT 15;
        """)
        rows = cur.fetchall()

    if not rows:
        return await ctx.send("❌ No tech indexed yet. Run `!techindex` first.")

    msg = ["🏆 **Most common battle-related trainings (indexed)**"]
    for r in rows:
        msg.append(f"• **{r['tech_name']}** — {r['cnt']}")
    await ctx.send("\n".join(msg))

@bot.command()
async def techpull(ctx, *, kingdom: str):
    """Read-only pull: dedupe by name (highest level wins; if tie newest wins)."""
    real = fuzzy_kingdom(kingdom) or kingdom

    with db_connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, created_at, raw, raw_gz
            FROM spy_reports
            WHERE kingdom=%s
            ORDER BY created_at DESC;
        """, (real,))
        reports = cur.fetchall()

    if not reports:
        return await ctx.send(f"❌ No saved spy reports found for **{real}**.")

    tech_map = {}
    scanned = 0
    seen_lines = 0

    for r in reports:
        scanned += 1
        raw_text = get_raw_text(r)
        techs = extract_tech_from_raw(raw_text)
        if not techs:
            continue

        techs = [(name, lvl) for (name, lvl) in techs if is_battle_related_tech(name)]
        if not techs:
            continue

        for name, lvl in techs:
            seen_lines += 1
            seen_ts = r["created_at"] or datetime.now(timezone.utc)
            prev = tech_map.get(name)

            if (prev is None) or (lvl > prev["lvl"]) or (lvl == prev["lvl"] and seen_ts > prev["seen"]):
                tech_map[name] = {"lvl": lvl, "seen": seen_ts, "rid": r["id"]}

    if not tech_map:
        return await ctx.send(f"❌ No battle-related tech found in saved reports for **{real}**.")

    items = sorted(tech_map.items(), key=lambda x: x[0].lower())

    embed = discord.Embed(title=f"📚 Tech Pull • {real}", color=0x2ECC71)
    embed.add_field(name="Reports scanned", value=str(scanned), inline=True)
    embed.add_field(name="Tech lines seen", value=str(seen_lines), inline=True)
    embed.add_field(name="Unique tech kept", value=str(len(items)), inline=True)

    lines = [f"• **{name}** — lvl **{data['lvl']}**" for name, data in items[:25]]
    embed.description = "\n".join(lines)

    if len(items) > 25:
        embed.set_footer(text=f"Showing 25 of {len(items)}")
    await ctx.send(embed=embed)

# ---------- Export: techallcsv ----------
@bot.command()
async def techallcsv(ctx):
    """
    Uploads a CSV of ALL indexed battle-related tech for ALL kingdoms.
    Cleanest spreadsheet import.
    """
    import io, csv

    with db_connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT kingdom, tech_name, tech_level, last_seen
            FROM player_tech
            ORDER BY kingdom ASC, tech_name ASC;
        """)
        rows = cur.fetchall()

    if not rows:
        return await ctx.send("❌ No tech indexed yet. Run `!techindex` first.")

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["Kingdom", "Tech", "Level", "Last Seen"])
    for r in rows:
        d = r["last_seen"].strftime("%Y-%m-%d %H:%M:%S") if r.get("last_seen") else ""
        w.writerow([r["kingdom"], r["tech_name"], r["tech_level"], d])

    data = buf.getvalue().encode("utf-8")
    file = discord.File(fp=io.BytesIO(data), filename="kg2_battle_tech_all.csv")
    await ctx.send("✅ Here’s the CSV export:", file=file)

# ---------- Backfill ----------
@bot.command()
@commands.check(is_admin_or_owner)
async def backfill(ctx, days: int = 30):
    async with backfill_lock:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        status_ch = discord.utils.get(ctx.guild.text_channels, name=ERROR_CHANNEL_NAME)

        async def post_status(text: str):
            try:
                if status_ch:
                    await status_ch.send(text)
                else:
                    await ctx.send(text)
            except Exception:
                pass

        await post_status(f"⏳ Backfill starting… last **{days}** days (since {cutoff.isoformat()}).")

        channels_scanned = 0
        msgs_scanned = 0
        spy_found = 0
        inserted = 0
        dupes = 0
        errors = 0

        me = ctx.guild.get_member(bot.user.id)

        for ch in ctx.guild.text_channels:
            perms = ch.permissions_for(me) if me else ch.permissions_for(ctx.guild.me)
            if not perms.view_channel or not perms.read_message_history:
                continue

            channels_scanned += 1
            await asyncio.sleep(0.25)

            try:
                async for m in ch.history(limit=None, after=cutoff, oldest_first=True):
                    msgs_scanned += 1
                    if m.author.bot:
                        continue

                    kingdom, dp, castles = parse_spy(m.content)
                    if not kingdom or not dp or dp < 1000:
                        continue

                    spy_found += 1
                    h = hash_report(m.content)
                    ts = m.created_at.replace(tzinfo=timezone.utc) if m.created_at else datetime.now(timezone.utc)

                    raw_gz = psycopg2.Binary(compress_report(m.content))
                    raw_text = m.content if KEEP_RAW_TEXT else None

                    try:
                        with db_connect() as conn, conn.cursor() as cur:
                            cur.execute("SELECT 1 FROM spy_reports WHERE report_hash=%s LIMIT 1;", (h,))
                            if cur.fetchone():
                                dupes += 1
                                continue

                            cur.execute("""
                                INSERT INTO spy_reports (kingdom, defense_power, castles, created_at, raw, raw_gz, report_hash)
                                VALUES (%s,%s,%s,%s,%s,%s,%s);
                            """, (kingdom, dp, castles, ts, raw_text, raw_gz, h))
                            inserted += 1
                    except Exception as e:
                        errors += 1
                        await send_error(ctx.guild, f"backfill insert: {e}")

                    if msgs_scanned % 200 == 0:
                        await asyncio.sleep(1.0)

            except Exception as e:
                errors += 1
                await send_error(ctx.guild, f"backfill channel {ch.name}: {e}")

        try:
            heal_sequences()
        except Exception:
            pass

        await post_status(
            "✅ Backfill complete.\n"
            f"Channels scanned: **{channels_scanned}**\n"
            f"Messages scanned: **{msgs_scanned:,}**\n"
            f"Spy reports detected: **{spy_found:,}**\n"
            f"Inserted: **{inserted:,}**\n"
            f"Duplicates skipped: **{dupes:,}**\n"
            f"Errors: **{errors:,}**"
        )

# ---------- Run ----------
bot.run(TOKEN)
