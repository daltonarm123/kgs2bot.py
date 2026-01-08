# ---------- KG2 Recon Bot • FULL FINAL BUILD (PostgreSQL) ----------
# Spy Capture + Embed Display • Spy History • Spy ID Lookup
# Calc (HC fixed @ 7 AP + explicit remaining % + Remaining DP shown)
# AP Planner w/ Buttons + Reset • AP Status • Session Locking • Error Logging
# Startup announces to #kg2recon-updates
# Startup self-heals ID sequences
# NEW: Tech indexing from saved spy report "technology information" section
#      !techindex / !tech <kingdom> / !techtop

import os, re, asyncio, difflib, hashlib, logging
from math import ceil
from datetime import datetime, timezone

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

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

logging.basicConfig(level=logging.INFO)

if not TOKEN:
    raise RuntimeError("Missing DISCORD_TOKEN env var.")
if not all([DB_HOST, DB_NAME, DB_USER, DB_PASS]):
    raise RuntimeError("Missing one or more DB env vars: DB_HOST, DB_NAME, DB_USER, DB_PASS")

# ---------- Constants ----------
HEAVY_CAVALRY_AP = 7  # KG2: 1 HC = 7 AP

# One-hit estimates (not chained), as you requested
AP_REDUCTIONS = [
    ("Minor Victory", 0.19),
    ("Victory", 0.35),
    ("Major Victory", 0.55),
    ("Overwhelming Victory", 0.875),
]

# Battle-related tech filter: tweak anytime
BATTLE_TECH_KEYWORDS = [
    "training", "leadership", "battle", "attack", "defense", "defensive", "offense", "offensive",
    "troop", "army", "cavalry", "archer", "pikemen", "knight", "siege",
    "damage", "health", "hp", "armor", "speed", "march", "morale", "accuracy",
]

# ---------- Discord ----------
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# ---------- Global Locks ----------
ap_lock = asyncio.Lock()
tech_index_lock = asyncio.Lock()

# ---------- DB ----------
def db_connect():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
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
        # Indexed tech table (normalized)
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
    return hashlib.sha256(text.encode()).hexdigest()

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
    """
    Finds section:
      The following technology information was also discovered:
    and parses lines like:
      Better Training Methods lvl 6
    Returns list of (tech_name, tech_level)
    """
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
        # Stop if we hit another “section”
        low = line.lower()
        if low.startswith("our spies also found") or low.startswith("the following ") or low.startswith("sender:") or low.startswith("recipient"):
            break

        m = re.match(r"^(.*?)\s+lvl\s+(\d+)\s*$", line, flags=re.IGNORECASE)
        if m:
            name = m.group(1).strip()
            lvl = int(m.group(2))
            techs.append((name, lvl))
        else:
            # if a weird line shows up, ignore it
            continue

    return techs

def ensure_ap_session(kingdom: str) -> bool:
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM dp_sessions WHERE kingdom=%s ORDER BY captured_at DESC LIMIT 1;",
            (kingdom,)
        )
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

        cur.execute(
            """
            INSERT INTO dp_sessions (kingdom, base_dp, castles, current_dp, hits, last_hit, captured_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s);
            """,
            (kingdom, dp, castles, dp, 0, None, ts)
        )
        return True

def build_spy_embed(row):
    adjusted = ceil((row["defense_power"] or 0) * (1 + castle_bonus(row["castles"] or 0)))
    embed = discord.Embed(title="🕵️ Spy Report", color=0x5865F2)
    embed.add_field(name="Kingdom", value=row["kingdom"] or "Unknown", inline=False)
    embed.add_field(name="Base DP", value=f"{(row['defense_power'] or 0):,}", inline=True)
    embed.add_field(name="Adjusted DP", value=f"{adjusted:,}", inline=True)
    embed.add_field(name="Castles", value=str(row["castles"] or 0), inline=True)
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

async def send_error(guild, msg: str):
    try:
        ch = discord.utils.get(guild.text_channels, name=ERROR_CHANNEL_NAME)
        if ch:
            await ch.send(f"⚠️ ERROR LOG:\n```py\n{msg}\n```")
    except:
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
            await ch.send(
                f"✅ KG2 Recon Bot started • {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} • DB sequences healed"
            )

# ---------- Auto Capture ----------
@bot.event
async def on_message(msg):
    if msg.author.bot or not msg.guild:
        return

    try:
        kingdom, dp, castles = parse_spy(msg.content)
        if not kingdom or not dp or dp < 1000:
            await bot.process_commands(msg)
            return

        h = hash_report(msg.content)
        ts = datetime.now(timezone.utc)

        with db_connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT id FROM spy_reports WHERE report_hash=%s LIMIT 1;", (h,))
            existing = cur.fetchone()

            if not existing:
                cur.execute(
                    """
                    INSERT INTO spy_reports (kingdom, defense_power, castles, created_at, raw, report_hash)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    RETURNING id, kingdom, defense_power, castles, created_at;
                    """,
                    (kingdom, dp, castles, ts, msg.content, h)
                )
                row = cur.fetchone()
                ensure_ap_session(kingdom)
                await msg.channel.send(embed=build_spy_embed(row))

    except Exception as e:
        await send_error(msg.guild, str(e))

    await bot.process_commands(msg)

# ---------- Spy Commands ----------
@bot.command()
async def spy(ctx, *, kingdom: str):
    real = fuzzy_kingdom(kingdom) or kingdom
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, kingdom, defense_power, castles, created_at FROM spy_reports WHERE kingdom=%s ORDER BY created_at DESC LIMIT 1;",
            (real,)
        )
        row = cur.fetchone()
    if not row:
        return await ctx.send("❌ No spy report found.")
    await ctx.send(embed=build_spy_embed(row))

@bot.command()
async def spyid(ctx, sid: int):
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, kingdom, defense_power, castles, created_at FROM spy_reports WHERE id=%s;",
            (sid,)
        )
        row = cur.fetchone()
    if not row:
        return await ctx.send("❌ Invalid spy report ID.")
    await ctx.send(embed=build_spy_embed(row))

@bot.command()
async def spyhistory(ctx, *, kingdom: str):
    real = fuzzy_kingdom(kingdom) or kingdom
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id, defense_power, created_at FROM spy_reports WHERE kingdom=%s ORDER BY created_at DESC LIMIT 5;",
            (real,)
        )
        rows = cur.fetchall()
    if not rows:
        return await ctx.send("❌ No spy history found.")

    out = [f"🗂 **Spy History — {real}**"]
    for r in rows:
        out.append(f"ID `{r['id']}` • DP `{r['defense_power']:,}` • {r['created_at']}")
    await ctx.send("\n".join(out))

# ---------- Calc (Correct Output) ----------
@bot.command()
async def calc(ctx):
    await ctx.send("📄 Paste spy report:")
    try:
        msg = await bot.wait_for("message", timeout=300, check=lambda m: m.author == ctx.author and m.channel == ctx.channel)
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

# ---------- AP Planner (Buttons + Reset) ----------
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
                cur.execute(
                    "SELECT id, current_dp FROM dp_sessions WHERE kingdom=%s ORDER BY captured_at DESC LIMIT 1;",
                    (self.kingdom,)
                )
                row = cur.fetchone()
                if not row:
                    return await interaction.response.send_message("❌ No active AP session.", ephemeral=True)

                reduce_amt = ceil((row["current_dp"] or 0) * reduction)
                new_dp = max(0, (row["current_dp"] or 0) - reduce_amt)

                cur.execute(
                    "UPDATE dp_sessions SET current_dp=%s, hits=hits+1, last_hit=%s WHERE id=%s;",
                    (new_dp, interaction.user.display_name, row["id"])
                )

        await interaction.response.edit_message(embed=build_ap_embed(self.kingdom), view=self.view)

class APResetButton(Button):
    def __init__(self, kingdom: str):
        super().__init__(label="Reset", style=discord.ButtonStyle.secondary)
        self.kingdom = kingdom

    async def callback(self, interaction: discord.Interaction):
        async with ap_lock:
            with db_connect() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT id, base_dp FROM dp_sessions WHERE kingdom=%s ORDER BY captured_at DESC LIMIT 1;",
                    (self.kingdom,)
                )
                row = cur.fetchone()
                if not row:
                    return await interaction.response.send_message("❌ No active AP session.", ephemeral=True)

                cur.execute(
                    "UPDATE dp_sessions SET current_dp=%s, hits=0, last_hit=NULL WHERE id=%s;",
                    (row["base_dp"], row["id"])
                )

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

# ---------- NEW: Tech Commands ----------
@bot.command()
async def techindex(ctx):
    """
    Scans all saved spy_reports.raw, extracts tech section, filters battle-related,
    upserts into player_tech for fast lookup.
    """
    async with tech_index_lock:
        await ctx.send("🔎 Scanning saved spy reports for battle-related trainings...")

        scanned_reports = 0
        extracted_lines = 0
        upserts = 0

        with db_connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT id, kingdom, created_at, raw FROM spy_reports WHERE raw IS NOT NULL ORDER BY created_at DESC;")
            reports = cur.fetchall()

            for r in reports:
                scanned_reports += 1
                kingdom = r["kingdom"]
                if not kingdom:
                    continue

                techs = extract_tech_from_raw(r["raw"])
                if not techs:
                    continue

                # Keep only battle-related tech lines
                techs = [(name, lvl) for (name, lvl) in techs if is_battle_related_tech(name)]
                if not techs:
                    continue

                extracted_lines += len(techs)
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

        await ctx.send(f"✅ Tech index updated.\nReports scanned: {scanned_reports:,}\nBattle-tech lines found: {extracted_lines:,}\nRows upserted: {upserts:,}")

@bot.command()
async def tech(ctx, *, kingdom: str):
    """
    Shows battle-related training/tech we last saw for a player.
    """
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
        return await ctx.send(f"❌ No indexed battle-related tech for **{real}**.\nRun `!techindex` first (or spy them again).")

    embed = discord.Embed(title=f"📚 Battle Trainings • {real}", color=0x2ECC71)
    # Discord embed field limits: keep it readable
    lines = [f"• {r['tech_name']} — lvl {r['tech_level']} (last: {r['last_seen']})" for r in rows[:25]]
    embed.description = "\n".join(lines)
    if len(rows) > 25:
        embed.set_footer(text=f"Showing 25 of {len(rows)}. (Add pagination later if needed)")
    await ctx.send(embed=embed)

@bot.command()
async def techtop(ctx):
    """
    Shows the most common battle-related tech across all indexed players.
    """
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

# ---------- Run ----------
bot.run(TOKEN)
