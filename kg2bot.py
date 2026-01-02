# ---------- KG2 Recon Bot • FULL FINAL BUILD (PostgreSQL) ----------
# Spy Capture + Embed Display • Spy History • Spy ID Lookup
# Calc (HC fixed @ 7 AP + AP→HC estimates)
# AP Planner w/ Buttons + Reset • AP Status • Session Locking • Error Logging

import os, re, asyncio, difflib, hashlib, logging, time
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
DATABASE_URL = os.getenv("DATABASE_URL") or "postgresql://kg2bot_db_user:tH4jvQNiIAvE8jmIVbVCMxsFnG1hvccA@dpg-d54eklm3jp1c73970rdg-a/kg2bot_db"
ERROR_CHANNEL_NAME = "kg2recon-updates"

logging.basicConfig(level=logging.INFO)

# ---------- Database Connection ----------
def connect_db(retries=5, delay=3):
    for i in range(retries):
        try:
            conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
            conn.autocommit = True
            return conn
        except Exception as e:
            logging.warning(f"DB connection failed (attempt {i+1}): {e}")
            time.sleep(delay)
    raise Exception("Could not connect to the database after several retries")

conn = connect_db()

# ---------- Table Setup ----------
with conn.cursor() as cur:
    cur.execute("""
    CREATE TABLE IF NOT EXISTS spy_reports (
        id SERIAL PRIMARY KEY,
        kingdom TEXT,
        defense_power INTEGER,
        castles INTEGER,
        captured_at TIMESTAMPTZ,
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

# ---------- Discord ----------
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# ---------- Global Locks ----------
ap_lock = asyncio.Lock()

# ---------- Constants ----------
HEAVY_CAVALRY_AP = 7  # ✅ KG2 Correct value

AP_MULTIPLIERS = {
    "minor": 1.2,          # ~20% reduction
    "victory": 1.55,       # ~35% reduction
    "major": 2.2,          # ~55% reduction
    "overwhelming": 8.0    # ~87.5% reduction
}

# ---------- Helpers ----------
def castle_bonus(c):
    return (c ** 0.5) / 100 if c else 0

def hash_report(text):
    return hashlib.sha256(text.encode()).hexdigest()

def parse_spy(text):
    kingdom, dp, castles = None, None, 0
    for line in text.splitlines():
        if line.lower().startswith("target:"):
            kingdom = line.split(":", 1)[1].strip()
        if "defensive power" in line.lower() or "approximate defensive power" in line.lower():
            m = re.search(r"\d+", line.replace(",", ""))
            if m:
                dp = int(m.group())
        if "number of castles" in line.lower():
            m = re.search(r"\d+", line)
            if m:
                castles = int(m.group())
    return kingdom, dp, castles

def fuzzy_kingdom(query):
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT kingdom FROM spy_reports")
        names = [r['kingdom'] for r in cur.fetchall()]
    match = difflib.get_close_matches(query, names, 1, 0.5)
    return match[0] if match else None

def ensure_ap_session(kingdom):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM dp_sessions WHERE kingdom=%s ORDER BY captured_at DESC LIMIT 1", (kingdom,))
        if cur.fetchone():
            return True

        cur.execute(
            "SELECT defense_power, castles, captured_at FROM spy_reports WHERE kingdom=%s ORDER BY captured_at DESC LIMIT 1",
            (kingdom,)
        )
        spy = cur.fetchone()
        if not spy:
            return False

        dp, castles, ts = spy['defense_power'], spy['castles'], spy['captured_at']
        cur.execute(
            "INSERT INTO dp_sessions (kingdom, base_dp, castles, current_dp, hits, last_hit, captured_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (kingdom, dp, castles, dp, 0, None, ts)
        )
    return True

# ---------- Startup ----------
@bot.event
async def on_ready():
    logging.info(f"KG2 Recon Bot logged in as {bot.user}")
    # Announce startup in the error/update channel
    for guild in bot.guilds:
        ch = discord.utils.get(guild.text_channels, name=ERROR_CHANNEL_NAME)
        if ch:
            await ch.send(f"✅ KG2 Recon Bot has started and is online! Logged in as {bot.user}")


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
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM spy_reports WHERE report_hash=%s", (h,))
            if not cur.fetchone():
                ts = datetime.now(timezone.utc).isoformat()
                cur.execute(
                    "INSERT INTO spy_reports (kingdom, defense_power, castles, captured_at, raw, report_hash) "
                    "VALUES (%s,%s,%s,%s,%s,%s)",
                    (kingdom, dp, castles, ts, msg.content, h)
                )
                ensure_ap_session(kingdom)

                cur.execute(
                    "SELECT id, kingdom, defense_power, castles, captured_at FROM spy_reports WHERE report_hash=%s",
                    (h,)
                )
                row = cur.fetchone()
                await msg.channel.send(embed=build_spy_embed(row))

    except Exception as e:
        await send_error(msg.guild, str(e))

    await bot.process_commands(msg)

# ---------- Embeds ----------
def build_spy_embed(row):
    sid, kingdom, dp, castles, ts = row['id'], row['kingdom'], row['defense_power'], row['castles'], row['captured_at']
    adjusted = ceil(dp * (1 + castle_bonus(castles)))

    embed = discord.Embed(title="🕵️ Spy Report", color=0x5865F2)
    embed.add_field(name="Kingdom", value=kingdom, inline=False)
    embed.add_field(name="Base DP", value=f"{dp:,}", inline=True)
    embed.add_field(name="Adjusted DP", value=f"{adjusted:,}", inline=True)
    embed.add_field(name="Castles", value=castles, inline=True)
    embed.set_footer(text=f"ID {sid} • Captured {ts}")
    return embed

def build_ap_embed(kingdom):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT base_dp, current_dp, hits, last_hit FROM dp_sessions WHERE kingdom=%s ORDER BY captured_at DESC LIMIT 1",
            (kingdom,)
        )
        row = cur.fetchone()
    if not row:
        return None

    base, dp, hits, last = row['base_dp'], row['current_dp'], row['hits'], row['last_hit']
    embed = discord.Embed(title=f"⚔️ AP Planner • {kingdom}", color=0xE74C3C)
    embed.add_field(name="Base DP", value=f"{base:,}")
    embed.add_field(name="Current DP", value=f"{dp:,}")
    embed.add_field(name="Hits Applied", value=hits)
    if last:
        embed.set_footer(text=f"Last hit by {last}")
    return embed

async def send_error(guild, msg):
    try:
        ch = discord.utils.get(guild.text_channels, name=ERROR_CHANNEL_NAME)
        if ch:
            await ch.send(f"⚠️ ERROR LOG:\n```py\n{msg}\n```")
    except:
        pass

# ---------- Commands ----------
@bot.command()
async def calc(ctx):
    await ctx.send("📄 Paste spy report:")
    try:
        msg = await bot.wait_for("message", timeout=300, check=lambda m: m.author == ctx.author)
    except asyncio.TimeoutError:
        return await ctx.send("⏰ Timed out.")

    kingdom, dp, castles = parse_spy(msg.content)
    if not kingdom or not dp:
        return await ctx.send("❌ Could not parse spy report.")

    adjusted_dp = ceil(dp * (1 + castle_bonus(castles)))

    def hc_needed(value):
        return ceil(value / HEAVY_CAVALRY_AP)

    embed = discord.Embed(title="⚔️ Combat Calculator (KG2)", color=0x5865F2)
    embed.add_field(name="Target", value=kingdom, inline=False)
    embed.add_field(name="Base DP", value=f"{dp:,}")
    embed.add_field(name="Adjusted DP", value=f"{adjusted_dp:,}")
    embed.add_field(name="HC Needed (No AP)", value=f"{hc_needed(adjusted_dp):,}", inline=False)

    for k, m in AP_MULTIPLIERS.items():
        remaining = adjusted_dp - int(adjusted_dp / m)
        embed.add_field(
            name=f"After {k.title()}",
            value=f"{hc_needed(remaining):,} HC\nRemaining DP: {remaining:,}",
            inline=True
        )

    embed.set_footer(text="HC = 7 AP | Percent-based KG2 reductions")
    await ctx.send(embed=embed)

# ---------- AP Planner ----------
class APView(View):
    def __init__(self, kingdom):
        super().__init__(timeout=None)
        self.kingdom = kingdom
        for k in AP_MULTIPLIERS:
            self.add_item(APButton(k, kingdom))
        self.add_item(APResetButton(kingdom))

class APButton(Button):
    def __init__(self, label, kingdom):
        super().__init__(label=label.title(), style=discord.ButtonStyle.danger)
        self.key = label
        self.kingdom = kingdom

    async def callback(self, interaction):
        async with ap_lock:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, current_dp FROM dp_sessions WHERE kingdom=%s ORDER BY captured_at DESC LIMIT 1",
                    (self.kingdom,)
                )
                sid, dp = cur.fetchone()['id'], cur.fetchone()['current_dp']
                reduction = int(dp / AP_MULTIPLIERS[self.key])
                new_dp = max(0, dp - reduction)
                cur.execute(
                    "UPDATE dp_sessions SET current_dp=%s, hits=hits+1, last_hit=%s WHERE id=%s",
                    (new_dp, interaction.user.display_name, sid)
                )
        await interaction.response.edit_message(embed=build_ap_embed(self.kingdom), view=self.view)

class APResetButton(Button):
    def __init__(self, kingdom):
        super().__init__(label="Reset", style=discord.ButtonStyle.secondary)
        self.kingdom = kingdom

    async def callback(self, interaction):
        async with ap_lock:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, base_dp FROM dp_sessions WHERE kingdom=%s ORDER BY captured_at DESC LIMIT 1",
                    (self.kingdom,)
                )
                sid, base = cur.fetchone()['id'], cur.fetchone()['base_dp']
                cur.execute(
                    "UPDATE dp_sessions SET current_dp=%s, hits=0, last_hit=NULL WHERE id=%s",
                    (base, sid)
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

@bot.command()
async def spy(ctx, *, kingdom: str):
    real = fuzzy_kingdom(kingdom) or kingdom
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, kingdom, defense_power, castles, captured_at FROM spy_reports WHERE kingdom=%s ORDER BY captured_at DESC LIMIT 1",
            (real,)
        )
        row = cur.fetchone()
    if not row:
        return await ctx.send("❌ No spy report found.")
    await ctx.send(embed=build_spy_embed(row))

@bot.command()
async def spyid(ctx, sid: int):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, kingdom, defense_power, castles, captured_at FROM spy_reports WHERE id=%s",
            (sid,)
        )
        row = cur.fetchone()
    if not row:
        return await ctx.send("❌ Invalid spy report ID.")
    await ctx.send(embed=build_spy_embed(row))

@bot.command()
async def spyhistory(ctx, *, kingdom: str):
    real = fuzzy_kingdom(kingdom) or kingdom
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, defense_power, captured_at FROM spy_reports WHERE kingdom=%s ORDER BY captured_at DESC LIMIT 5",
            (real,)
        )
        rows = cur.fetchall()
    if not rows:
        return await ctx.send("❌ No spy history found.")

    msg = f"🗂 **Spy History — {real}**\n"
    for r in rows:
        msg += f"ID `{r['id']}` • DP `{r['defense_power']:,}` • {r['captured_at']}\n"
    await ctx.send(msg)

# ---------- Run ----------
bot.run(TOKEN)
