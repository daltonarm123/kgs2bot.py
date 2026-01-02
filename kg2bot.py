# ---------- KG2 Recon Bot • FULL POSTGRESQL FINAL BUILD ----------
# Spy Capture + Embed Display • Spy History • Spy ID Lookup
# Calc (HC corrected with explicit % math)
# AP Planner w/ Buttons + Reset • AP Status • Session Locking • Error Logging

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

# PostgreSQL connection info
DB_HOST = os.getenv("DB_HOST", "dpg-d54eklm3jp1c73970rdg-a.oregon-postgres.render.com")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME", "kg2bot_db")
DB_USER = os.getenv("DB_USER", "kg2bot_db_user")
DB_PASS = os.getenv("DB_PASS", "tH4jvQNiIAvE8jmIVbVCMxsFnG1hvccA")

logging.basicConfig(level=logging.INFO)

# ---------- DB Helpers ----------
def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        cursor_factory=RealDictCursor
    )

def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS spy_reports (
                id SERIAL PRIMARY KEY,
                kingdom TEXT,
                defense_power BIGINT,
                castles INTEGER,
                captured_at TIMESTAMP WITH TIME ZONE,
                raw TEXT
            );
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS dp_sessions (
                id SERIAL PRIMARY KEY,
                kingdom TEXT,
                base_dp BIGINT,
                castles INTEGER,
                current_dp BIGINT,
                hits INTEGER,
                last_hit TEXT,
                captured_at TIMESTAMP WITH TIME ZONE
            );
            """)
        conn.commit()

init_db()

# ---------- Discord ----------
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# ---------- Global Locks ----------
ap_lock = asyncio.Lock()

# ---------- Constants ----------
HEAVY_CAVALRY_AP = 7  # ✅ KG2 value
AP_MULTIPLIERS = {
    "minor": 0.19,         # 19%
    "victory": 0.35,       # 35%
    "major": 0.55,         # 55%
    "overwhelming": 0.87   # 87%
}

# ---------- Helpers ----------
def castle_bonus(c):
    return (c ** 0.5) / 100 if c else 0

def parse_spy(text):
    kingdom, dp, castles = None, None, 0
    for line in text.splitlines():
        if line.lower().startswith("target:"):
            kingdom = line.split(":", 1)[1].strip()
        if "defensive power" in line.lower():
            m = re.search(r"\d+", line.replace(",", ""))
            if m:
                dp = int(m.group())
        if "number of castles" in line.lower():
            m = re.search(r"\d+", line)
            if m:
                castles = int(m.group())
    return kingdom, dp, castles

def fuzzy_kingdom(query):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT kingdom FROM spy_reports")
            names = [r["kingdom"] for r in cur.fetchall()]
    match = difflib.get_close_matches(query, names, 1, 0.5)
    return match[0] if match else None

def ensure_ap_session(kingdom):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM dp_sessions WHERE kingdom=%s ORDER BY captured_at DESC LIMIT 1",
                (kingdom,)
            )
            if cur.fetchone():
                return True
            cur.execute(
                "SELECT defense_power, castles, captured_at FROM spy_reports WHERE kingdom=%s ORDER BY captured_at DESC LIMIT 1",
                (kingdom,)
            )
            spy = cur.fetchone()
            if not spy:
                return False
            dp, castles, ts = spy["defense_power"], spy["castles"], spy["captured_at"]
            cur.execute(
                "INSERT INTO dp_sessions (kingdom, base_dp, castles, current_dp, hits, last_hit, captured_at) "
                "VALUES (%s,%s,%s,%s,0,NULL,%s)",
                (kingdom, dp, castles, dp, ts)
            )
            conn.commit()
            return True

# ---------- Startup ----------
@bot.event
async def on_ready():
    logging.info(f"KG2 Recon Bot logged in as {bot.user}")
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

        # Check if exact same raw report exists
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM spy_reports WHERE raw=%s LIMIT 1",
                    (msg.content,)
                )
                if cur.fetchone() is None:
                    ts = datetime.now(timezone.utc)
                    cur.execute(
                        "INSERT INTO spy_reports (kingdom, defense_power, castles, captured_at, raw) "
                        "VALUES (%s,%s,%s,%s,%s) RETURNING id, kingdom, defense_power, castles, captured_at",
                        (kingdom, dp, castles, ts, msg.content)
                    )
                    row = cur.fetchone()
                    conn.commit()
                    ensure_ap_session(kingdom)
                    await msg.channel.send(embed=build_spy_embed(row))
    except Exception as e:
        await send_error(msg.guild, str(e))
    await bot.process_commands(msg)

# ---------- Embeds ----------
def build_spy_embed(row):
    sid, kingdom, dp, castles, ts = row["id"], row["kingdom"], row["defense_power"], row["castles"], row["captured_at"]
    adjusted = ceil(dp * (1 + castle_bonus(castles)))
    embed = discord.Embed(title="🕵️ Spy Report", color=0x5865F2)
    embed.add_field(name="Kingdom", value=kingdom, inline=False)
    embed.add_field(name="Base DP", value=f"{dp:,}")
    embed.add_field(name="Adjusted DP", value=f"{adjusted:,}")
    embed.add_field(name="Castles", value=castles)
    embed.set_footer(text=f"ID {sid} • Captured {ts}")
    return embed

def build_ap_embed(kingdom):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT base_dp, current_dp, hits, last_hit FROM dp_sessions WHERE kingdom=%s ORDER BY captured_at DESC LIMIT 1",
                (kingdom,)
            )
            row = cur.fetchone()
    if not row:
        return None
    base, dp, hits, last = row["base_dp"], row["current_dp"], row["hits"], row["last_hit"]
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
    hc_needed = lambda val: ceil(val / HEAVY_CAVALRY_AP)

    embed = discord.Embed(title="⚔️ Combat Calculator (KG2)", color=0x5865F2)
    embed.add_field(name="Target", value=kingdom, inline=False)
    embed.add_field(name="Base DP", value=f"{dp:,}")
    embed.add_field(name="Adjusted DP", value=f"{adjusted_dp:,}")
    embed.add_field(name="HC Needed (No AP)", value=f"{hc_needed(adjusted_dp):,} HC", inline=False)

    for label, percent in AP_MULTIPLIERS.items():
        remaining = ceil(adjusted_dp * (1 - percent))
        embed.add_field(
            name=f"After {label.title()} Victory (−{int(percent*100)}%)",
            value=f"Remaining DP: {remaining:,}\nHC Required: {hc_needed(remaining):,}",
            inline=False
        )

    embed.set_footer(text=f"HC = {HEAVY_CAVALRY_AP} AP | Explicit KG2 remaining % math")
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
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT id, current_dp FROM dp_sessions WHERE kingdom=%s ORDER BY captured_at DESC LIMIT 1",
                        (self.kingdom,)
                    )
                    row = cur.fetchone()
                    sid, dp = row["id"], row["current_dp"]
                    reduction = ceil(dp * AP_MULTIPLIERS[self.key])
                    new_dp = max(0, dp - reduction)
                    cur.execute(
                        "UPDATE dp_sessions SET current_dp=%s, hits=hits+1, last_hit=%s WHERE id=%s",
                        (new_dp, interaction.user.display_name, sid)
                    )
                    conn.commit()
        await interaction.response.edit_message(embed=build_ap_embed(self.kingdom), view=self.view)

class APResetButton(Button):
    def __init__(self, kingdom):
        super().__init__(label="Reset", style=discord.ButtonStyle.secondary)
        self.kingdom = kingdom

    async def callback(self, interaction):
        async with ap_lock:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT id, base_dp FROM dp_sessions WHERE kingdom=%s ORDER BY captured_at DESC LIMIT 1",
                        (self.kingdom,)
                    )
                    row = cur.fetchone()
                    sid, base = row["id"], row["base_dp"]
                    cur.execute(
                        "UPDATE dp_sessions SET current_dp=%s, hits=0, last_hit=NULL WHERE id=%s",
                        (base, sid)
                    )
                    conn.commit()
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

# ---------- Run ----------
bot.run(TOKEN)
