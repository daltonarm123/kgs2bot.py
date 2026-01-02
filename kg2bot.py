# ---------- KG2 Recon Bot • PostgreSQL Version ----------
# Spy Capture + Embed Display • Spy History • Spy ID Lookup
# Calc (HC fixed @ 7 AP + AP→HC estimates)
# AP Planner w/ Buttons + Reset • AP Status • Session Locking • Error Logging
# Startup message to kg2recon-updates

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
DB_HOST = os.getenv("DB_HOST", "dpg-d54eklm3jp1c73970rdg-a.oregon-postgres.render.com")
DB_NAME = os.getenv("DB_NAME", "kg2bot_db")
DB_USER = os.getenv("DB_USER", "kg2bot_db_user")
DB_PASS = os.getenv("DB_PASS", "tH4jvQNiIAvE8jmIVbVCMxsFnG1hvccA")
ERROR_CHANNEL_NAME = "kg2recon-updates"

logging.basicConfig(level=logging.INFO)

# ---------- Database ----------
def get_conn():
    for _ in range(5):
        try:
            return psycopg2.connect(
                host=DB_HOST,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASS,
                cursor_factory=RealDictCursor,
                sslmode="require"
            )
        except Exception as e:
            logging.error(f"DB connection failed, retrying... {e}")
            asyncio.sleep(1)
    raise ConnectionError("Could not connect to database.")

conn = get_conn()
conn.autocommit = True
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS spy_reports (
    id SERIAL PRIMARY KEY,
    kingdom TEXT,
    alliance TEXT,
    defense_power INTEGER,
    castles INTEGER,
    created_at TIMESTAMP,
    raw TEXT,
    report_hash TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS dp_sessions (
    id SERIAL PRIMARY KEY,
    kingdom TEXT,
    base_dp INTEGER,
    castles INTEGER,
    current_dp INTEGER,
    hits INTEGER,
    last_hit TEXT,
    captured_at TIMESTAMP
);
""")

# ---------- Discord ----------
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# ---------- Global Locks ----------
ap_lock = asyncio.Lock()

# ---------- Constants ----------
HEAVY_CAVALRY_AP = 7  # ✅ Correct KG2 value

AP_MULTIPLIERS = {
    "minor": 0.19,         # −19%
    "victory": 0.35,       # −35%
    "major": 0.55,         # −55%
    "overwhelming": 0.87   # −87%
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
    cur.execute("SELECT DISTINCT kingdom FROM spy_reports")
    names = [r['kingdom'] for r in cur.fetchall()]
    match = difflib.get_close_matches(query, names, 1, 0.5)
    return match[0] if match else None

def ensure_ap_session(kingdom):
    cur.execute(
        "SELECT 1 FROM dp_sessions WHERE kingdom=%s ORDER BY captured_at DESC LIMIT 1",
        (kingdom,)
    )
    if cur.fetchone():
        return True

    cur.execute(
        "SELECT defense_power, castles, created_at FROM spy_reports WHERE kingdom=%s ORDER BY created_at DESC LIMIT 1",
        (kingdom,)
    )
    spy = cur.fetchone()
    if not spy:
        return False
    dp, castles, ts = spy['defense_power'], spy['castles'], spy['created_at']
    cur.execute(
        "INSERT INTO dp_sessions (kingdom, base_dp, castles, current_dp, hits, last_hit, captured_at) VALUES (%s,%s,%s,%s,%s,%s,%s)",
        (kingdom, dp, castles, dp, 0, None, ts)
    )
    return True

# ---------- Startup ----------
@bot.event
async def on_ready():
    logging.info(f"KG2 Recon Bot logged in as {bot.user}")
    for guild in bot.guilds:
        ch = discord.utils.get(guild.text_channels, name=ERROR_CHANNEL_NAME)
        if ch:
            await ch.send(f"✅ KG2 Recon Bot started up at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

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
        cur.execute("SELECT 1 FROM spy_reports WHERE report_hash=%s", (h,))
        if not cur.fetchone():
            ts = datetime.now(timezone.utc)
            cur.execute(
                "INSERT INTO spy_reports (kingdom, defense_power, castles, created_at, raw, report_hash) VALUES (%s,%s,%s,%s,%s,%s)",
                (kingdom, dp, castles, ts, msg.content, h)
            )
            ensure_ap_session(kingdom)

            cur.execute("SELECT id, kingdom, defense_power, castles, created_at FROM spy_reports WHERE report_hash=%s", (h,))
            row = cur.fetchone()
            await msg.channel.send(embed=build_spy_embed(row))

    except Exception as e:
        await send_error(msg.guild, str(e))
    await bot.process_commands(msg)

# ---------- Embeds ----------
def build_spy_embed(row):
    adjusted = ceil(row['defense_power'] * (1 + castle_bonus(row['castles'])))
    embed = discord.Embed(title="🕵️ Spy Report", color=0x5865F2)
    embed.add_field(name="Kingdom", value=row['kingdom'], inline=False)
    embed.add_field(name="Base DP", value=f"{row['defense_power']:,}", inline=True)
    embed.add_field(name="Adjusted DP", value=f"{adjusted:,}", inline=True)
    embed.add_field(name="Castles", value=row['castles'], inline=True)
    embed.set_footer(text=f"ID {row['id']} • Captured {row['created_at']}")
    return embed

def build_ap_embed(kingdom):
    cur.execute("SELECT base_dp, current_dp, hits, last_hit FROM dp_sessions WHERE kingdom=%s ORDER BY captured_at DESC LIMIT 1", (kingdom,))
    row = cur.fetchone()
    if not row:
        return None
    embed = discord.Embed(title=f"⚔️ AP Planner • {kingdom}", color=0xE74C3C)
    embed.add_field(name="Base DP", value=f"{row['base_dp']:,}")
    embed.add_field(name="Current DP", value=f"{row['current_dp']:,}")
    embed.add_field(name="Hits Applied", value=row['hits'])
    if row['last_hit']:
        embed.set_footer(text=f"Last hit by {row['last_hit']}")
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
    hc_needed_no_ap = ceil(adjusted_dp / HEAVY_CAVALRY_AP)

    embed = discord.Embed(title="⚔️ Combat Calculator (KG2)", color=0x5865F2)
    embed.add_field(name="Target", value=kingdom, inline=False)
    embed.add_field(name="Base DP", value=f"{dp:,}")
    embed.add_field(name="Adjusted DP", value=f"{adjusted_dp:,}")
    embed.add_field(name="HC Needed (No AP)", value=f"{hc_needed_no_ap:,} HC", inline=False)

    remaining = adjusted_dp
    for label, pct in AP_MULTIPLIERS.items():
        remaining_dp = ceil(remaining * (1 - pct))
        hc_required = ceil(remaining_dp / HEAVY_CAVALRY_AP)
        embed.add_field(name=f"After {label.title()} Victory (−{int(pct*100)}%)",
                        value=f"Remaining DP: {remaining_dp:,}\nHC Required: {hc_required:,}",
                        inline=False)
        remaining = remaining_dp

    embed.set_footer(text="HC = 7 AP | Explicit KG2 remaining % math")
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
            cur.execute("SELECT id, current_dp FROM dp_sessions WHERE kingdom=%s ORDER BY captured_at DESC LIMIT 1", (self.kingdom,))
            row = cur.fetchone()
            reduction = int(row['current_dp'] * AP_MULTIPLIERS[self.key])
            new_dp = max(0, row['current_dp'] - reduction)
            cur.execute("UPDATE dp_sessions SET current_dp=%s, hits=hits+1, last_hit=%s WHERE id=%s",
                        (new_dp, interaction.user.display_name, row['id']))
        await interaction.response.edit_message(embed=build_ap_embed(self.kingdom), view=self.view)

class APResetButton(Button):
    def __init__(self, kingdom):
        super().__init__(label="Reset", style=discord.ButtonStyle.secondary)
        self.kingdom = kingdom

    async def callback(self, interaction):
        async with ap_lock:
            cur.execute("SELECT id, base_dp FROM dp_sessions WHERE kingdom=%s ORDER BY captured_at DESC LIMIT 1", (self.kingdom,))
            row = cur.fetchone()
            cur.execute("UPDATE dp_sessions SET current_dp=%s, hits=0, last_hit=NULL WHERE id=%s",
                        (row['base_dp'], row['id']))
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
