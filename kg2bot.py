# ---------- KG2 Recon Bot • FULL FINAL POSTGRES BUILD ----------
# Spy Capture + Embed Display • Spy History • Spy ID Lookup
# Calc (HC fixed @ 7 AP + AP→HC estimates)
# AP Planner w/ Buttons + Reset • AP Status • Session Locking • Error Logging
# Startup announces to #kg2recon-updates

import os, re, asyncio, hashlib, logging
from math import ceil
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import RealDictCursor
import discord
from discord.ext import commands
from discord.ui import View, Button
from dotenv import load_dotenv

# ---------- Setup ----------
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")

# PostgreSQL connection info (Render)
DB_HOST = os.getenv("DB_HOST", "dpg-d54eklm3jp1c73970rdg-a.oregon-postgres.render.com")
DB_NAME = os.getenv("DB_NAME", "kg2bot_db")
DB_USER = os.getenv("DB_USER", "kg2bot_db_user")
DB_PASS = os.getenv("DB_PASS", "tH4jvQNiIAvE8jmIVbVCMxsFnG1hvccA")
DB_PORT = int(os.getenv("DB_PORT", 5432))

ERROR_CHANNEL_NAME = "kg2recon-updates"

logging.basicConfig(level=logging.INFO)

# ---------- Constants ----------
HEAVY_CAVALRY_AP = 7  # ✅ KG2 value

# Explicit % reductions
AP_MULTIPLIERS = {
    "minor": 0.19,         # 19%
    "victory": 0.35,       # 35%
    "major": 0.55,         # 55%
    "overwhelming": 0.875  # 87.5%
}

# ---------- Database ----------
def get_db_conn():
    """Returns a new connection to PostgreSQL"""
    for _ in range(5):  # retry 5 times
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASS,
                port=DB_PORT,
                cursor_factory=RealDictCursor,
                sslmode='require'
            )
            return conn
        except Exception as e:
            logging.warning(f"Postgres connect failed: {e}")
            asyncio.sleep(2)
    raise Exception("Could not connect to Postgres.")

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
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT kingdom FROM spy_reports")
    names = [r['kingdom'] for r in cur.fetchall()]
    conn.close()
    import difflib
    match = difflib.get_close_matches(query, names, 1, 0.5)
    return match[0] if match else None

def hc_needed(dp_value):
    return ceil(dp_value / HEAVY_CAVALRY_AP)

# ---------- Discord ----------
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

ap_lock = asyncio.Lock()

# ---------- Startup ----------
@bot.event
async def on_ready():
    logging.info(f"KG2 Recon Bot logged in as {bot.user}")
    try:
        for guild in bot.guilds:
            ch = discord.utils.get(guild.text_channels, name=ERROR_CHANNEL_NAME)
            if ch:
                await ch.send(f"⚡ KG2 Recon Bot has started up! Logged in as {bot.user}.")
    except Exception as e:
        logging.error(f"Startup announcement failed: {e}")

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
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM spy_reports WHERE report_hash=%s", (h,))
        if not cur.fetchone():
            ts = datetime.now(timezone.utc)
            cur.execute("""
                INSERT INTO spy_reports (kingdom, defense_power, castles, created_at, raw, report_hash)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (kingdom, dp, castles, ts, msg.content, h))
            conn.commit()
        conn.close()
    except Exception as e:
        await send_error(msg.guild, str(e))
    await bot.process_commands(msg)

# ---------- Error Logging ----------
async def send_error(guild, msg):
    try:
        ch = discord.utils.get(guild.text_channels, name=ERROR_CHANNEL_NAME)
        if ch:
            await ch.send(f"⚠️ ERROR LOG:\n```py\n{msg}\n```")
    except:
        pass

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
    embed = discord.Embed(title="⚔️ Combat Calculator (KG2)", color=0x5865F2)
    embed.add_field(name="Target", value=kingdom, inline=False)
    embed.add_field(name="Base DP", value=f"{dp:,}")
    embed.add_field(name="Adjusted DP", value=f"{adjusted_dp:,}")

    # HC Needed (No AP)
    embed.add_field(name="HC Needed (No AP)", value=f"{hc_needed(adjusted_dp):,} HC", inline=False)

    # After Minor/Victory/Major/Overwhelming
    current_dp = adjusted_dp
    for key, reduction in AP_MULTIPLIERS.items():
        remaining_dp = ceil(current_dp * (1 - reduction))
        hc_req = hc_needed(remaining_dp)
        embed.add_field(
            name=f"After {key.title()} Victory (−{int(reduction*100)}%)",
            value=f"Remaining DP: {remaining_dp:,}\nHC Required: {hc_req:,}",
            inline=False
        )
        current_dp = remaining_dp

    embed.set_footer(text=f"HC = {HEAVY_CAVALRY_AP} AP | Explicit KG2 remaining % math")
    await ctx.send(embed=embed)

# ---------- Run ----------
bot.run(TOKEN)
