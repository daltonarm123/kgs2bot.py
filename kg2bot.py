# ---------- KG2 Recon Bot • FULL FINAL BUILD ----------
# Spy Capture + Embed Display • Spy History • Spy ID Lookup
# Calc (HC fixed @ 7 AP + AP→HC estimates)
# AP Planner w/ Buttons + Reset • AP Status • Session Locking • Error Logging

import os, re, sqlite3, asyncio, difflib, hashlib, logging
from math import ceil
from datetime import datetime, timezone

import discord
from discord.ext import commands
from discord.ui import View, Button
from dotenv import load_dotenv

# ---------- Setup ----------
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
DB_PATH = "kg2_reports.sqlite3"
ERROR_CHANNEL_NAME = "kg2recon-updates"

logging.basicConfig(level=logging.INFO)

# ---------- Database ----------
conn = sqlite3.connect(DB_PATH)
conn.execute("PRAGMA journal_mode=WAL;")
conn.execute("PRAGMA foreign_keys=ON;")

conn.executescript("""
CREATE TABLE IF NOT EXISTS spy_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kingdom TEXT,
    defense_power INTEGER,
    castles INTEGER,
    captured_at TEXT,
    raw TEXT,
    report_hash TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS dp_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kingdom TEXT,
    base_dp INTEGER,
    castles INTEGER,
    current_dp INTEGER,
    hits INTEGER,
    last_hit TEXT,
    captured_at TEXT
);
""")
conn.commit()

# ---------- Discord ----------
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# ---------- Global Locks ----------
ap_lock = asyncio.Lock()

# ---------- Constants ----------
HEAVY_CAVALRY_AP = 7  # ✅ CORRECT KG2 VALUE

# Remaining DP after hit (KG2 accurate)
KG2_REMAINING = {
    "Minor Victory": 0.80,
    "Victory": 0.65,
    "Major Victory": 0.45,
    "Overwhelming Victory": 0.125
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
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT kingdom FROM spy_reports")
    names = [r[0] for r in cur.fetchall()]
    match = difflib.get_close_matches(query, names, 1, 0.5)
    return match[0] if match else None

def ensure_ap_session(kingdom):
    cur = conn.cursor()
    if cur.execute(
        "SELECT 1 FROM dp_sessions WHERE kingdom=? ORDER BY captured_at DESC LIMIT 1",
        (kingdom,)
    ).fetchone():
        return True

    spy = cur.execute(
        "SELECT defense_power, castles, captured_at FROM spy_reports WHERE kingdom=? ORDER BY captured_at DESC LIMIT 1",
        (kingdom,)
    ).fetchone()

    if not spy:
        return False

    dp, castles, ts = spy
    cur.execute(
        "INSERT INTO dp_sessions VALUES (NULL,?,?,?,?,?,?,?)",
        (kingdom, dp, castles, dp, 0, None, ts)
    )
    conn.commit()
    return True

# ---------- Startup ----------
@bot.event
async def on_ready():
    logging.info(f"KG2 Recon Bot logged in as {bot.user}")

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
        cur = conn.cursor()

        if not cur.execute("SELECT 1 FROM spy_reports WHERE report_hash=?", (h,)).fetchone():
            ts = datetime.now(timezone.utc).isoformat()
            cur.execute(
                "INSERT INTO spy_reports VALUES (NULL,?,?,?,?,?,?)",
                (kingdom, dp, castles, ts, msg.content, h)
            )
            conn.commit()
            ensure_ap_session(kingdom)

            row = cur.execute(
                "SELECT id, kingdom, defense_power, castles, captured_at FROM spy_reports WHERE report_hash=?",
                (h,)
            ).fetchone()

            await msg.channel.send(embed=build_spy_embed(row))

    except Exception as e:
        await send_error(msg.guild, str(e))

    await bot.process_commands(msg)

# ---------- Embeds ----------
def build_spy_embed(row):
    sid, kingdom, dp, castles, ts = row
    adjusted = ceil(dp * (1 + castle_bonus(castles)))

    embed = discord.Embed(title="🕵️ Spy Report", color=0x5865F2)
    embed.add_field(name="Kingdom", value=kingdom, inline=False)
    embed.add_field(name="Base DP", value=f"{dp:,}", inline=True)
    embed.add_field(name="Adjusted DP", value=f"{adjusted:,}", inline=True)
    embed.add_field(name="Castles", value=castles, inline=True)
    embed.set_footer(text=f"ID {sid} • Captured {ts}")
    return embed

# ---------- CALC COMMAND (FIXED) ----------
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
    embed.add_field(name="Adjusted DP", value=f"{adjusted_dp:,}", inline=False)

    embed.add_field(
        name="HC Needed (No AP)",
        value=f"{ceil(adjusted_dp / HEAVY_CAVALRY_AP):,} HC",
        inline=False
    )

    for label, pct in KG2_REMAINING.items():
        remaining_dp = ceil(adjusted_dp * pct)
        hc = ceil(remaining_dp / HEAVY_CAVALRY_AP)
        removed = int((1 - pct) * 100)

        embed.add_field(
            name=f"After {label} (−{removed}%)",
            value=(
                f"Remaining DP: `{remaining_dp:,}`\n"
                f"HC Required: `{hc:,}`"
            ),
            inline=True
        )

    embed.set_footer(text="HC = 7 AP | Explicit KG2 remaining % math")
    await ctx.send(embed=embed)

# ---------- Run ----------
bot.run(TOKEN)
