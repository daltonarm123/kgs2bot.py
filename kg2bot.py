# KG2 Recon Bot (discord.py v2)
# Sleek embeds • Auto-capture • History • AP Planner • Castle bonus • DP chaining • Calc feature

import os, re, json, sqlite3, asyncio
from math import ceil
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple, List

import discord
from discord.ext import commands
from dotenv import load_dotenv

# ---------- Env ----------
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
DB_PATH = os.getenv("DB_PATH", "kg2_reports.sqlite3")

# ---------- Database ----------
conn = sqlite3.connect(DB_PATH)
conn.execute("PRAGMA journal_mode=WAL;")
conn.execute("PRAGMA foreign_keys=ON;")

SCHEMA = """
CREATE TABLE IF NOT EXISTS spy_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kingdom TEXT NOT NULL,
    alliance TEXT,
    honour REAL,
    ranking INTEGER,
    networth INTEGER,
    spies_sent INTEGER,
    spies_lost INTEGER,
    result_level TEXT,
    castles INTEGER,
    resources_json TEXT,
    troops_json TEXT,
    movements_json TEXT,
    markets_json TEXT,
    tech_json TEXT,
    defense_power INTEGER,
    captured_at TEXT,
    author_id TEXT,
    raw TEXT
);

CREATE INDEX IF NOT EXISTS idx_spy_kingdom_captured
ON spy_reports(kingdom, captured_at DESC);

CREATE TABLE IF NOT EXISTS channel_settings (
    guild_id TEXT,
    channel_id TEXT,
    autocapture INTEGER DEFAULT 0,
    default_kingdom TEXT,
    PRIMARY KEY (guild_id, channel_id)
);

CREATE TABLE IF NOT EXISTS dp_sessions (
    id INTEGER PRIMARY KEY,
    target TEXT NOT NULL,
    spy_report_id INTEGER,
    captured_at TEXT NOT NULL,
    base_dp_start INTEGER NOT NULL,
    castles INTEGER NOT NULL,
    current_base_dp INTEGER NOT NULL,
    current_with_castles INTEGER NOT NULL,
    hits_applied INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (spy_report_id) REFERENCES spy_reports(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS dp_hits (
    id INTEGER PRIMARY KEY,
    session_id INTEGER NOT NULL,
    hit_at TEXT NOT NULL,
    tier TEXT NOT NULL,
    attacker_casualties INTEGER,
    attacker_army_size INTEGER,
    land_gained INTEGER,
    FOREIGN KEY(session_id) REFERENCES dp_sessions(id) ON DELETE CASCADE
);
"""
conn.executescript(SCHEMA)
conn.commit()

# ---------- Discord ----------
intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
bot = commands.Bot(command_prefix="!", intents=intents)

# ---------- Theme ----------
THEME_COLOR = 0x5865F2

# ---------- Helpers ----------
def human(n):
    if n is None:
        return "-"
    try:
        f = float(n)
        return f"{int(f):,}" if f.is_integer() else f"{f:.2f}"
    except:
        return str(n)

def table_from_dict(d: Dict[str, int]) -> str:
    lines = [f"{k}: {human(v)}" for k, v in d.items()]
    return "```" + "\n".join(lines) + "```"

# ---------- Channel Watch ----------
def set_watch(guild_id, channel_id, on):
    conn.execute("""
        INSERT INTO channel_settings (guild_id, channel_id, autocapture)
        VALUES (?, ?, ?)
        ON CONFLICT(guild_id, channel_id)
        DO UPDATE SET autocapture=excluded.autocapture
    """, (str(guild_id), str(channel_id), 1 if on else 0))
    conn.commit()

def get_watch(guild_id, channel_id):
    cur = conn.cursor()
    cur.execute("SELECT autocapture FROM channel_settings WHERE guild_id=? AND channel_id=?",
                (str(guild_id), str(channel_id)))
    row = cur.fetchone()
    return bool(row[0]) if row else False

# ---------- Parsing ----------
FIELD_PAT = re.compile(
    r"(Target|Alliance|Honour|Ranking|Networth|Spies Sent|Spies Lost|Result Level|Number of Castles|Approximate defensive power)",
    re.I
)
NUM_PAT = re.compile(r"[-+]?\d+(?:\.\d+)?")

def parse_spy_report(raw: str) -> Dict[str, Any]:
    data = {
        "kingdom": None,
        "alliance": None,
        "honour": None,
        "ranking": None,
        "networth": None,
        "spies_sent": None,
        "spies_lost": None,
        "result_level": None,
        "castles": None,
        "defense_power": None,
        "captured_at": datetime.now(timezone.utc).isoformat()
    }

    for line in raw.splitlines():
        if "Target:" in line:
            data["kingdom"] = line.split(":", 1)[1].strip()
        if "Alliance:" in line:
            data["alliance"] = line.split(":", 1)[1].strip()
        if "Approximate defensive power" in line:
            nums = NUM_PAT.findall(line.replace(",", ""))
            if nums:
                data["defense_power"] = int(nums[0])
        if "Number of Castles" in line:
            nums = NUM_PAT.findall(line)
            if nums:
                data["castles"] = int(nums[0])

    return data

# ---------- Save ----------
def save_report(author_id, raw):
    data = parse_spy_report(raw)
    if not data["kingdom"]:
        return None, None

    cur = conn.cursor()
    cur.execute("""
        INSERT INTO spy_reports (kingdom, alliance, castles, defense_power, captured_at, author_id, raw)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        data["kingdom"],
        data["alliance"],
        data["castles"],
        data["defense_power"],
        data["captured_at"],
        str(author_id),
        raw
    ))
    conn.commit()
    return cur.lastrowid, data["kingdom"]

# ---------- AUTO CAPTURE (FIXED) ----------
@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    await bot.process_commands(message)

    if not message.guild:
        return

    if not get_watch(message.guild.id, message.channel.id):
        return

    content = message.content
    if "Target:" not in content or "Spy" not in content:
        return

    rid, kingdom = save_report(message.author.id, content)
    if not rid:
        return

    embed = discord.Embed(
        title="🕵️ Spy Report Captured",
        description=f"**Target:** {kingdom}\n**Report ID:** `{rid}`",
        color=THEME_COLOR
    )
    await message.channel.send(embed=embed)

# ---------- Commands ----------
@bot.command()
async def watchhere(ctx, state: str):
    if state.lower() not in ("on", "off"):
        await ctx.send("Usage: `!watchhere on|off`")
        return
    set_watch(ctx.guild.id, ctx.channel.id, state.lower() == "on")
    await ctx.send(f"Auto-capture **{state.upper()}** for this channel.")

@bot.command()
async def spy(ctx, *, kingdom: str):
    cur = conn.cursor()
    cur.execute("""
        SELECT id, defense_power, castles, captured_at
        FROM spy_reports
        WHERE kingdom=?
        ORDER BY captured_at DESC
        LIMIT 1
    """, (kingdom,))
    row = cur.fetchone()
    if not row:
        await ctx.send("No spy reports found.")
        return

    rid, dp, castles, ts = row
    embed = discord.Embed(
        title=f"Spy Report – {kingdom}",
        color=THEME_COLOR
    )
    embed.add_field(name="Defense Power", value=human(dp))
    embed.add_field(name="Castles", value=human(castles))
    embed.set_footer(text=f"Report ID {rid} • {ts}")
    await ctx.send(embed=embed)

# ---------- CALC ----------
TROOP_ATTACK_VALUES = {
    "pikemen":5, "footmen":5, "archers":7,
    "crossbowmen":8, "heavy cavalry":15, "knights":20
}

@bot.command()
async def calc(ctx):
    await ctx.send("Paste the spy report.")

    try:
        msg = await bot.wait_for("message", check=lambda m: m.author == ctx.author, timeout=300)
    except asyncio.TimeoutError:
        return

    spy = parse_spy_report(msg.content)
    if not spy["defense_power"]:
        await ctx.send("Could not parse spy report.")
        return

    defender_dp = spy["defense_power"]
    needed = round((defender_dp * 1.75) / 7)

    await ctx.send(
        f"**Recommended Heavy Cavalry:** `{needed}`\n"
        f"Send troops like:\n`Heavy Cavalry 1000, Archers 500`"
    )

# ---------- RUN ----------
bot.run(TOKEN)
