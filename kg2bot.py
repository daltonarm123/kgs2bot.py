# KG2 Recon Bot (discord.py v2)
# Audited & Hardened Version

import os, re, json, sqlite3, asyncio, hashlib
from math import ceil
from datetime import datetime, timezone
from typing import Dict, Any

import discord
from discord.ext import commands
from dotenv import load_dotenv

# ---------- ENV ----------
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
DB_PATH = os.getenv("DB_PATH", "kg2_reports.sqlite3")

# ---------- DATABASE ----------
conn = sqlite3.connect(
    DB_PATH,
    check_same_thread=False
)
conn.row_factory = sqlite3.Row
conn.execute("PRAGMA journal_mode=WAL;")
conn.execute("PRAGMA foreign_keys=ON;")

conn.executescript("""
CREATE TABLE IF NOT EXISTS spy_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kingdom TEXT NOT NULL,
    alliance TEXT,
    castles INTEGER,
    defense_power INTEGER NOT NULL,
    captured_at TEXT NOT NULL,
    author_id TEXT,
    raw TEXT NOT NULL,
    raw_hash TEXT UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_spy_kingdom_time
ON spy_reports(kingdom, captured_at DESC);

CREATE TABLE IF NOT EXISTS channel_settings (
    guild_id TEXT,
    channel_id TEXT,
    autocapture INTEGER DEFAULT 0,
    PRIMARY KEY (guild_id, channel_id)
);
""")
conn.commit()

# ---------- DISCORD ----------
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)
THEME_COLOR = 0x5865F2

# ---------- HELPERS ----------
def normalize_kingdom(name: str) -> str:
    return name.strip().lower()

def hash_raw(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def human(n):
    if n is None:
        return "-"
    return f"{int(n):,}"

# ---------- WATCH ----------
def set_watch(guild_id, channel_id, on):
    conn.execute("""
        INSERT INTO channel_settings (guild_id, channel_id, autocapture)
        VALUES (?, ?, ?)
        ON CONFLICT(guild_id, channel_id)
        DO UPDATE SET autocapture=excluded.autocapture
    """, (str(guild_id), str(channel_id), 1 if on else 0))
    conn.commit()

def is_watching(guild_id, channel_id) -> bool:
    cur = conn.execute("""
        SELECT autocapture FROM channel_settings
        WHERE guild_id=? AND channel_id=?
    """, (str(guild_id), str(channel_id)))
    row = cur.fetchone()
    return bool(row and row["autocapture"])

# ---------- PARSING ----------
NUM_PAT = re.compile(r"\d+")

def parse_spy_report(raw: str) -> Dict[str, Any]:
    data = {
        "kingdom": None,
        "alliance": None,
        "castles": None,
        "defense_power": None,
        "captured_at": datetime.now(timezone.utc).isoformat()
    }

    for line in raw.splitlines():
        line = line.strip()
        if line.startswith("Target:"):
            data["kingdom"] = line.split(":", 1)[1].strip()
        elif line.startswith("Alliance:"):
            data["alliance"] = line.split(":", 1)[1].strip()
        elif "Number of Castles" in line:
            nums = NUM_PAT.findall(line)
            if nums:
                data["castles"] = int(nums[0])
        elif "Approximate defensive power" in line:
            nums = NUM_PAT.findall(line.replace(",", ""))
            if nums:
                data["defense_power"] = int(nums[0])

    return data

# ---------- SAVE ----------
def save_report(author_id, raw):
    data = parse_spy_report(raw)

    # Hard requirements
    if not data["kingdom"] or not data["defense_power"]:
        return None, None, "invalid"

    kingdom_norm = normalize_kingdom(data["kingdom"])
    raw_h = hash_raw(raw)

    try:
        cur = conn.execute("""
            INSERT INTO spy_reports
            (kingdom, alliance, castles, defense_power, captured_at, author_id, raw, raw_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            kingdom_norm,
            data["alliance"],
            data["castles"],
            data["defense_power"],
            data["captured_at"],
            str(author_id),
            raw,
            raw_h
        ))
        conn.commit()
        return cur.lastrowid, data["kingdom"], "ok"

    except sqlite3.IntegrityError:
        # Duplicate spy report
        return None, data["kingdom"], "duplicate"

# ---------- AUTO CAPTURE ----------
@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    await bot.process_commands(message)

    if not message.guild:
        return

    if not is_watching(message.guild.id, message.channel.id):
        return

    content = message.content
    if "Target:" not in content or "Spy" not in content:
        return

    rid, kingdom_display, status = save_report(message.author.id, content)

    if status == "duplicate":
        return

    if status != "ok":
        return

    embed = discord.Embed(
        title="🕵️ Spy Report Captured",
        description=f"**Target:** {kingdom_display}\n**Report ID:** `{rid}`",
        color=THEME_COLOR
    )
    embed.set_footer(text="KG2 Recon • Auto-captured")
    await message.channel.send(embed=embed)

# ---------- COMMANDS ----------
@bot.command()
async def watchhere(ctx, state: str):
    if state.lower() not in ("on", "off"):
        await ctx.send("Usage: `!watchhere on|off`")
        return

    set_watch(ctx.guild.id, ctx.channel.id, state.lower() == "on")
    await ctx.send(f"Auto-capture **{state.upper()}** for this channel.")

@bot.command()
async def spy(ctx, *, kingdom: str):
    kingdom_norm = normalize_kingdom(kingdom)

    cur = conn.execute("""
        SELECT id, defense_power, castles, captured_at
        FROM spy_reports
        WHERE kingdom=?
        ORDER BY captured_at DESC
        LIMIT 1
    """, (kingdom_norm,))
    row = cur.fetchone()

    if not row:
        await ctx.send("No spy reports found.")
        return

    embed = discord.Embed(
        title=f"Spy Report – {kingdom.title()}",
        color=THEME_COLOR
    )
    embed.add_field(name="Defense Power", value=human(row["defense_power"]))
    embed.add_field(name="Castles", value=human(row["castles"]))
    embed.set_footer(text=f"Report ID {row['id']} • {row['captured_at']}")
    await ctx.send(embed=embed)

# ---------- CALC ----------
@bot.command()
async def calc(ctx):
    await ctx.send("Paste the spy report.")

    try:
        msg = await bot.wait_for(
            "message",
            check=lambda m: m.author == ctx.author and m.channel == ctx.channel,
            timeout=300
        )
    except asyncio.TimeoutError:
        return

    spy = parse_spy_report(msg.content)
    if not spy["defense_power"]:
        await ctx.send("Failed to parse spy report.")
        return

    needed_hc = round((spy["defense_power"] * 1.75) / 7)
    await ctx.send(
        f"**Recommended Heavy Cavalry:** `{needed_hc}`\n"
        f"Example:\n`Heavy Cavalry 1000, Archers 500`"
    )

# ---------- RUN ----------
bot.run(TOKEN)
