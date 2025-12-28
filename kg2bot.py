# ---------- KG2 Recon Bot (FULL ENHANCED VERSION) ----------
# Discord.py v2 • Auto-capture • Deduplication • Fuzzy matching
# AP Planner • Troop Calc • Spy History • Auto-watch channels

import os, re, json, sqlite3, asyncio, difflib, hashlib, logging
from math import ceil
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple
import discord
from discord.ext import commands
from dotenv import load_dotenv

# ---------- Setup ----------
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
DB_PATH = os.getenv("DB_PATH", "kg2_reports.sqlite3")

logging.basicConfig(level=logging.INFO)

# ---------- Database ----------
conn = sqlite3.connect(DB_PATH)
conn.execute("PRAGMA journal_mode=WAL;")
conn.execute("PRAGMA foreign_keys=ON;")

conn.executescript("""
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
    defense_power INTEGER,
    captured_at TEXT,
    author_id TEXT,
    raw TEXT,
    report_hash TEXT UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_spy_kingdom_time
ON spy_reports(kingdom, captured_at DESC);

CREATE TABLE IF NOT EXISTS channel_settings (
    guild_id TEXT,
    channel_id TEXT,
    autocapture INTEGER DEFAULT 0,
    PRIMARY KEY (guild_id, channel_id)
);

CREATE TABLE IF NOT EXISTS dp_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    target TEXT,
    spy_report_id INTEGER,
    base_dp INTEGER,
    castles INTEGER,
    with_castles INTEGER,
    captured_at TEXT
);
""")
conn.commit()

# ---------- Discord ----------
intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
bot = commands.Bot(command_prefix="!", intents=intents)

# ---------- Constants ----------
THEME_COLOR = 0x5865F2

TROOP_ATTACK_VALUES = {
    "pikemen": 5,
    "footmen": 5,
    "archers": 7,
    "crossbowmen": 8,
    "heavy cavalry": 15,
    "knights": 20
}

TROOP_EFFICIENCY = {
    "heavy cavalry": 15,
    "knights": 20,
    "archers": 7,
    "pikemen": 5
}

# ---------- Helpers ----------
def hash_report(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()

def castle_bonus(castles: int) -> float:
    return (castles ** 0.5) / 100 if castles else 0

def human(n):
    return f"{int(n):,}" if n is not None else "-"

def fuzzy_kingdom(name: str) -> Optional[str]:
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT kingdom FROM spy_reports")
    kingdoms = [r[0] for r in cur.fetchall()]
    match = difflib.get_close_matches(name, kingdoms, n=1, cutoff=0.5)
    return match[0] if match else None

# ---------- Spy Parsing ----------
FIELD_PAT = re.compile(r"^(Target|Alliance|Honour|Ranking|Networth|Spies Sent|Spies Lost|Result Level|Number of Castles|Approximate defensive power).*?:\s*(.+)$", re.I)
NUM_PAT = re.compile(r"\d+")

def parse_spy(raw: str) -> Dict[str, Any]:
    data = {
        "kingdom": None, "alliance": None, "honour": None,
        "ranking": None, "networth": None, "spies_sent": None,
        "spies_lost": None, "result_level": None,
        "castles": None, "defense_power": None
    }
    for line in raw.splitlines():
        m = FIELD_PAT.match(line.strip())
        if not m: continue
        key, val = m.group(1).lower(), m.group(2)
        nums = NUM_PAT.findall(val.replace(",", ""))
        if key.startswith("target"): data["kingdom"] = val.strip()
        elif key.startswith("alliance"): data["alliance"] = val.strip()
        elif nums:
            data[key.replace(" ", "_")] = int(nums[0])
    return data

def save_spy(author_id: int, raw: str):
    data = parse_spy(raw)
    if not data["kingdom"]: return None

    h = hash_report(raw)
    cur = conn.cursor()
    if cur.execute("SELECT 1 FROM spy_reports WHERE report_hash=?", (h,)).fetchone():
        return None

    ts = datetime.now(timezone.utc).isoformat()
    cur.execute("""
        INSERT INTO spy_reports
        (kingdom, alliance, honour, ranking, networth,
         spies_sent, spies_lost, result_level,
         castles, defense_power, captured_at, author_id, raw, report_hash)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        data["kingdom"], data["alliance"], data["honour"],
        data["ranking"], data["networth"],
        data["spies_sent"], data["spies_lost"],
        data["result_level"], data["castles"],
        data["defense_power"], ts, str(author_id), raw, h
    ))
    rid = cur.lastrowid

    if data["defense_power"]:
        with_castles = ceil(data["defense_power"] * (1 + castle_bonus(data["castles"] or 0)))
        cur.execute("""
            INSERT INTO dp_sessions
            (target, spy_report_id, base_dp, castles, with_castles, captured_at)
            VALUES (?,?,?,?,?,?)
        """, (data["kingdom"], rid, data["defense_power"], data["castles"], with_castles, ts))

    conn.commit()
    return data["kingdom"]

# ---------- Events ----------
@bot.event
async def on_message(message):
    if message.author.bot or not message.guild:
        return

    cur = conn.cursor()
    row = cur.execute(
        "SELECT autocapture FROM channel_settings WHERE guild_id=? AND channel_id=?",
        (str(message.guild.id), str(message.channel.id))
    ).fetchone()

    if row and row[0]:
        kingdom = save_spy(message.author.id, message.content)
        if kingdom:
            await message.channel.send(f"📥 Spy report saved for **{kingdom}**")

    await bot.process_commands(message)

@bot.event
async def on_guild_channel_create(channel):
    if not isinstance(channel, discord.TextChannel): return
    cur = conn.cursor()
    if cur.execute("SELECT 1 FROM channel_settings WHERE guild_id=? AND autocapture=1", (str(channel.guild.id),)).fetchone():
        cur.execute("INSERT OR IGNORE INTO channel_settings VALUES (?,?,1)", (str(channel.guild.id), str(channel.id)))
        conn.commit()
        await channel.send("📡 Auto-watching this new channel.")

# ---------- Commands ----------
@bot.command(name="kg2help")
@bot.command(name="commands")
async def help_cmd(ctx):
    embed = discord.Embed(title="KG2 Recon Commands", color=THEME_COLOR)
    embed.add_field(name="!watchhere on/off", value="Watch this channel", inline=False)
    embed.add_field(name="!watchall on/off", value="Watch all channels", inline=False)
    embed.add_field(name="!watchstatus", value="Show watch status", inline=False)
    embed.add_field(name="!spy <kingdom>", value="Show latest spy report", inline=False)
    embed.add_field(name="!spyhistory <kingdom>", value="Show past reports", inline=False)
    embed.add_field(name="!ap <kingdom>", value="AP planner", inline=False)
    embed.add_field(name="!calc", value="Combat calculator", inline=False)
    await ctx.send(embed=embed)

@bot.command()
async def watchhere(ctx, mode: str):
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO channel_settings VALUES (?,?,?)",
                (str(ctx.guild.id), str(ctx.channel.id), 1 if mode=="on" else 0))
    conn.commit()
    await ctx.send("📡 Watching this channel." if mode=="on" else "🛑 Stopped watching this channel.")

@bot.command()
async def watchall(ctx, mode: str):
    for ch in ctx.guild.text_channels:
        conn.execute("INSERT OR REPLACE INTO channel_settings VALUES (?,?,?)",
                     (str(ctx.guild.id), str(ch.id), 1 if mode=="on" else 0))
    conn.commit()
    await ctx.send("📡 Watching all channels." if mode=="on" else "🛑 Stopped watching all channels.")

@bot.command()
async def watchstatus(ctx):
    cur = conn.cursor()
    rows = cur.execute("SELECT channel_id, autocapture FROM channel_settings WHERE guild_id=?", (str(ctx.guild.id),)).fetchall()
    msg = "\n".join([f"<#{r[0]}> → {'ON' if r[1] else 'OFF'}" for r in rows])
    await ctx.send(f"```{msg}```")

@bot.command()
async def spy(ctx, *, kingdom: str):
    cur = conn.cursor()
    row = cur.execute(
        "SELECT kingdom, defense_power, castles, captured_at FROM spy_reports WHERE kingdom=? ORDER BY captured_at DESC LIMIT 1",
        (kingdom,)
    ).fetchone()

    if not row:
        match = fuzzy_kingdom(kingdom)
        if not match:
            return await ctx.send("❌ No spy reports found.")
        kingdom = match
        row = cur.execute(
            "SELECT kingdom, defense_power, castles, captured_at FROM spy_reports WHERE kingdom=? ORDER BY captured_at DESC LIMIT 1",
            (kingdom,)
        ).fetchone()

    base, castles = row[1], row[2]
    with_castles = ceil(base * (1 + castle_bonus(castles)))
    await ctx.send(
        f"🕵️ **Last Spy Report for {kingdom}**\n"
        f"🛡️ Base DP: {human(base)}\n"
        f"🏰 With Castles: {human(with_castles)}\n"
        f"📅 {row[3]}"
    )

@bot.command()
async def spyhistory(ctx, kingdom: str):
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT captured_at, defense_power FROM spy_reports WHERE kingdom=? ORDER BY captured_at DESC LIMIT 5",
        (kingdom,)
    ).fetchall()
    if not rows:
        return await ctx.send("❌ No history found.")
    msg = "\n".join([f"{r[0]} → DP {human(r[1])}" for r in rows])
    await ctx.send(f"📜 **Spy History for {kingdom}**\n```{msg}```")

@bot.command()
async def ap(ctx, kingdom: str):
    cur = conn.cursor()
    row = cur.execute(
        "SELECT base_dp, castles, with_castles FROM dp_sessions WHERE target=? ORDER BY captured_at DESC LIMIT 1",
        (kingdom,)
    ).fetchone()
    if not row:
        return await ctx.send("❌ No AP session found.")
    await ctx.send(
        f"📊 **AP Planner • {kingdom}**\n"
        f"Base DP: {human(row[0])}\n"
        f"With Castles: {human(row[2])}"
    )

@bot.command()
async def calc(ctx):
    await ctx.send("📄 Paste spy report:")
    spy_msg = await bot.wait_for("message", check=lambda m: m.author==ctx.author, timeout=300)
    data = parse_spy(spy_msg.content)
    dp = data.get("defense_power")
    if not dp:
        return await ctx.send("❌ Failed to parse DP.")

    best = max(TROOP_EFFICIENCY.items(), key=lambda x: x[1])
    needed = ceil(dp / best[1])

    await ctx.send(
        f"🧠 **Optimal Troop:** {best[0].title()}\n"
        f"⚔️ Needed to match DP: {needed}"
    )

    await ctx.send("📦 Send your troops as JSON:")
    tmsg = await bot.wait_for("message", check=lambda m: m.author==ctx.author, timeout=300)
    troops = json.loads(tmsg.content)

    total = sum(TROOP_ATTACK_VALUES.get(k.lower(),0)*v for k,v in troops.items())
    ratio = total/dp
    outcome = "💀 Loss" if ratio<1 else "⚔️ Win" if ratio<2 else "🏆 Overkill"
    await ctx.send(f"Attack: {human(total)} vs DP {human(dp)} → {outcome}")

# ---------- Run ----------
bot.run(TOKEN)
