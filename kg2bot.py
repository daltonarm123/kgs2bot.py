# ---------- KG2 Recon Bot • FULL FEATURE BUILD ----------
# Interactive Calc • AP Hit Tracking • Cogs • Slash Commands

import os, re, json, sqlite3, asyncio, difflib, hashlib, logging
from math import ceil
from datetime import datetime, timezone
import discord
from discord.ext import commands
from dotenv import load_dotenv

# ---------- Setup ----------
load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
DB_PATH = "kg2_reports.sqlite3"
logging.basicConfig(level=logging.INFO)

# ---------- DB ----------
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

CREATE TABLE IF NOT EXISTS channel_settings (
    guild_id TEXT,
    channel_id TEXT,
    autocapture INTEGER,
    PRIMARY KEY (guild_id, channel_id)
);

CREATE TABLE IF NOT EXISTS dp_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kingdom TEXT,
    base_dp INTEGER,
    castles INTEGER,
    current_dp INTEGER,
    hits INTEGER,
    captured_at TEXT
);
""")
conn.commit()

# ---------- Discord ----------
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# ---------- Constants ----------
TROOP_ATTACK = {
    "heavy cavalry": 15,
    "knights": 20,
    "archers": 7,
    "pikemen": 5
}

AP_MULTIPLIERS = {
    "minor": 1.2,
    "victory": 1.55,
    "major": 2.2,
    "overwhelming": 8.0
}

# ---------- Helpers ----------
def castle_bonus(c):
    return (c ** 0.5) / 100 if c else 0

def hash_report(t):
    return hashlib.sha256(t.encode()).hexdigest()

def parse_spy(text):
    dp = None
    castles = 0
    kingdom = None
    for line in text.splitlines():
        if "Target" in line:
            kingdom = line.split(":")[1].strip()
        if "defensive power" in line.lower():
            dp = int(re.search(r"\d+", line.replace(",", "")).group())
        if "Number of Castles" in line:
            castles = int(re.search(r"\d+", line).group())
    return kingdom, dp, castles

def fuzzy_kingdom(q):
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT kingdom FROM spy_reports")
    names = [r[0] for r in cur.fetchall()]
    m = difflib.get_close_matches(q, names, 1, 0.5)
    return m[0] if m else None

# ---------- Events ----------
@bot.event
async def on_message(msg):
    if msg.author.bot or not msg.guild:
        return

    cur = conn.cursor()
    row = cur.execute(
        "SELECT autocapture FROM channel_settings WHERE guild_id=? AND channel_id=?",
        (str(msg.guild.id), str(msg.channel.id))
    ).fetchone()

    if row and row[0]:
        kingdom, dp, castles = parse_spy(msg.content)
        if kingdom and dp:
            h = hash_report(msg.content)
            if not cur.execute("SELECT 1 FROM spy_reports WHERE report_hash=?", (h,)).fetchone():
                ts = datetime.now(timezone.utc).isoformat()
                cur.execute(
                    "INSERT INTO spy_reports VALUES (NULL,?,?,?,?,?,?)",
                    (kingdom, dp, castles, ts, msg.content, h)
                )
                cur.execute(
                    "INSERT INTO dp_sessions VALUES (NULL,?,?,?,?,?,?)",
                    (kingdom, dp, castles, dp, 0, ts)
                )
                conn.commit()
                await msg.channel.send(f"📥 Spy report saved for **{kingdom}**")

    await bot.process_commands(msg)

# ---------- Watch ----------
@bot.command()
async def watchhere(ctx, mode: str):
    conn.execute(
        "INSERT OR REPLACE INTO channel_settings VALUES (?,?,?)",
        (str(ctx.guild.id), str(ctx.channel.id), 1 if mode=="on" else 0)
    )
    conn.commit()
    await ctx.send("📡 Watching this channel." if mode=="on" else "🛑 Stopped watching.")

@bot.command()
async def watchall(ctx, mode: str):
    for ch in ctx.guild.text_channels:
        conn.execute(
            "INSERT OR REPLACE INTO channel_settings VALUES (?,?,?)",
            (str(ctx.guild.id), str(ch.id), 1 if mode=="on" else 0)
        )
    conn.commit()
    await ctx.send("📡 Watching all channels." if mode=="on" else "🛑 Stopped watching all channels.")

# ---------- Spy ----------
@bot.hybrid_command()
async def spy(ctx, *, kingdom: str):
    cur = conn.cursor()
    row = cur.execute(
        "SELECT defense_power, castles, captured_at FROM spy_reports WHERE kingdom=? ORDER BY captured_at DESC LIMIT 1",
        (kingdom,)
    ).fetchone()

    if not row:
        kingdom = fuzzy_kingdom(kingdom)
        if not kingdom:
            return await ctx.send("❌ No spy reports found.")

        row = cur.execute(
            "SELECT defense_power, castles, captured_at FROM spy_reports WHERE kingdom=? ORDER BY captured_at DESC LIMIT 1",
            (kingdom,)
        ).fetchone()

    base, castles, ts = row
    with_castles = ceil(base * (1 + castle_bonus(castles)))

    embed = discord.Embed(title=f"🕵️ Spy Report • {kingdom}", color=0x5865F2)
    embed.add_field(name="Base DP", value=f"{base:,}")
    embed.add_field(name="With Castles", value=f"{with_castles:,}")
    embed.set_footer(text=f"Captured {ts}")

    await ctx.send(embed=embed)

# ---------- AP ----------
@bot.command()
async def ap(ctx, action: str, kingdom: str = None, result: str = None):
    cur = conn.cursor()

    if action == "hit":
        if result not in AP_MULTIPLIERS:
            return await ctx.send("Use: minor | victory | major | overwhelming")

        row = cur.execute(
            "SELECT id, current_dp FROM dp_sessions WHERE kingdom=? ORDER BY captured_at DESC LIMIT 1",
            (kingdom,)
        ).fetchone()

        if not row:
            return await ctx.send("❌ No AP session.")

        sid, dp = row
        reduction = int(dp / AP_MULTIPLIERS[result])
        new_dp = max(0, dp - reduction)

        cur.execute(
            "UPDATE dp_sessions SET current_dp=?, hits=hits+1 WHERE id=?",
            (new_dp, sid)
        )
        conn.commit()

        return await ctx.send(
            f"⚔️ **{result.title()} hit applied**\nDP: {dp:,} → {new_dp:,}"
        )

    # view
    row = cur.execute(
        "SELECT base_dp, current_dp, hits FROM dp_sessions WHERE kingdom=? ORDER BY captured_at DESC LIMIT 1",
        (action,)
    ).fetchone()

    if not row:
        return await ctx.send("❌ No AP session.")

    await ctx.send(
        f"📊 **AP Planner • {action}**\n"
        f"Base DP: {row[0]:,}\n"
        f"Current DP: {row[1]:,}\n"
        f"Hits Applied: {row[2]}"
    )

# ---------- CALC ----------
@bot.command()
async def calc(ctx):
    await ctx.send("📄 Paste spy report:")
    spy = await bot.wait_for("message", check=lambda m: m.author==ctx.author)

    kingdom, dp, castles = parse_spy(spy.content)
    if not dp:
        return await ctx.send("❌ Could not parse DP.")

    best = max(TROOP_ATTACK.items(), key=lambda x: x[1])
    needed = ceil(dp / best[1])

    embed = discord.Embed(title="⚔️ Combat Calculator", color=0x5865F2)
    embed.add_field(name="Target", value=kingdom)
    embed.add_field(name="Defense Power", value=f"{dp:,}")
    embed.add_field(name="Suggested Troop", value=f"{best[0].title()} × {needed}")

    await ctx.send(embed=embed)
    await ctx.send("📦 Send troops as `troop=count` (comma separated):")

    msg = await bot.wait_for("message", check=lambda m: m.author==ctx.author)
    troops = {}
    for part in msg.content.split(","):
        t,c = part.split("=")
        troops[t.strip().lower()] = int(c)

    attack = sum(TROOP_ATTACK.get(t,0)*c for t,c in troops.items())
    ratio = attack / dp
    outcome = "💀 Loss" if ratio < 1 else "⚔️ Win" if ratio < 2 else "🏆 Overkill"

    embed.add_field(name="Your Attack", value=f"{attack:,}")
    embed.add_field(name="Outcome", value=outcome)
    await ctx.send(embed=embed)

# ---------- Help ----------
@bot.command(name="kg2help")
@bot.command(name="commands")
async def help_cmd(ctx):
    await ctx.send(
        "**KG2 Recon Commands**\n"
        "`!watchhere on/off`\n"
        "`!watchall on/off`\n"
        "`!spy <kingdom>`\n"
        "`!ap <kingdom>`\n"
        "`!ap hit <kingdom> <minor|victory|major|overwhelming>`\n"
        "`!calc`"
    )

# ---------- Run ----------
bot.run(TOKEN)
