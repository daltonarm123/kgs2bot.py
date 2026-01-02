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

AP_MULTIPLIERS = {
    "minor": 1.2,          # ~20%
    "victory": 1.55,       # ~35%
    "major": 2.2,          # ~55%
    "overwhelming": 8.0    # ~87.5%
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

def build_ap_embed(kingdom):
    cur = conn.cursor()
    row = cur.execute(
        "SELECT base_dp, current_dp, hits, last_hit FROM dp_sessions WHERE kingdom=? ORDER BY captured_at DESC LIMIT 1",
        (kingdom,)
    ).fetchone()
    if not row:
        return None

    base, dp, hits, last = row
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

    embed.add_field(
        name="HC Needed (No AP)",
        value=f"{hc_needed(adjusted_dp):,}",
        inline=False
    )

    for k, m in AP_MULTIPLIERS.items():
        remaining = adjusted_dp - int(adjusted_dp / m)
        embed.add_field(
            name=f"After {k.title()}",
            value=f"{hc_needed(remaining):,} HC",
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
            cur = conn.cursor()
            sid, dp = cur.execute(
                "SELECT id, current_dp FROM dp_sessions WHERE kingdom=? ORDER BY captured_at DESC LIMIT 1",
                (self.kingdom,)
            ).fetchone()

            reduction = int(dp / AP_MULTIPLIERS[self.key])
            cur.execute(
                "UPDATE dp_sessions SET current_dp=?, hits=hits+1, last_hit=? WHERE id=?",
                (max(0, dp - reduction), interaction.user.display_name, sid)
            )
            conn.commit()

        await interaction.response.edit_message(embed=build_ap_embed(self.kingdom), view=self.view)

class APResetButton(Button):
    def __init__(self, kingdom):
        super().__init__(label="Reset", style=discord.ButtonStyle.secondary)
        self.kingdom = kingdom

    async def callback(self, interaction):
        async with ap_lock:
            cur = conn.cursor()
            sid, base = cur.execute(
                "SELECT id, base_dp FROM dp_sessions WHERE kingdom=? ORDER BY captured_at DESC LIMIT 1",
                (self.kingdom,)
            ).fetchone()
            cur.execute(
                "UPDATE dp_sessions SET current_dp=?, hits=0, last_hit=NULL WHERE id=?",
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
