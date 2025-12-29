# ---------- KG2 Recon Bot • FULL PATCHED AUTO-WATCH + ERROR LOGS ----------
# Calc (HC only) • AP Planner w/ Buttons + Reset • Spy Capture • Auto-Watch All Channels • Error Logs

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

# ---------- Error Logging ----------
# Store last 20 errors in memory
ERROR_LOGS = []

def log_error(err_text):
    ts = datetime.now(timezone.utc).isoformat()
    ERROR_LOGS.append(f"{ts} • {err_text}")
    # keep only last 20 errors
    if len(ERROR_LOGS) > 20:
        ERROR_LOGS.pop(0)

@bot.event
async def on_command_error(ctx, error):
    log_error(f"Command {ctx.command} failed: {str(error)}")
    await ctx.send("❌ An error occurred. Use `!errorlogs` to see the last errors.")

@bot.command()
async def errorlogs(ctx):
    if not ERROR_LOGS:
        await ctx.send("✅ No recent errors logged.")
        return

    # Send last 10 errors
    last_errors = "\n".join(ERROR_LOGS[-10:])
    # Discord messages have a 2000 char limit
    if len(last_errors) > 1990:
        last_errors = last_errors[-1990:]
    await ctx.send(f"📄 Last errors:\n```\n{last_errors}\n```")

# ---------- Constants ----------
HEAVY_CAVALRY_ATTACK = 15

AP_MULTIPLIERS = {
    "minor": 1.2,
    "victory": 1.55,
    "major": 2.2,
    "overwhelming": 8.0
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
            dp = int(re.search(r"\d+", line.replace(",", "")).group())
        if "number of castles" in line.lower():
            castles = int(re.search(r"\d+", line).group())
    return kingdom, dp, castles

def fuzzy_kingdom(query):
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT kingdom FROM spy_reports")
    names = [r[0] for r in cur.fetchall()]
    match = difflib.get_close_matches(query, names, 1, 0.5)
    return match[0] if match else None

def ensure_ap_session(kingdom):
    cur = conn.cursor()
    exists = cur.execute(
        "SELECT 1 FROM dp_sessions WHERE kingdom=? ORDER BY captured_at DESC LIMIT 1",
        (kingdom,)
    ).fetchone()
    if exists:
        return True
    spy = cur.execute(
        "SELECT defense_power, castles, captured_at FROM spy_reports WHERE kingdom=? ORDER BY captured_at DESC LIMIT 1",
        (kingdom,)
    ).fetchone()
    if not spy:
        return False
    dp, castles, ts = spy
    cur.execute(
        "INSERT INTO dp_sessions VALUES (NULL,?,?,?,?,?,?)",
        (kingdom, dp, castles, dp, 0, None, ts)
    )
    conn.commit()
    return True

# ---------- Auto Capture (AUTO-WATCH ALL CHANNELS) ----------
@bot.event
async def on_message(msg):
    if msg.author.bot or not msg.guild:
        return

    try:
        kingdom, dp, castles = parse_spy(msg.content)
        if kingdom and dp:
            cur = conn.cursor()
            h = hash_report(msg.content)
            if not cur.execute("SELECT 1 FROM spy_reports WHERE report_hash=?", (h,)).fetchone():
                ts = datetime.now(timezone.utc).isoformat()
                cur.execute(
                    "INSERT INTO spy_reports VALUES (NULL,?,?,?,?,?,?)",
                    (kingdom, dp, castles, ts, msg.content, h)
                )
                ensure_ap_session(kingdom)
                conn.commit()
                await msg.channel.send(f"📥 Spy report saved for **{kingdom}**")
    except Exception as e:
        log_error(f"Auto-capture failed: {str(e)}")

    await bot.process_commands(msg)

# ---------- Spy Lookup ----------
@bot.command()
async def spy(ctx, *, kingdom: str):
    try:
        cur = conn.cursor()
        row = cur.execute(
            "SELECT defense_power, castles, captured_at FROM spy_reports WHERE kingdom=? ORDER BY captured_at DESC LIMIT 1",
            (kingdom,)
        ).fetchone()
        if not row:
            kingdom = fuzzy_kingdom(kingdom)
            if not kingdom:
                return await ctx.send("❌ No spy report found.")
            row = cur.execute(
                "SELECT defense_power, castles, captured_at FROM spy_reports WHERE kingdom=? ORDER BY captured_at DESC LIMIT 1",
                (kingdom,)
            ).fetchone()
        base, castles, ts = row
        final_dp = ceil(base * (1 + castle_bonus(castles)))
        embed = discord.Embed(title=f"🕵️ Spy Report • {kingdom}", color=0x5865F2)
        embed.add_field(name="Base DP", value=f"{base:,}")
        embed.add_field(name="With Castles", value=f"{final_dp:,}")
        embed.set_footer(text=f"Captured {ts}")
        await ctx.send(embed=embed)
    except Exception as e:
        log_error(f"!spy command failed: {str(e)}")
        await ctx.send("❌ Failed to process spy command. Use `!errorlogs` for details.")

# ---------- AP Dashboard ----------
def build_ap_embed(kingdom):
    cur = conn.cursor()
    row = cur.execute(
        "SELECT base_dp, current_dp, hits, last_hit FROM dp_sessions WHERE kingdom=? ORDER BY captured_at DESC LIMIT 1",
        (kingdom,)
    ).fetchone()
    if not row:
        return None
    base, dp, hits, last_hit = row
    embed = discord.Embed(title=f"⚔️ AP Planner • {kingdom}", color=0xE74C3C)
    embed.add_field(name="Base DP", value=f"{base:,}")
    embed.add_field(name="Current DP", value=f"{dp:,}")
    embed.add_field(name="Hits Applied", value=str(hits))
    if last_hit:
        embed.set_footer(text=f"Last hit by {last_hit}")
    return embed

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
        self.result = label
        self.kingdom = kingdom

    async def callback(self, interaction: discord.Interaction):
        try:
            cur = conn.cursor()
            sid, dp = cur.execute(
                "SELECT id, current_dp FROM dp_sessions WHERE kingdom=? ORDER BY captured_at DESC LIMIT 1",
                (self.kingdom,)
            ).fetchone()
            reduction = int(dp / AP_MULTIPLIERS[self.result])
            new_dp = max(0, dp - reduction)
            cur.execute(
                "UPDATE dp_sessions SET current_dp=?, hits=hits+1, last_hit=? WHERE id=?",
                (new_dp, interaction.user.display_name, sid)
            )
            conn.commit()
            await interaction.response.edit_message(
                embed=build_ap_embed(self.kingdom),
                view=self.view
            )
        except Exception as e:
            log_error(f"APButton callback failed: {str(e)}")

class APResetButton(Button):
    def __init__(self, kingdom):
        super().__init__(label="Reset", style=discord.ButtonStyle.secondary)
        self.kingdom = kingdom

    async def callback(self, interaction: discord.Interaction):
        try:
            cur = conn.cursor()
            row = cur.execute(
                "SELECT id, base_dp FROM dp_sessions WHERE kingdom=? ORDER BY captured_at DESC LIMIT 1",
                (self.kingdom,)
            ).fetchone()
            if not row:
                return await interaction.response.send_message("❌ No AP session found.", ephemeral=True)
            sid, base_dp = row
            cur.execute(
                "UPDATE dp_sessions SET current_dp=?, hits=0, last_hit=NULL WHERE id=?",
                (base_dp, sid)
            )
            conn.commit()
            await interaction.response.edit_message(
                embed=build_ap_embed(self.kingdom),
                view=self.view
            )
        except Exception as e:
            log_error(f"APResetButton callback failed: {str(e)}")

@bot.command()
async def ap(ctx, *, kingdom: str):
    try:
        real = fuzzy_kingdom(kingdom) or kingdom
        if not ensure_ap_session(real):
            return await ctx.send("❌ No spy report found for that kingdom.")
        embed = build_ap_embed(real)
        if not embed:
            return await ctx.send("❌ Failed to load AP session.")
        await ctx.send(embed=embed, view=APView(real))
    except Exception as e:
        log_error(f"!ap command failed: {str(e)}")

# ---------- Calc (HC ONLY) ----------
@bot.command()
async def calc(ctx):
    try:
        await ctx.send("📄 Paste spy report:")
        spy_msg = await bot.wait_for(
            "message",
            timeout=300,
            check=lambda m: m.author == ctx.author and m.channel == ctx.channel
        )
        await ctx.send("🔍 Processing spy report...")
        kingdom, dp, castles = parse_spy(spy_msg.content)
        if not kingdom or not dp:
            return await ctx.send("❌ Could not parse spy report. Make sure it’s a full KG2 spy report.")

        h = hash_report(spy_msg.content)
        cur = conn.cursor()
        if not cur.execute("SELECT 1 FROM spy_reports WHERE report_hash=?", (h,)).fetchone():
            ts = datetime.now(timezone.utc).isoformat()
            cur.execute(
                "INSERT INTO spy_reports VALUES (NULL,?,?,?,?,?,?)",
                (kingdom, dp, castles, ts, spy_msg.content, h)
            )
            conn.commit()
        ensure_ap_session(kingdom)
        needed_hc = ceil(dp / HEAVY_CAVALRY_ATTACK)
        embed = discord.Embed(
            title="⚔️ Combat Calculator",
            description=f"**Target:** {kingdom}",
            color=0x5865F2
        )
        embed.add_field(name="Defense Power", value=f"{dp:,}", inline=False)
        embed.add_field(
            name="Suggested Hit (HC ONLY)",
            value=f"🟦 Heavy Cavalry × **{needed_hc:,}**",
            inline=False
        )
        embed.set_footer(text="AP session created / updated")
        await ctx.send(embed=embed)
    except Exception as e:
        log_error(f"!calc command failed: {str(e)}")
        await ctx.send("❌ Failed to process calc command. Use `!errorlogs` for details.")

# ---------- Help ----------
@bot.command(name="kg2help")
async def kg2help(ctx):
    await ctx.send(
        "**KG2 Recon Commands**\n"
        "`!spy <kingdom>`\n"
        "`!calc` (HC only)\n"
        "`!ap <kingdom>` (buttons + reset)\n"
        "`!errorlogs` (show last errors)"
    )

# ---------- Run ----------
bot.run(TOKEN)
