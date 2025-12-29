# ---------- KG2 Recon Bot • FULL PATCHED BUILD w/ ERROR LOGGING & STARTUP CONFIRM ----------
# Calc (HC only) • AP Planner w/ Buttons + Reset • Spy Capture • Auto-Watch • Error Logs to Discord

import os, re, sqlite3, asyncio, difflib, hashlib, logging, traceback
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
ERROR_LOG = "kg2_errors.log"
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

# ---------- Constants ----------
HEAVY_CAVALRY_ATTACK = 15
AP_MULTIPLIERS = {"minor":1.2,"victory":1.55,"major":2.2,"overwhelming":8.0}

# ---------- Helpers ----------
def castle_bonus(c): return (c ** 0.5) / 100 if c else 0
def hash_report(text): return hashlib.sha256(text.encode()).hexdigest()

def parse_spy(text):
    kingdom, dp, castles = None, None, 0
    for line in text.splitlines():
        if line.lower().startswith("target:"): kingdom = line.split(":",1)[1].strip()
        if "defensive power" in line.lower(): dp = int(re.search(r"\d+", line.replace(",","")).group())
        if "number of castles" in line.lower(): castles = int(re.search(r"\d+", line).group())
    return kingdom, dp, castles

def fuzzy_kingdom(query):
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT kingdom FROM spy_reports")
    names = [r[0] for r in cur.fetchall()]
    match = difflib.get_close_matches(query, names, 1, 0.5)
    return match[0] if match else None

def ensure_ap_session(kingdom):
    cur = conn.cursor()
    exists = cur.execute("SELECT 1 FROM dp_sessions WHERE kingdom=? ORDER BY captured_at DESC LIMIT 1",(kingdom,)).fetchone()
    if exists: return True
    spy = cur.execute("SELECT defense_power, castles, captured_at FROM spy_reports WHERE kingdom=? ORDER BY captured_at DESC LIMIT 1",(kingdom,)).fetchone()
    if not spy: return False
    dp, castles, ts = spy
    cur.execute("INSERT INTO dp_sessions VALUES (NULL,?,?,?,?,?,?)",(kingdom, dp, castles, dp, 0, None, ts))
    conn.commit()
    return True

async def log_error(ctx, error):
    ts = datetime.now(timezone.utc).isoformat()
    err_text = f"\n[{ts}] ERROR in {ctx}:\n{traceback.format_exc()}\n"

    # Save to file
    with open(ERROR_LOG,"a",encoding="utf-8") as f:
        f.write(err_text)

    # Send to Discord channel if exists
    await bot.wait_until_ready()
    for guild in bot.guilds:
        channel = discord.utils.get(guild.text_channels, name=ERROR_CHANNEL_NAME)
        if channel:
            chunks = [err_text[i:i+1900] for i in range(0, len(err_text), 1900)]
            for chunk in chunks:
                try: await channel.send(f"```py\n{chunk}\n```")
                except: pass
            break

# ---------- Auto Capture ----------
@bot.event
async def on_message(msg):
    try:
        if msg.author.bot or not msg.guild: return
        cur = conn.cursor()
        kingdom, dp, castles = parse_spy(msg.content)
        if kingdom and dp:
            h = hash_report(msg.content)
            if not cur.execute("SELECT 1 FROM spy_reports WHERE report_hash=?", (h,)).fetchone():
                ts = datetime.now(timezone.utc).isoformat()
                cur.execute("INSERT INTO spy_reports VALUES (NULL,?,?,?,?,?,?)",(kingdom, dp, castles, ts, msg.content, h))
                ensure_ap_session(kingdom)
                conn.commit()
                await msg.channel.send(f"📥 Spy report saved for **{kingdom}**")
    except Exception as e:
        await log_error("on_message", e)
    await bot.process_commands(msg)

# ---------- AP Dashboard ----------
def build_ap_embed(kingdom):
    cur = conn.cursor()
    row = cur.execute("SELECT base_dp, current_dp, hits, last_hit FROM dp_sessions WHERE kingdom=? ORDER BY captured_at DESC LIMIT 1",(kingdom,)).fetchone()
    if not row: return None
    base, dp, hits, last_hit = row
    embed = discord.Embed(title=f"⚔️ AP Planner • {kingdom}", color=0xE74C3C)
    embed.add_field(name="Base DP", value=f"{base:,}")
    embed.add_field(name="Current DP", value=f"{dp:,}")
    embed.add_field(name="Hits Applied", value=str(hits))
    if last_hit: embed.set_footer(text=f"Last hit by {last_hit}")
    return embed

class APView(View):
    def __init__(self, kingdom):
        super().__init__(timeout=None)
        self.kingdom = kingdom
        for k in AP_MULTIPLIERS: self.add_item(APButton(k, kingdom))
        self.add_item(APResetButton(kingdom))

class APButton(Button):
    def __init__(self, label, kingdom):
        super().__init__(label=label.title(), style=discord.ButtonStyle.danger)
        self.result = label
        self.kingdom = kingdom
    async def callback(self, interaction: discord.Interaction):
        try:
            cur = conn.cursor()
            sid, dp = cur.execute("SELECT id, current_dp FROM dp_sessions WHERE kingdom=? ORDER BY captured_at DESC LIMIT 1",(self.kingdom,)).fetchone()
            reduction = int(dp / AP_MULTIPLIERS[self.result])
            new_dp = max(0, dp - reduction)
            cur.execute("UPDATE dp_sessions SET current_dp=?, hits=hits+1, last_hit=? WHERE id=?",(new_dp, interaction.user.display_name, sid))
            conn.commit()
            await interaction.response.edit_message(embed=build_ap_embed(self.kingdom), view=self.view)
        except Exception as e:
            await log_error(f"APButton({self.result})", e)
            await interaction.response.send_message("❌ An error occurred.", ephemeral=True)

class APResetButton(Button):
    def __init__(self, kingdom):
        super().__init__(label="Reset", style=discord.ButtonStyle.secondary)
        self.kingdom = kingdom
    async def callback(self, interaction: discord.Interaction):
        try:
            cur = conn.cursor()
            row = cur.execute("SELECT id, base_dp FROM dp_sessions WHERE kingdom=? ORDER BY captured_at DESC LIMIT 1",(self.kingdom,)).fetchone()
            if not row: return await interaction.response.send_message("❌ No AP session found.", ephemeral=True)
            sid, base_dp = row
            cur.execute("UPDATE dp_sessions SET current_dp=?, hits=0, last_hit=NULL WHERE id=?",(base_dp, sid))
            conn.commit()
            await interaction.response.edit_message(embed=build_ap_embed(self.kingdom), view=self.view)
        except Exception as e:
            await log_error("APResetButton", e)
            await interaction.response.send_message("❌ An error occurred.", ephemeral=True)

# ---------- Commands ----------
@bot.command()
async def ap(ctx, *, kingdom: str):
    try:
        real = fuzzy_kingdom(kingdom) or kingdom
        if not ensure_ap_session(real): return await ctx.send("❌ No spy report found for that kingdom.")
        embed = build_ap_embed(real)
        if not embed: return await ctx.send("❌ Failed to load AP session.")
        await ctx.send(embed=embed, view=APView(real))
    except Exception as e: await log_error("ap command", e)

@bot.command()
async def calc(ctx):
    try:
        await ctx.send("📄 Paste spy report:")
        spy_msg = await bot.wait_for("message", timeout=300, check=lambda m: m.author==ctx.author and m.channel==ctx.channel)
        await ctx.send("🔍 Processing spy report...")
        kingdom, dp, castles = parse_spy(spy_msg.content)
        if not kingdom or not dp: return await ctx.send("❌ Could not parse spy report. Make sure it’s a full KG2 spy report.")
        h = hash_report(spy_msg.content)
        cur = conn.cursor()
        if not cur.execute("SELECT 1 FROM spy_reports WHERE report_hash=?", (h,)).fetchone():
            ts = datetime.now(timezone.utc).isoformat()
            cur.execute("INSERT INTO spy_reports VALUES (NULL,?,?,?,?,?,?)",(kingdom, dp, castles, ts, spy_msg.content, h))
            conn.commit()
        ensure_ap_session(kingdom)
        needed_hc = ceil(dp / HEAVY_CAVALRY_ATTACK)
        embed = discord.Embed(title="⚔️ Combat Calculator", description=f"**Target:** {kingdom}", color=0x5865F2)
        embed.add_field(name="Defense Power", value=f"{dp:,}", inline=False)
        embed.add_field(name="Suggested Hit (HC ONLY)", value=f"🟦 Heavy Cavalry × **{needed_hc:,}**", inline=False)
        embed.set_footer(text="AP session created / updated")
        await ctx.send(embed=embed)
    except Exception as e:
        await log_error("calc command", e)
        await ctx.send("❌ An error occurred while processing the spy report.")

@bot.command()
async def spy(ctx, *, kingdom: str):
    try:
        cur = conn.cursor()
        row = cur.execute("SELECT defense_power, castles, captured_at FROM spy_reports WHERE kingdom=? ORDER BY captured_at DESC LIMIT 1",(kingdom,)).fetchone()
        if not row:
            kingdom = fuzzy_kingdom(kingdom)
            if not kingdom: return await ctx.send("❌ No spy report found.")
            row = cur.execute("SELECT defense_power, castles, captured_at FROM spy_reports WHERE kingdom=? ORDER BY captured_at DESC LIMIT 1",(kingdom,)).fetchone()
        base, castles, ts = row
        final_dp = ceil(base * (1 + castle_bonus(castles)))
        embed = discord.Embed(title=f"🕵️ Spy Report • {kingdom}", color=0x5865F2)
        embed.add_field(name="Base DP", value=f"{base:,}")
        embed.add_field(name="With Castles", value=f"{final_dp:,}")
        embed.set_footer(text=f"Captured {ts}")
        await ctx.send(embed=embed)
    except Exception as e:
        await log_error("spy command", e)
        await ctx.send("❌ An error occurred while fetching the spy report.")

@bot.command(name="kg2help")
async def kg2help(ctx):
    try:
        await ctx.send(
            "**KG2 Recon Commands**\n"
            "`!spy <kingdom>`\n"
            "`!calc` (HC only)\n"
            "`!ap <kingdom>` (buttons + reset)"
        )
    except Exception as e:
        await log_error("kg2help command", e)

# ---------- Startup Event ----------
@bot.event
async def on_ready():
    await bot.wait_until_ready()
    total_channels = sum(len(guild.text_channels) for guild in bot.guilds)
    for guild in bot.guilds:
        channel = discord.utils.get(guild.text_channels, name=ERROR_CHANNEL_NAME)
        if channel:
            await channel.send(f"✅ KG2 Recon Bot is online and actively watching **{total_channels} channels** across {len(bot.guilds)} servers.")

# ---------- Run ----------
bot.run(TOKEN)
