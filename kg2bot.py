import discord
from discord.ext import commands
import sqlite3
import re
from datetime import datetime

# ------------------ CONFIG ------------------

TOKEN = "YOUR_BOT_TOKEN_HERE"
DB_FILE = "kg2recon.db"
COMMAND_PREFIX = "!"

intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
intents.messages = True

bot = commands.Bot(command_prefix=COMMAND_PREFIX, intents=intents)

# ------------------ DATABASE ------------------

def get_db():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def init_db():
    with get_db() as db:
        db.execute("""
        CREATE TABLE IF NOT EXISTS spy_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER NOT NULL,
            kingdom TEXT NOT NULL,
            defense_power INTEGER,
            castles INTEGER,
            captured_at TEXT NOT NULL
        )
        """)

        db.execute("""
        CREATE TABLE IF NOT EXISTS channel_settings (
            guild_id INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            watch INTEGER NOT NULL,
            PRIMARY KEY (guild_id, channel_id)
        )
        """)

        db.execute("""
        CREATE INDEX IF NOT EXISTS idx_spy_kingdom
        ON spy_reports (guild_id, kingdom)
        """)


def set_watch(guild_id, channel_id, watch: bool):
    with get_db() as db:
        db.execute("""
        INSERT INTO channel_settings (guild_id, channel_id, watch)
        VALUES (?, ?, ?)
        ON CONFLICT(guild_id, channel_id)
        DO UPDATE SET watch=excluded.watch
        """, (guild_id, channel_id, int(watch)))


def is_watching(guild_id, channel_id) -> bool:
    with get_db() as db:
        row = db.execute("""
        SELECT watch FROM channel_settings
        WHERE guild_id=? AND channel_id=?
        """, (guild_id, channel_id)).fetchone()
        return bool(row["watch"]) if row else False


def save_report(guild_id, kingdom, defense_power, castles):
    kingdom = kingdom.strip().lower()

    with get_db() as db:
        db.execute("""
        INSERT INTO spy_reports
        (guild_id, kingdom, defense_power, castles, captured_at)
        VALUES (?, ?, ?, ?, ?)
        """, (
            guild_id,
            kingdom,
            defense_power,
            castles,
            datetime.utcnow().isoformat()
        ))


def get_latest_report(guild_id, kingdom):
    kingdom = kingdom.strip().lower()

    with get_db() as db:
        return db.execute("""
        SELECT *
        FROM spy_reports
        WHERE guild_id=? AND kingdom=?
        ORDER BY captured_at DESC
        LIMIT 1
        """, (guild_id, kingdom)).fetchone()


# ------------------ SPY PARSER ------------------

SPY_REGEX = re.compile(
    r"Target:\s*(?P<kingdom>.+?)\n.*?"
    r"Defense Power:\s*(?P<dp>[\d,]+).*?"
    r"Castles:\s*(?P<castles>\d+)",
    re.S | re.I
)


def parse_spy_report(content: str):
    match = SPY_REGEX.search(content)
    if not match:
        return None

    return {
        "kingdom": match.group("kingdom"),
        "defense_power": int(match.group("dp").replace(",", "")),
        "castles": int(match.group("castles"))
    }

# ------------------ EVENTS ------------------

@bot.event
async def on_ready():
    init_db()
    print(f"✅ Logged in as {bot.user}")


@bot.event
async def on_message(message):
    if message.author.bot or not message.guild:
        return

    if not is_watching(message.guild.id, message.channel.id):
        await bot.process_commands(message)
        return

    data = parse_spy_report(message.content)
    if data:
        save_report(
            message.guild.id,
            data["kingdom"],
            data["defense_power"],
            data["castles"]
        )

        await message.channel.send(
            f"🕵️ **Spy Report Captured**\n"
            f"Target: **{data['kingdom'].title()}**\n"
            f"Defense Power: **{data['defense_power']:,}**\n"
            f"Castles: **{data['castles']}**"
        )

    await bot.process_commands(message)

# ------------------ COMMANDS ------------------

@bot.command()
@commands.has_permissions(manage_channels=True)
async def watchhere(ctx, state: str):
    state = state.lower()
    if state not in ("on", "off"):
        await ctx.send("Usage: `!watchhere on|off`")
        return

    set_watch(ctx.guild.id, ctx.channel.id, state == "on")
    await ctx.send(
        f"Auto-capture **{state.upper()}** for this channel."
    )


@bot.command()
@commands.has_permissions(manage_guild=True)
async def watchall(ctx, state: str):
    state = state.lower()
    if state not in ("on", "off"):
        await ctx.send("Usage: `!watchall on|off`")
        return

    on = state == "on"
    count = 0

    for channel in ctx.guild.text_channels:
        set_watch(ctx.guild.id, channel.id, on)
        count += 1

    await ctx.send(
        f"Auto-capture **{state.upper()}** for **{count} channels**."
    )


@bot.command()
async def spy(ctx, *, kingdom: str):
    report = get_latest_report(ctx.guild.id, kingdom)

    if not report:
        await ctx.send("❌ No spy reports found.")
        return

    await ctx.send(
        f"🕵️ **Latest Spy Report**\n"
        f"Target: **{report['kingdom'].title()}**\n"
        f"Defense Power: **{report['defense_power']:,}**\n"
        f"Castles: **{report['castles']}**\n"
        f"Captured: <t:{int(datetime.fromisoformat(report['captured_at']).timestamp())}:R>"
    )

# ------------------ RUN ------------------

bot.run(TOKEN)
