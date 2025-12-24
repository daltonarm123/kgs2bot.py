import discord
from discord.ext import commands
import sqlite3
import re
from datetime import datetime, timedelta
import csv
import io

# ---------------- CONFIG ----------------

TOKEN = "YOUR_BOT_TOKEN_HERE"
DB_FILE = "kg2recon.db"
PREFIX = "!"

intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
intents.messages = True

bot = commands.Bot(command_prefix=PREFIX, intents=intents)

# ---------------- DATABASE ----------------

def get_db():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def init_db():
    with get_db() as db:
        db.executescript("""
        CREATE TABLE IF NOT EXISTS spy_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id INTEGER,
            kingdom TEXT,
            defense_power INTEGER,
            castles INTEGER,
            captured_at TEXT
        );

        CREATE TABLE IF NOT EXISTS channel_settings (
            guild_id INTEGER,
            channel_id INTEGER,
            watch INTEGER,
            PRIMARY KEY (guild_id, channel_id)
        );

        CREATE TABLE IF NOT EXISTS guild_settings (
            guild_id INTEGER PRIMARY KEY,
            watch_all INTEGER
        );

        CREATE INDEX IF NOT EXISTS idx_spy_lookup
        ON spy_reports (guild_id, kingdom, captured_at);
        """)

# ---------------- WATCH HELPERS ----------------

def set_watch(guild_id, channel_id, watch):
    with get_db() as db:
        db.execute("""
        INSERT INTO channel_settings VALUES (?, ?, ?)
        ON CONFLICT(guild_id, channel_id)
        DO UPDATE SET watch=excluded.watch
        """, (guild_id, channel_id, int(watch)))


def is_watching(guild_id, channel_id):
    with get_db() as db:
        row = db.execute("""
        SELECT watch FROM channel_settings
        WHERE guild_id=? AND channel_id=?
        """, (guild_id, channel_id)).fetchone()
        return bool(row["watch"]) if row else False


def set_watch_all(guild_id, state):
    with get_db() as db:
        db.execute("""
        INSERT INTO guild_settings VALUES (?, ?)
        ON CONFLICT(guild_id)
        DO UPDATE SET watch_all=excluded.watch_all
        """, (guild_id, int(state)))


def is_watch_all(guild_id):
    with get_db() as db:
        row = db.execute("""
        SELECT watch_all FROM guild_settings WHERE guild_id=?
        """, (guild_id,)).fetchone()
        return bool(row["watch_all"]) if row else False

# ---------------- SPY HELPERS ----------------

def normalize_kingdom(k):
    return k.strip().lower()


def is_duplicate(guild_id, kingdom, dp, castles):
    with get_db() as db:
        row = db.execute("""
        SELECT captured_at FROM spy_reports
        WHERE guild_id=? AND kingdom=? AND defense_power=? AND castles=?
        ORDER BY captured_at DESC LIMIT 1
        """, (guild_id, kingdom, dp, castles)).fetchone()

        if not row:
            return False

        last = datetime.fromisoformat(row["captured_at"])
        return datetime.utcnow() - last < timedelta(minutes=10)


def save_spy(guild_id, kingdom, dp, castles):
    kingdom = normalize_kingdom(kingdom)

    if is_duplicate(guild_id, kingdom, dp, castles):
        return False

    with get_db() as db:
        db.execute("""
        INSERT INTO spy_reports
        VALUES (NULL, ?, ?, ?, ?, ?)
        """, (
            guild_id,
            kingdom,
            dp,
            castles,
            datetime.utcnow().isoformat()
        ))
    return True


def fuzzy_match_kingdom(guild_id, query):
    q = normalize_kingdom(query)
    with get_db() as db:
        rows = db.execute("""
        SELECT DISTINCT kingdom FROM spy_reports
        WHERE guild_id=? AND kingdom LIKE ?
        """, (guild_id, f"%{q}%")).fetchall()

    return rows[0]["kingdom"] if rows else None

# ---------------- PARSER ----------------

SPY_REGEX = re.compile(
    r"Target:\s*(?P<kingdom>.+?)\n.*?"
    r"Defense Power:\s*(?P<dp>[\d,]+).*?"
    r"Castles:\s*(?P<castles>\d+)",
    re.I | re.S
)

def parse_spy(text):
    m = SPY_REGEX.search(text)
    if not m:
        return None
    return {
        "kingdom": m.group("kingdom"),
        "dp": int(m.group("dp").replace(",", "")),
        "castles": int(m.group("castles"))
    }

# ---------------- EVENTS ----------------

@bot.event
async def on_ready():
    init_db()
    print(f"✅ Logged in as {bot.user}")


@bot.event
async def on_guild_channel_create(channel):
    if isinstance(channel, discord.TextChannel):
        if is_watch_all(channel.guild.id):
            set_watch(channel.guild.id, channel.id, True)


@bot.event
async def on_message(message):
    if message.author.bot or not message.guild:
        return

    if is_watching(message.guild.id, message.channel.id):
        data = parse_spy(message.content)
        if data:
            saved = save_spy(
                message.guild.id,
                data["kingdom"],
                data["dp"],
                data["castles"]
            )
            if saved:
                await message.channel.send(
                    f"🕵️ Spy Report Captured: **{data['kingdom'].title()}**"
                )

    await bot.process_commands(message)

# ---------------- COMMANDS ----------------

@bot.command()
@commands.has_permissions(manage_channels=True)
async def watchhere(ctx, state: str):
    set_watch(ctx.guild.id, ctx.channel.id, state.lower() == "on")
    await ctx.send(f"Watch **{state.upper()}** for this channel.")


@bot.command()
@commands.has_permissions(manage_guild=True)
async def watchall(ctx, state: str):
    on = state.lower() == "on"
    set_watch_all(ctx.guild.id, on)

    for ch in ctx.guild.text_channels:
        set_watch(ctx.guild.id, ch.id, on)

    await ctx.send(f"Watch **{state.upper()}** for all channels.")


@bot.command()
async def watchstatus(ctx):
    watched = []
    for ch in ctx.guild.text_channels:
        if is_watching(ctx.guild.id, ch.id):
            watched.append(ch.mention)

    if not watched:
        await ctx.send("No channels are being watched.")
    else:
        await ctx.send("📡 Watched Channels:\n" + "\n".join(watched))


@bot.command()
async def spy(ctx, *, kingdom: str):
    match = fuzzy_match_kingdom(ctx.guild.id, kingdom)
    if not match:
        await ctx.send("❌ No spy reports found.")
        return

    with get_db() as db:
        r = db.execute("""
        SELECT * FROM spy_reports
        WHERE guild_id=? AND kingdom=?
        ORDER BY captured_at DESC LIMIT 1
        """, (ctx.guild.id, match)).fetchone()

    await ctx.send(
        f"🕵️ **Latest Spy Report**\n"
        f"Target: **{match.title()}**\n"
        f"DP: **{r['defense_power']:,}**\n"
        f"Castles: **{r['castles']}**"
    )


@bot.command()
async def spyhistory(ctx, *, kingdom: str):
    match = fuzzy_match_kingdom(ctx.guild.id, kingdom)
    if not match:
        await ctx.send("❌ No history found.")
        return

    with get_db() as db:
        rows = db.execute("""
        SELECT defense_power, castles, captured_at
        FROM spy_reports
        WHERE guild_id=? AND kingdom=?
        ORDER BY captured_at DESC LIMIT 10
        """, (ctx.guild.id, match)).fetchall()

    msg = f"📜 **Spy History — {match.title()}**\n"
    for r in rows:
        t = int(datetime.fromisoformat(r["captured_at"]).timestamp())
        msg += f"DP {r['defense_power']:,} | C {r['castles']} | <t:{t}:R>\n"

    await ctx.send(msg)


@bot.command()
async def exportspy(ctx, *, kingdom: str):
    match = fuzzy_match_kingdom(ctx.guild.id, kingdom)
    if not match:
        await ctx.send("❌ No data.")
        return

    with get_db() as db:
        rows = db.execute("""
        SELECT defense_power, castles, captured_at
        FROM spy_reports
        WHERE guild_id=? AND kingdom=?
        ORDER BY captured_at DESC
        """, (ctx.guild.id, match)).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Defense Power", "Castles", "Captured At"])
    for r in rows:
        writer.writerow([r["defense_power"], r["castles"], r["captured_at"]])

    output.seek(0)
    await ctx.send(
        file=discord.File(
            fp=io.BytesIO(output.getvalue().encode()),
            filename=f"{match}_spy_reports.csv"
        )
    )

# ---------------- RUN ----------------

bot.run(TOKEN)
