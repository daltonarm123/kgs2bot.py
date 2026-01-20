# ---------- KG2 Recon Bot • FULL PATCHED BUILD ----------
# Spy auto-capture (Postgres) • Combat Calc • AP Planner w/ Buttons • Spy History/ID
# Tech capture/indexing (supports raw OR gz)
# REMOVED: Scheduled self-restart / maintenance auto refresher (was causing spam)
# FIX: Restart announcement spam (reconnect guard + DB dedupe + cooldown)

import os, re, asyncio, difflib, hashlib, logging, gzip, sys, time
from math import ceil
from datetime import datetime, timezone

import discord
from discord.ext import commands, tasks
from discord.ui import View, Button
from dotenv import load_dotenv

import psycopg2
from psycopg2.extras import RealDictCursor

# ------------------- PATCH INFO (edit this each deploy) -------------------
BOT_VERSION = "2026-01-20.6"
PATCH_NOTES = [
    "Removed: Scheduled Maintenance auto-refresh (was spamming).",
    "Fixed: Restart announcement spam (on_ready reconnect guard + DB cooldown).",
    "Fixed: Missing APView (AP buttons now work; !ap / !apfix no longer crash).",
    "Fixed: Tech indexing works even when KEEP_RAW_TEXT=false (reads gz backup).",
    "Fixed: !techtop command completed (was truncated / syntax issue).",
    "Added: !techpull (index latest report tech) + !techindex (index all) + !tech (view top tech).",
]
# ------------------------------------------------------------------------

load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ERROR_CHANNEL_NAME = "kg2recon-updates"

logging.basicConfig(level=logging.INFO)

if not TOKEN:
    raise RuntimeError("Missing DISCORD_TOKEN env var.")
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL env var.")

# If false, store gz bytes but NOT plain-text raw column.
KEEP_RAW_TEXT = os.getenv("KEEP_RAW_TEXT", "false").lower() in ("1", "true", "yes", "y")

# ---------- Constants ----------
HEAVY_CAVALRY_AP = 7
AP_REDUCTIONS = [
    ("Minor Victory", 0.19),
    ("Victory", 0.35),
    ("Major Victory", 0.55),
    ("Overwhelming Victory", 0.875),
]

# ---------- Discord ----------
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# ---------- Locks ----------
ap_lock = asyncio.Lock()

# ---------- Announcement anti-spam ----------
ANNOUNCED_READY_THIS_PROCESS = False
ANNOUNCE_COOLDOWN_SECONDS = 15 * 60  # 15 minutes

# ---------- DB ----------
def db_connect():
    return psycopg2.connect(
        DATABASE_URL,
        cursor_factory=RealDictCursor,
        sslmode="require",
    )

def init_db():
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS spy_reports (
            id SERIAL PRIMARY KEY,
            kingdom TEXT,
            defense_power INTEGER,
            castles INTEGER,
            created_at TIMESTAMPTZ,
            raw TEXT,
            raw_gz BYTEA,
            report_hash TEXT UNIQUE
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS dp_sessions (
            id SERIAL PRIMARY KEY,
            kingdom TEXT,
            base_dp INTEGER,
            castles INTEGER,
            current_dp INTEGER,
            hits INTEGER,
            last_hit TEXT,
            captured_at TIMESTAMPTZ
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS tech_index (
            id SERIAL PRIMARY KEY,
            kingdom TEXT,
            tech_name TEXT,
            tech_level INTEGER,
            captured_at TIMESTAMPTZ,
            report_id INTEGER REFERENCES spy_reports(id),
            UNIQUE(kingdom, tech_name, tech_level, report_id)
        );
        """)
        # meta table to dedupe announcements across restarts
        cur.execute("""
        CREATE TABLE IF NOT EXISTS bot_meta (
            k TEXT PRIMARY KEY,
            v TEXT,
            updated_at TIMESTAMPTZ
        );
        """)

        # schema self-heal
        cur.execute("ALTER TABLE spy_reports ADD COLUMN IF NOT EXISTS kingdom TEXT;")
        cur.execute("ALTER TABLE spy_reports ADD COLUMN IF NOT EXISTS defense_power INTEGER;")
        cur.execute("ALTER TABLE spy_reports ADD COLUMN IF NOT EXISTS castles INTEGER;")
        cur.execute("ALTER TABLE spy_reports ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ;")
        cur.execute("ALTER TABLE spy_reports ADD COLUMN IF NOT EXISTS raw TEXT;")
        cur.execute("ALTER TABLE spy_reports ADD COLUMN IF NOT EXISTS raw_gz BYTEA;")
        cur.execute("ALTER TABLE spy_reports ADD COLUMN IF NOT EXISTS report_hash TEXT;")

        try:
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS spy_reports_report_hash_uq ON spy_reports(report_hash);")
        except Exception:
            pass

def heal_sequences():
    with db_connect() as conn, conn.cursor() as cur:
        for table in ["spy_reports", "dp_sessions", "tech_index"]:
            cur.execute(
                f"SELECT setval(pg_get_serial_sequence('{table}','id'), "
                f"COALESCE((SELECT MAX(id) FROM {table}), 1), true);"
            )

# ---------- Helpers ----------
def now_utc():
    return datetime.now(timezone.utc)

def castle_bonus(c: int) -> float:
    return (c ** 0.5) / 100 if c else 0.0

def hash_report(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def parse_spy(text: str):
    kingdom, dp, castles = None, None, 0
    for line in text.splitlines():
        ll = line.lower().strip()
        if ll.startswith("target:"):
            kingdom = line.split(":", 1)[1].strip()
        if "approximate defensive power" in ll or "defensive power" in ll:
            m = re.search(r"\d+", line.replace(",", ""))
            if m:
                dp = int(m.group())
        if "number of castles" in ll:
            m = re.search(r"\d+", line)
            if m:
                castles = int(m.group())
    return kingdom, dp, castles

def parse_tech(text: str):
    techs = []
    for line in text.splitlines():
        m = re.search(r"([\w\s\-\(\)\+\/]+?)\s+Lv\.?\s*(\d+)", line, re.IGNORECASE)
        if m:
            name, level = m.groups()
            techs.append((name.strip(), int(level)))
    return techs

def compress_report(text: str) -> bytes:
    return gzip.compress(text.encode("utf-8"), compresslevel=9)

def decompress_report(raw_gz: bytes) -> str:
    try:
        if isinstance(raw_gz, memoryview):
            raw_gz = raw_gz.tobytes()
        return gzip.decompress(raw_gz).decode("utf-8", errors="replace")
    except Exception:
        return ""

def get_member_for_perms(guild: discord.Guild):
    try:
        if guild.me:
            return guild.me
        if bot.user:
            return guild.get_member(bot.user.id)
    except Exception:
        return None
    return None

def can_send(channel: discord.abc.GuildChannel, guild: discord.Guild) -> bool:
    try:
        member = get_member_for_perms(guild)
        if not member:
            return True
        return channel.permissions_for(member).send_messages
    except Exception:
        return True

async def send_error(guild: discord.Guild, msg: str):
    try:
        ch = discord.utils.get(guild.text_channels, name=ERROR_CHANNEL_NAME)
        if ch and can_send(ch, guild):
            await ch.send(f"⚠️ ERROR LOG:\n```py\n{msg}\n```")
    except Exception:
        pass
    logging.error(msg)

def extract_report_text_for_row(row) -> str:
    raw = row.get("raw")
    if raw:
        return raw
    raw_gz = row.get("raw_gz")
    if raw_gz:
        return decompress_report(raw_gz)
    return ""

def index_tech_from_report_row(cur, row) -> int:
    text = extract_report_text_for_row(row)
    if not text:
        return 0
    techs = parse_tech(text)
    if not techs:
        return 0

    count = 0
    for name, lvl in techs:
        cur.execute("""
            INSERT INTO tech_index (kingdom, tech_name, tech_level, captured_at, report_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, (row.get("kingdom"), name, lvl, row.get("created_at") or now_utc(), row.get("id")))
        count += 1
    return count

def fuzzy_kingdom(query: str):
    if not query:
        return None
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT DISTINCT kingdom FROM spy_reports WHERE kingdom IS NOT NULL;")
        names = [r["kingdom"] for r in cur.fetchall() if r.get("kingdom")]
    match = difflib.get_close_matches(query, names, 1, 0.5)
    return match[0] if match else None

def get_latest_spy_report_for_kingdom(kingdom: str):
    if not kingdom:
        return None
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, kingdom, defense_power, castles, created_at, raw, raw_gz
            FROM spy_reports
            WHERE kingdom=%s AND defense_power IS NOT NULL AND defense_power > 0
            ORDER BY created_at DESC NULLS LAST, id DESC
            LIMIT 1;
        """, (kingdom,))
        return cur.fetchone()

def get_latest_spy_report_any():
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, kingdom, defense_power, castles, created_at, raw, raw_gz
            FROM spy_reports
            WHERE defense_power IS NOT NULL AND defense_power > 0
            ORDER BY created_at DESC NULLS LAST, id DESC
            LIMIT 1;
        """)
        return cur.fetchone()

def rebuild_ap_session(kingdom: str) -> bool:
    spy = get_latest_spy_report_for_kingdom(kingdom)
    if not spy:
        return False
    base_dp = int(spy["defense_power"] or 0)
    castles = int(spy["castles"] or 0)
    if base_dp <= 0:
        return False

    captured_at = spy["created_at"] or now_utc()
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM dp_sessions WHERE kingdom=%s;", (kingdom,))
        cur.execute("""
            INSERT INTO dp_sessions (kingdom, base_dp, castles, current_dp, hits, last_hit, captured_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s);
        """, (kingdom, base_dp, castles, base_dp, 0, None, captured_at))
    return True

def ensure_ap_session(kingdom: str) -> bool:
    if not kingdom:
        return False
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, base_dp, current_dp
            FROM dp_sessions
            WHERE kingdom=%s
            ORDER BY captured_at DESC NULLS LAST, id DESC
            LIMIT 1;
        """, (kingdom,))
        sess = cur.fetchone()

    if sess:
        if int(sess.get("base_dp") or 0) <= 0:
            return rebuild_ap_session(kingdom)
        return True

    return rebuild_ap_session(kingdom)

def build_spy_embed(row):
    dp = int(row.get("defense_power") or 0)
    castles = int(row.get("castles") or 0)
    adjusted = ceil(dp * (1 + castle_bonus(castles)))
    embed = discord.Embed(title="🕵️ Spy Report", color=0x5865F2)
    embed.add_field(name="Kingdom", value=row.get("kingdom") or "Unknown", inline=False)
    embed.add_field(name="Base DP", value=f"{dp:,}", inline=True)
    embed.add_field(name="Adjusted DP", value=f"{adjusted:,}", inline=True)
    embed.add_field(name="Castles", value=str(castles), inline=True)
    embed.set_footer(text=f"ID {row['id']} • Captured {row.get('created_at')}")
    return embed

def build_ap_embed(kingdom: str):
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT base_dp, current_dp, hits, last_hit, castles, captured_at
            FROM dp_sessions
            WHERE kingdom=%s
            ORDER BY captured_at DESC NULLS LAST, id DESC
            LIMIT 1;
        """, (kingdom,))
        row = cur.fetchone()
    if not row:
        return None

    base_dp = int(row.get("base_dp") or 0)
    current_dp = int(row.get("current_dp") or 0)
    hits = int(row.get("hits") or 0)
    castles = int(row.get("castles") or 0)

    embed = discord.Embed(title=f"⚔️ AP Planner • {kingdom}", color=0xE74C3C)
    embed.add_field(name="Base DP", value=f"{base_dp:,}", inline=True)
    embed.add_field(name="Current DP", value=f"{current_dp:,}", inline=True)
    embed.add_field(name="Hits Applied", value=str(hits), inline=True)
    embed.add_field(name="Castles", value=str(castles), inline=True)
    embed.add_field(name="HC Needed (est.)", value=f"{ceil(current_dp / HEAVY_CAVALRY_AP):,}", inline=True)
    if row.get("last_hit"):
        embed.set_footer(text=f"Last hit by {row['last_hit']} • Captured {row.get('captured_at')}")
    else:
        embed.set_footer(text=f"Captured {row.get('captured_at')}")
    return embed

# ---------- AP View ----------
class APView(View):
    def __init__(self, kingdom: str, timeout: float = 600):
        super().__init__(timeout=timeout)
        self.kingdom = kingdom

        for label, red in AP_REDUCTIONS:
            self.add_item(self._make_hit_button(label, red))
        self.add_item(self._make_reset_button())
        self.add_item(self._make_rebuild_button())

    def _make_hit_button(self, label: str, red: float) -> Button:
        async def callback(interaction: discord.Interaction):
            await interaction.response.defer(thinking=False)
            try:
                async with ap_lock:
                    with db_connect() as conn, conn.cursor() as cur:
                        cur.execute("""
                            SELECT id, current_dp, hits
                            FROM dp_sessions
                            WHERE kingdom=%s
                            ORDER BY captured_at DESC NULLS LAST, id DESC
                            LIMIT 1;
                        """, (self.kingdom,))
                        sess = cur.fetchone()
                        if not sess:
                            await interaction.followup.send("❌ No active session. Paste a spy report first, then run `!ap` again.")
                            return

                        current_dp = int(sess.get("current_dp") or 0)
                        new_dp = ceil(current_dp * (1 - red))
                        new_hits = int(sess.get("hits") or 0) + 1
                        last_hit = interaction.user.display_name if interaction.user else "Unknown"

                        cur.execute("""
                            UPDATE dp_sessions
                            SET current_dp=%s, hits=%s, last_hit=%s
                            WHERE id=%s;
                        """, (new_dp, new_hits, last_hit, sess["id"]))

                embed = build_ap_embed(self.kingdom)
                if embed:
                    try:
                        await interaction.message.edit(embed=embed, view=self)
                    except Exception:
                        await interaction.followup.send(embed=embed, view=self)
            except Exception as e:
                await interaction.followup.send("⚠️ Failed to apply hit.")
                if interaction.guild:
                    await send_error(interaction.guild, f"AP hit button error: {e}")

        btn = Button(label=label, style=discord.ButtonStyle.danger)
        btn.callback = callback
        return btn

    def _make_reset_button(self) -> Button:
        async def callback(interaction: discord.Interaction):
            await interaction.response.defer(thinking=False)
            try:
                async with ap_lock:
                    with db_connect() as conn, conn.cursor() as cur:
                        cur.execute("""
                            SELECT id, base_dp
                            FROM dp_sessions
                            WHERE kingdom=%s
                            ORDER BY captured_at DESC NULLS LAST, id DESC
                            LIMIT 1;
                        """, (self.kingdom,))
                        sess = cur.fetchone()
                        if not sess:
                            await interaction.followup.send("❌ No active session to reset.")
                            return
                        base_dp = int(sess.get("base_dp") or 0)
                        cur.execute("""
                            UPDATE dp_sessions
                            SET current_dp=%s, hits=0, last_hit=NULL
                            WHERE id=%s;
                        """, (base_dp, sess["id"]))

                embed = build_ap_embed(self.kingdom)
                if embed:
                    try:
                        await interaction.message.edit(embed=embed, view=self)
                    except Exception:
                        await interaction.followup.send(embed=embed, view=self)
            except Exception as e:
                await interaction.followup.send("⚠️ Failed to reset session.")
                if interaction.guild:
                    await send_error(interaction.guild, f"AP reset error: {e}")

        btn = Button(label="Reset", style=discord.ButtonStyle.secondary)
        btn.callback = callback
        return btn

    def _make_rebuild_button(self) -> Button:
        async def callback(interaction: discord.Interaction):
            await interaction.response.defer(thinking=False)
            try:
                async with ap_lock:
                    ok = rebuild_ap_session(self.kingdom)
                if not ok:
                    await interaction.followup.send("❌ Could not rebuild (no valid spy report found).")
                    return
                embed = build_ap_embed(self.kingdom)
                if embed:
                    try:
                        await interaction.message.edit(embed=embed, view=self)
                    except Exception:
                        await interaction.followup.send(embed=embed, view=self)
            except Exception as e:
                await interaction.followup.send("⚠️ Failed to rebuild session.")
                if interaction.guild:
                    await send_error(interaction.guild, f"AP rebuild error: {e}")

        btn = Button(label="Rebuild", style=discord.ButtonStyle.primary)
        btn.callback = callback
        return btn

# ---------- Announcement dedupe helpers ----------
def _meta_get(cur, key: str):
    cur.execute("SELECT v FROM bot_meta WHERE k=%s LIMIT 1;", (key,))
    row = cur.fetchone()
    return row["v"] if row else None

def _meta_set(cur, key: str, value: str):
    cur.execute("""
        INSERT INTO bot_meta (k, v, updated_at)
        VALUES (%s, %s, %s)
        ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v, updated_at=EXCLUDED.updated_at;
    """, (key, value, now_utc()))

# ---------- Bot Events ----------
@bot.event
async def on_ready():
    global ANNOUNCED_READY_THIS_PROCESS

    try:
        init_db()
        heal_sequences()
    except Exception as e:
        logging.error(f"DB init failed: {e}")

    # ---- FIX: stop restart announcement spam ----
    # 1) If Discord reconnect triggers on_ready again in SAME process: do nothing.
    if ANNOUNCED_READY_THIS_PROCESS:
        logging.info("on_ready called again (same process) - announcement suppressed.")
        return
    ANNOUNCED_READY_THIS_PROCESS = True

    # 2) If Render is restarting the process repeatedly: DB cooldown blocks repeated posts.
    try:
        with db_connect() as conn, conn.cursor() as cur:
            last_ver = _meta_get(cur, "announce_last_ver")
            last_ts = _meta_get(cur, "announce_last_ts")
            now_ts = int(time.time())
            last_ts_int = int(last_ts) if last_ts and str(last_ts).isdigit() else 0

            if last_ver == BOT_VERSION and (now_ts - last_ts_int) < ANNOUNCE_COOLDOWN_SECONDS:
                logging.info("Announcement suppressed (same version + cooldown).")
                return

            _meta_set(cur, "announce_last_ver", BOT_VERSION)
            _meta_set(cur, "announce_last_ts", str(now_ts))
    except Exception as e:
        logging.error(f"Announcement dedupe DB write failed: {e}")
    # --------------------------------------------

    patch_lines = "\n".join([f"• {x}" for x in PATCH_NOTES])
    for guild in bot.guilds:
        ch = discord.utils.get(guild.text_channels, name=ERROR_CHANNEL_NAME)
        if ch and can_send(ch, guild):
            try:
                await ch.send(
                    f"✅ **KG2 Recon Bot restarted**\n"
                    f"Version: `{BOT_VERSION}`\n"
                    f"Patch:\n{patch_lines}"
                )
            except Exception:
                pass

@bot.event
async def on_message(msg: discord.Message):
    if msg.author.bot or not msg.guild:
        return

    try:
        kingdom, dp, castles = parse_spy(msg.content)
        if kingdom and dp and dp >= 1000:
            h = hash_report(msg.content)
            ts = (msg.created_at.replace(tzinfo=timezone.utc) if msg.created_at else now_utc())
            raw_gz = psycopg2.Binary(compress_report(msg.content))
            raw_text = msg.content if KEEP_RAW_TEXT else None

            with db_connect() as conn, conn.cursor() as cur:
                cur.execute("SELECT id FROM spy_reports WHERE report_hash=%s LIMIT 1;", (h,))
                exists = cur.fetchone()

                if not exists:
                    cur.execute("""
                        INSERT INTO spy_reports (kingdom, defense_power, castles, created_at, raw, raw_gz, report_hash)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)
                        RETURNING id, kingdom, defense_power, castles, created_at, raw, raw_gz;
                    """, (kingdom, dp, castles, ts, raw_text, raw_gz, h))
                    row = cur.fetchone()

                    ensure_ap_session(kingdom)

                    if can_send(msg.channel, msg.guild):
                        await msg.channel.send(embed=build_spy_embed(row))
                else:
                    if can_send(msg.channel, msg.guild):
                        await msg.channel.send("✅ Duplicate spy report detected.")
    except Exception as e:
        await send_error(msg.guild, f"on_message spy capture error: {e}")

    await bot.process_commands(msg)

# ---------- Commands ----------
@bot.command()
async def calc(ctx, *, kingdom: str = None):
    try:
        if kingdom:
            real = fuzzy_kingdom(kingdom) or kingdom
            row = get_latest_spy_report_for_kingdom(real)
        else:
            row = get_latest_spy_report_any()

        if not row:
            await ctx.send("📄 No report found. Paste one:")
            try:
                msg = await bot.wait_for(
                    "message",
                    timeout=60,
                    check=lambda m: m.author == ctx.author and m.channel == ctx.channel
                )
            except asyncio.TimeoutError:
                return await ctx.send("⏰ Timed out.")
            k, dp, c = parse_spy(msg.content)
            if not k or not dp:
                return await ctx.send("❌ Parse error.")
            target, used = k, "(pasted)"
        else:
            dp = int(row["defense_power"])
            c = int(row["castles"] or 0)
            target = row["kingdom"]
            used = f"(from DB: {row['id']})"

        adj = ceil(dp * (1 + castle_bonus(c)))
        embed = discord.Embed(title="⚔️ Combat Calculator", color=0x5865F2)
        embed.add_field(name="Target", value=f"{target} {used}", inline=False)
        embed.add_field(name="Base DP", value=f"{dp:,}", inline=True)
        embed.add_field(name="Adjusted DP", value=f"{adj:,}", inline=True)
        embed.add_field(name="Castles", value=str(c), inline=True)

        for label, red in AP_REDUCTIONS:
            rem = ceil(adj * (1 - red))
            embed.add_field(
                name=f"{label} (-{int(red*100)}%)",
                value=f"DP: {rem:,}\nHC: {ceil(rem/HEAVY_CAVALRY_AP):,}",
                inline=False
            )

        await ctx.send(embed=embed)
    except Exception as e:
        await ctx.send("⚠️ calc failed.")
        await send_error(ctx.guild, f"calc error: {e}")

@bot.command()
async def spy(ctx, *, kingdom: str):
    try:
        real = fuzzy_kingdom(kingdom) or kingdom
        row = get_latest_spy_report_for_kingdom(real)
        if not row:
            return await ctx.send(f"❌ No reports for **{real}**.")
        await ctx.send(embed=build_spy_embed(row))
    except Exception as e:
        await ctx.send("⚠️ spy failed.")
        await send_error(ctx.guild, f"spy error: {e}")

@bot.command()
async def spyid(ctx, report_id: int):
    try:
        with db_connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT id, kingdom, defense_power, castles, created_at, raw, raw_gz FROM spy_reports WHERE id=%s;", (report_id,))
            row = cur.fetchone()
        if not row:
            return await ctx.send(f"❌ ID {report_id} not found.")
        await ctx.send(embed=build_spy_embed(row))
    except Exception as e:
        await ctx.send("⚠️ spyid failed.")
        await send_error(ctx.guild, f"spyid error: {e}")

@bot.command()
async def spyhistory(ctx, *, kingdom: str):
    try:
        real = fuzzy_kingdom(kingdom) or kingdom
        with db_connect() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT id, defense_power, created_at
                FROM spy_reports
                WHERE kingdom=%s
                ORDER BY created_at DESC NULLS LAST, id DESC
                LIMIT 5;
            """, (real,))
            rows = cur.fetchall()

        if not rows:
            return await ctx.send(f"❌ No history for **{real}**.")

        lines = []
        for r in rows:
            dp = int(r.get("defense_power") or 0)
            dt = r.get("created_at")
            dts = dt.strftime("%Y-%m-%d") if dt else "unknown-date"
            lines.append(f"`#{r['id']}`: {dp:,} DP ({dts})")

        await ctx.send(f"📂 **History for {real}**:\n" + "\n".join(lines))
    except Exception as e:
        await ctx.send("⚠️ spyhistory failed.")
        await send_error(ctx.guild, f"spyhistory error: {e}")

@bot.command()
async def ap(ctx, *, kingdom: str):
    try:
        real = fuzzy_kingdom(kingdom) or kingdom
        if ensure_ap_session(real):
            emb = build_ap_embed(real)
            if not emb:
                return await ctx.send("❌ No active session embed. Try `!apfix <kingdom>`.")
            await ctx.send(embed=emb, view=APView(real))
        else:
            await ctx.send("❌ No spy report found for that kingdom.")
    except Exception as e:
        await ctx.send("⚠️ ap failed.")
        await send_error(ctx.guild, f"ap error: {e}")

@bot.command()
async def apstatus(ctx, *, kingdom: str):
    try:
        real = fuzzy_kingdom(kingdom) or kingdom
        emb = build_ap_embed(real)
        if emb:
            await ctx.send(embed=emb)
        else:
            await ctx.send("❌ No active session.")
    except Exception as e:
        await ctx.send("⚠️ apstatus failed.")
        await send_error(ctx.guild, f"apstatus error: {e}")

@bot.command()
async def apfix(ctx, *, kingdom: str):
    try:
        real = fuzzy_kingdom(kingdom) or kingdom
        if rebuild_ap_session(real):
            await ctx.send(f"✅ Rebuilt AP session for **{real}**.")
            emb = build_ap_embed(real)
            if emb:
                await ctx.send(embed=emb, view=APView(real))
        else:
            await ctx.send("❌ No valid spy report found.")
    except Exception as e:
        await ctx.send("⚠️ apfix failed.")
        await send_error(ctx.guild, f"apfix error: {e}")

@bot.command()
async def techindex(ctx):
    """Index tech entries from ALL reports (raw if present, else gz)."""
    try:
        with db_connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT id, kingdom, raw, raw_gz, created_at FROM spy_reports WHERE kingdom IS NOT NULL;")
            reports = cur.fetchall()
            count = 0
            for r in reports:
                count += index_tech_from_report_row(cur, r)
        await ctx.send(f"✅ Indexed {count} tech entries from all saved reports.")
    except Exception as e:
        await ctx.send("⚠️ techindex failed.")
        await send_error(ctx.guild, f"techindex error: {e}")

@bot.command()
async def techpull(ctx, *, kingdom: str):
    """Index tech entries from the LATEST spy report for a kingdom."""
    try:
        real = fuzzy_kingdom(kingdom) or kingdom
        row = get_latest_spy_report_for_kingdom(real)
        if not row:
            return await ctx.send(f"❌ No reports for **{real}**.")
        with db_connect() as conn, conn.cursor() as cur:
            count = index_tech_from_report_row(cur, row)
        if count <= 0:
            await ctx.send(f"⚠️ No tech lines detected in the latest report for **{real}**.")
        else:
            await ctx.send(f"✅ Indexed {count} tech entries from the latest report for **{real}**.")
    except Exception as e:
        await ctx.send("⚠️ techpull failed.")
        await send_error(ctx.guild, f"techpull error: {e}")

@bot.command()
async def tech(ctx, *, kingdom: str):
    """Show top 15 tech entries (highest levels) for a kingdom."""
    try:
        real = fuzzy_kingdom(kingdom) or kingdom
        with db_connect() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT tech_name, tech_level
                FROM tech_index
                WHERE kingdom=%s
                ORDER BY tech_level DESC, tech_name ASC
                LIMIT 15;
            """, (real,))
            rows = cur.fetchall()

        if not rows:
            return await ctx.send(f"❌ No tech found for **{real}**. Run `!techpull {real}` or `!techindex` first.")

        txt = "\n".join([f"• {r['tech_name']}: Lv. {int(r['tech_level'] or 0)}" for r in rows])
        await ctx.send(f"🧪 **Tech Index: {real}**\n{txt}")
    except Exception as e:
        await ctx.send("⚠️ tech failed.")
        await send_error(ctx.guild, f"tech error: {e}")

@bot.command()
async def techtop(ctx, *, tech_name: str):
    """Leaderboard for a tech across kingdoms. Example: !techtop Heavy Cavalry"""
    try:
        q = tech_name.strip()
        if not q:
            return await ctx.send("❌ Provide a tech name. Example: `!techtop Heavy Cavalry`")

        with db_connect() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT kingdom, MAX(tech_level) AS best_level
                FROM tech_index
                WHERE tech_name ILIKE %s
                GROUP BY kingdom
                ORDER BY best_level DESC, kingdom ASC
                LIMIT 10;
            """, (f"%{q}%",))
            rows = cur.fetchall()

        if not rows:
            return await ctx.send(f"❌ No matches found for tech: **{q}** (index first with `!techindex`).")

        lines = []
        rank = 1
        for r in rows:
            kingdom = r.get("kingdom") or "Unknown"
            lvl = int(r.get("best_level") or 0)
            lines.append(f"`#{rank}` **{kingdom}** — Lv. **{lvl}**")
            rank += 1

        await ctx.send("🏆 **Top Kingdoms for:** `{}`\n{}".format(q, "\n".join(lines)))
    except Exception as e:
        await ctx.send("⚠️ techtop failed.")
        await send_error(ctx.guild, f"techtop error: {e}")

# ---------- START BOT ----------
bot.run(TOKEN)
