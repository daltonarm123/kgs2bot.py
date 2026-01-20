# ---------- KG2 Recon Bot • FULL PATCHED BUILD ----------
# Spy auto-capture (Postgres) • Combat Calc • AP Planner w/ Buttons • Spy History/ID
# Tech capture/indexing (supports raw OR gz) • Per-kingdom deduped tech table + CSV export
# FIX: Restart announcement spam (reconnect guard + DB dedupe + cooldown)
# FIX: !calc prompts for paste by default, DB optional
# FIX: tech parsing supports KG2 format: "lvl 5" / "lv 5" and backward-searching techpull
# ADD: !refresh (admin-only) restart
# ADD: !techexport creates CSV of each kingdom + all deduped research

import os, re, asyncio, difflib, hashlib, logging, gzip, sys, time, io
from math import ceil
from datetime import datetime, timezone

import discord
from discord.ext import commands
from discord.ui import View, Button
from dotenv import load_dotenv

import psycopg2
from psycopg2.extras import RealDictCursor

# ------------------- PATCH INFO -------------------
BOT_VERSION = "2026-01-20.9"
PATCH_NOTES = [
    "Fixed: Tech parsing for KG2 format (lvl/lv/level).",
    "Fixed: Reports save even if DP missing when tech lines exist.",
    "Fixed: !techpull searches backwards through recent reports until tech is found.",
    "Added: kingdom_tech table (deduped: best level per kingdom+tech).",
    "Added: !techexport CSV (kingdom + tech list) for Excel/PowerBI.",
    "Added: !refresh (admin-only manual restart).",
    "Kept: Restart announcement dedupe + AP planner + spy history/ID + calc prompt.",
]
# -------------------------------------------------

load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ERROR_CHANNEL_NAME = "kg2recon-updates"

logging.basicConfig(level=logging.INFO)

if not TOKEN:
    raise RuntimeError("Missing DISCORD_TOKEN env var.")
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL env var.")

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

        # Raw tech lines per report (history)
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

        # NEW: deduped “best tech per kingdom”
        cur.execute("""
        CREATE TABLE IF NOT EXISTS kingdom_tech (
            kingdom TEXT NOT NULL,
            tech_name TEXT NOT NULL,
            best_level INTEGER NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            source_report_id INTEGER,
            PRIMARY KEY (kingdom, tech_name)
        );
        """)

        # meta table for restart announcement dedupe
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
    """
    KG2 tech section uses:
      "Improved Tools lvl 5"
      "Something lv 12"
    We support: lv / lvl / level / Lv / Lv.
    """
    techs = []
    for line in text.splitlines():
        s = line.strip()
        if not s:
            continue

        s = s.lstrip("•-*—– ").strip()

        # Name + lvl/lv/level + number
        m = re.match(r"^(.+?)\s+(?:lv\.?|lvl\.?|level)\s*(\d{1,3})\s*$", s, re.IGNORECASE)
        if m:
            name = m.group(1).strip()
            lvl = int(m.group(2))
            if 1 <= lvl <= 300 and len(name) >= 3:
                techs.append((name, lvl))
            continue

        # Name: 12  OR  Name - 12
        m = re.match(r"^(.+?)\s*[:\-–—]\s*(\d{1,3})\s*$", s)
        if m:
            name = m.group(1).strip()
            lvl = int(m.group(2))
            if 1 <= lvl <= 300 and len(name) >= 3:
                techs.append((name, lvl))
            continue

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

def fuzzy_kingdom(query: str):
    if not query:
        return None
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT DISTINCT kingdom FROM spy_reports WHERE kingdom IS NOT NULL;")
        names = [r["kingdom"] for r in cur.fetchall() if r.get("kingdom")]
    match = difflib.get_close_matches(query, names, 1, 0.5)
    return match[0] if match else None

def get_latest_spy_report_for_kingdom(kingdom: str):
    """DP-valid report (used by AP sessions)"""
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

def get_latest_report_for_kingdom_any(kingdom: str):
    """Any report (DP or not)"""
    if not kingdom:
        return None
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, kingdom, defense_power, castles, created_at, raw, raw_gz
            FROM spy_reports
            WHERE kingdom=%s
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

def get_recent_reports_for_kingdom(kingdom: str, limit: int = 25):
    with db_connect() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, kingdom, defense_power, castles, created_at, raw, raw_gz
            FROM spy_reports
            WHERE kingdom=%s
            ORDER BY created_at DESC NULLS LAST, id DESC
            LIMIT %s;
        """, (kingdom, limit))
        return cur.fetchall()

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
    dp = int(row.get("defense_power") or 0) if row.get("defense_power") is not None else 0
    castles = int(row.get("castles") or 0)
    adjusted = ceil(dp * (1 + castle_bonus(castles))) if dp > 0 else 0

    embed = discord.Embed(title="🕵️ Spy Report", color=0x5865F2)
    embed.add_field(name="Kingdom", value=row.get("kingdom") or "Unknown", inline=False)
    embed.add_field(name="Base DP", value=(f"{dp:,}" if dp else "N/A"), inline=True)
    embed.add_field(name="Adjusted DP", value=(f"{adjusted:,}" if adjusted else "N/A"), inline=True)
    embed.add_field(name="Castles", value=str(castles), inline=True)
    embed.set_footer(text=f"ID {row['id']} • Captured {row.get('created_at')}")
    return embed

def build_calc_embed(target: str, dp: int, castles: int, used: str):
    adj = ceil(dp * (1 + castle_bonus(castles)))
    embed = discord.Embed(title="⚔️ Combat Calculator", color=0x5865F2)
    embed.add_field(name="Target", value=f"{target} {used}", inline=False)
    embed.add_field(name="Base DP", value=f"{dp:,}", inline=True)
    embed.add_field(name="Adjusted DP", value=f"{adj:,}", inline=True)
    embed.add_field(name="Castles", value=str(castles), inline=True)

    for label, red in AP_REDUCTIONS:
        rem = ceil(adj * (1 - red))
        embed.add_field(
            name=f"{label} (-{int(red*100)}%)",
            value=f"DP: {rem:,}\nHC: {ceil(rem/HEAVY_CAVALRY_AP):,}",
            inline=False
        )
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

# ---------- Tech Indexing (dedupe) ----------
def upsert_kingdom_tech(cur, kingdom: str, tech_name: str, level: int, report_id: int, captured_at):
    """
    Deduped: keep best (highest) level for kingdom+tech_name.
    """
    if not kingdom or not tech_name or not level:
        return False

    cur.execute("""
        SELECT best_level FROM kingdom_tech
        WHERE kingdom=%s AND tech_name=%s
        LIMIT 1;
    """, (kingdom, tech_name))
    row = cur.fetchone()
    existing = int(row["best_level"]) if row else None

    if existing is None or level > existing:
        cur.execute("""
            INSERT INTO kingdom_tech (kingdom, tech_name, best_level, updated_at, source_report_id)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (kingdom, tech_name) DO UPDATE
            SET best_level=EXCLUDED.best_level,
                updated_at=EXCLUDED.updated_at,
                source_report_id=EXCLUDED.source_report_id;
        """, (kingdom, tech_name, level, captured_at or now_utc(), report_id))
        return True

    return False

def index_tech_from_report_row(cur, row) -> tuple[int, int]:
    """
    Index tech from a report:
      - tech_index (history): ON CONFLICT DO NOTHING
      - kingdom_tech (dedupe): keep max level per tech
    Returns: (history_count, dedupe_updates)
    """
    text = extract_report_text_for_row(row)
    if not text:
        return (0, 0)

    techs = parse_tech(text)
    if not techs:
        return (0, 0)

    history_count = 0
    dedupe_updates = 0
    captured_at = row.get("created_at") or now_utc()
    kingdom = row.get("kingdom")
    report_id = row.get("id")

    for name, lvl in techs:
        cur.execute("""
            INSERT INTO tech_index (kingdom, tech_name, tech_level, captured_at, report_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, (kingdom, name, lvl, captured_at, report_id))
        history_count += 1

        if upsert_kingdom_tech(cur, kingdom, name, lvl, report_id, captured_at):
            dedupe_updates += 1

    return (history_count, dedupe_updates)

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

    # suppress reconnect spam in same process
    if ANNOUNCED_READY_THIS_PROCESS:
        logging.info("on_ready called again (same process) - announcement suppressed.")
        return
    ANNOUNCED_READY_THIS_PROCESS = True

    # suppress rapid restarts via DB cooldown
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
        techs = parse_tech(msg.content)

        # Save if we have a target AND either:
        # - DP looks valid, OR
        # - tech lines exist (even if DP missing)
        should_save = bool(kingdom) and (
            (dp is not None and dp >= 1000) or
            (techs and len(techs) >= 1)
        )

        if should_save:
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

                    # auto AP session only if DP exists
                    if dp is not None and dp >= 1000:
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
    """
    Behavior:
      - !calc            -> prompts you to paste a spy report (default)
      - !calc <kingdom>  -> uses latest saved report for that kingdom (DP report)
      - !calc db         -> uses latest saved report overall (DP report)
    """
    try:
        arg = (kingdom or "").strip()

        if arg.lower() in ("db", "last", "latest"):
            row = get_latest_spy_report_any()
            if not row:
                return await ctx.send("❌ No saved spy reports in DB yet.")
            dp = int(row["defense_power"])
            c = int(row["castles"] or 0)
            target = row["kingdom"] or "Unknown"
            used = f"(from DB: {row['id']})"
            return await ctx.send(embed=build_calc_embed(target, dp, c, used))

        if arg:
            real = fuzzy_kingdom(arg) or arg
            row = get_latest_spy_report_for_kingdom(real)
            if not row:
                return await ctx.send(f"❌ No saved DP reports for **{real}**. Paste a spy report and try again.")
            dp = int(row["defense_power"])
            c = int(row["castles"] or 0)
            target = row["kingdom"] or real
            used = f"(from DB: {row['id']})"
            return await ctx.send(embed=build_calc_embed(target, dp, c, used))

        await ctx.send("📄 Paste the spy report you want to calculate against (you have 90 seconds).")
        try:
            msg = await bot.wait_for(
                "message",
                timeout=90,
                check=lambda m: m.author == ctx.author and m.channel == ctx.channel
            )
        except asyncio.TimeoutError:
            return await ctx.send("⏰ Timed out. Run `!calc` again.")

        k, dp, c = parse_spy(msg.content)
        if not k or not dp:
            return await ctx.send("❌ Could not parse that spy report. Make sure it includes Target + Defensive Power.")
        await ctx.send(embed=build_calc_embed(k, int(dp), int(c or 0), "(pasted)"))

    except Exception as e:
        await ctx.send("⚠️ calc failed.")
        await send_error(ctx.guild, f"calc error: {e}")

@bot.command()
async def spy(ctx, *, kingdom: str):
    try:
        real = fuzzy_kingdom(kingdom) or kingdom
        row = get_latest_report_for_kingdom_any(real)
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
            dp = r.get("defense_power")
            dp_s = f"{int(dp):,} DP" if dp else "DP N/A"
            dt = r.get("created_at")
            dts = dt.strftime("%Y-%m-%d") if dt else "unknown-date"
            lines.append(f"`#{r['id']}`: {dp_s} ({dts})")

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
            await ctx.send("❌ No DP spy report found for that kingdom.")
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
            await ctx.send("❌ No valid DP spy report found.")
    except Exception as e:
        await ctx.send("⚠️ apfix failed.")
        await send_error(ctx.guild, f"apfix error: {e}")

@bot.command()
async def techpull(ctx, *, kingdom: str):
    """
    Index tech from the most recent report that actually CONTAINS tech lines.
    Searches backwards through up to 25 recent reports.
    """
    try:
        real = fuzzy_kingdom(kingdom) or kingdom
        reports = get_recent_reports_for_kingdom(real, limit=25)
        if not reports:
            return await ctx.send(f"❌ No reports found for **{real}**.")

        chosen = None
        chosen_techs = None

        for r in reports:
            text = extract_report_text_for_row(r)
            techs = parse_tech(text)
            if techs:
                chosen = r
                chosen_techs = techs
                break

        if not chosen:
            return await ctx.send(f"⚠️ No tech lines detected in the last {len(reports)} saved reports for **{real}**.\n"
                                  f"Tip: paste a fresh full spy report that includes the tech section, then run `!techpull {real}` again.")

        with db_connect() as conn, conn.cursor() as cur:
            hist_count, dedupe_updates = index_tech_from_report_row(cur, chosen)

        await ctx.send(
            f"✅ Tech pulled for **{real}** from report `#{chosen['id']}`.\n"
            f"• Parsed tech lines: **{len(chosen_techs)}**\n"
            f"• Added to history: **{hist_count}**\n"
            f"• Updated best-tech list: **{dedupe_updates}**"
        )

    except Exception as e:
        await ctx.send("⚠️ techpull failed.")
        await send_error(ctx.guild, f"techpull error: {e}")

@bot.command()
async def techindex(ctx):
    """
    Index tech for ALL saved reports into:
      - tech_index (history)
      - kingdom_tech (deduped best tech per kingdom)
    """
    try:
        with db_connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT id, kingdom, raw, raw_gz, created_at FROM spy_reports WHERE kingdom IS NOT NULL;")
            reports = cur.fetchall()

            total_hist = 0
            total_updates = 0
            scanned = 0
            reports_with_tech = 0

            for r in reports:
                scanned += 1
                hist_count, dedupe_updates = index_tech_from_report_row(cur, r)
                if hist_count > 0:
                    reports_with_tech += 1
                total_hist += hist_count
                total_updates += dedupe_updates

        await ctx.send(
            f"✅ Tech index complete.\n"
            f"• Reports scanned: **{scanned}**\n"
            f"• Reports with tech: **{reports_with_tech}**\n"
            f"• Added to history: **{total_hist}**\n"
            f"• Updated best-tech list: **{total_updates}**"
        )
    except Exception as e:
        await ctx.send("⚠️ techindex failed.")
        await send_error(ctx.guild, f"techindex error: {e}")

@bot.command()
async def tech(ctx, *, kingdom: str):
    """Show top 15 tech entries (highest levels) for a kingdom from deduped best-tech list."""
    try:
        real = fuzzy_kingdom(kingdom) or kingdom
        with db_connect() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT tech_name, best_level
                FROM kingdom_tech
                WHERE kingdom=%s
                ORDER BY best_level DESC, tech_name ASC
                LIMIT 15;
            """, (real,))
            rows = cur.fetchall()

        if not rows:
            return await ctx.send(f"❌ No tech found for **{real}**.\nRun `!techpull {real}` or `!techindex` first.")

        txt = "\n".join([f"• {r['tech_name']}: lvl {int(r['best_level'] or 0)}" for r in rows])
        await ctx.send(f"🧪 **Tech (Best Known): {real}**\n{txt}")
    except Exception as e:
        await ctx.send("⚠️ tech failed.")
        await send_error(ctx.guild, f"tech error: {e}")

@bot.command()
async def techtop(ctx, *, tech_name: str):
    """Leaderboard for a tech across kingdoms (uses deduped best-tech list)."""
    try:
        q = tech_name.strip()
        if not q:
            return await ctx.send("❌ Provide a tech name. Example: `!techtop Heavy Cavalry`")

        with db_connect() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT kingdom, best_level
                FROM kingdom_tech
                WHERE tech_name ILIKE %s
                ORDER BY best_level DESC, kingdom ASC
                LIMIT 10;
            """, (f"%{q}%",))
            rows = cur.fetchall()

        if not rows:
            return await ctx.send(f"❌ No matches found for tech: **{q}**.\nIndex first with `!techindex` or `!techpull <kingdom>`.")

        lines = []
        for i, r in enumerate(rows, start=1):
            kingdom = r.get("kingdom") or "Unknown"
            lvl = int(r.get("best_level") or 0)
            lines.append(f"`#{i}` **{kingdom}** — lvl **{lvl}**")

        await ctx.send("🏆 **Top Kingdoms for:** `{}`\n{}".format(q, "\n".join(lines)))
    except Exception as e:
        await ctx.send("⚠️ techtop failed.")
        await send_error(ctx.guild, f"techtop error: {e}")

@bot.command()
async def techexport(ctx):
    """
    Export deduped tech list for all kingdoms as a CSV file for Excel/Power BI.
    Columns: kingdom, tech_name, best_level, updated_at, source_report_id
    """
    try:
        with db_connect() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT kingdom, tech_name, best_level, updated_at, source_report_id
                FROM kingdom_tech
                ORDER BY kingdom ASC, tech_name ASC;
            """)
            rows = cur.fetchall()

        if not rows:
            return await ctx.send("❌ No tech data to export yet. Run `!techindex` first.")

        out = io.StringIO()
        out.write("kingdom,tech_name,best_level,updated_at,source_report_id\n")
        for r in rows:
            # Simple CSV escaping for commas/quotes
            kingdom = (r.get("kingdom") or "").replace('"', '""')
            tech_name = (r.get("tech_name") or "").replace('"', '""')
            best_level = int(r.get("best_level") or 0)
            updated_at = r.get("updated_at")
            updated_at_s = updated_at.isoformat() if updated_at else ""
            source_id = r.get("source_report_id") or ""

            out.write(f"\"{kingdom}\",\"{tech_name}\",{best_level},\"{updated_at_s}\",{source_id}\n")

        data = out.getvalue().encode("utf-8")
        out.close()

        filename = f"kg2_tech_export_{BOT_VERSION}.csv"
        file = discord.File(fp=io.BytesIO(data), filename=filename)

        await ctx.send("📎 Here’s the current deduped tech export (best known per kingdom):", file=file)

    except Exception as e:
        await ctx.send("⚠️ techexport failed.")
        await send_error(ctx.guild, f"techexport error: {e}")

def _is_admin(ctx: commands.Context) -> bool:
    try:
        if ctx.guild and ctx.author and getattr(ctx.author, "guild_permissions", None):
            return bool(ctx.author.guild_permissions.administrator)
    except Exception:
        pass
    return False

@bot.command(name="refresh")
async def refresh(ctx):
    """Admin-only manual restart (Render will restart the service)."""
    try:
        if not _is_admin(ctx):
            return await ctx.send("❌ You don’t have permission to use this command.")

        try:
            await ctx.send("🔄 Refreshing bot now… (manual restart)")
        except Exception:
            pass

        if ctx.guild:
            try:
                ch = discord.utils.get(ctx.guild.text_channels, name=ERROR_CHANNEL_NAME)
                if ch and can_send(ch, ctx.guild):
                    await ch.send(f"🔄 Manual refresh requested by **{ctx.author.display_name}**")
            except Exception:
                pass

        await asyncio.sleep(1.0)
        os.execv(sys.executable, [sys.executable] + sys.argv)

    except Exception as e:
        await ctx.send("⚠️ Refresh failed.")
        if ctx.guild:
            await send_error(ctx.guild, f"refresh error: {e}")

# ---------- START BOT ----------
bot.run(TOKEN)
