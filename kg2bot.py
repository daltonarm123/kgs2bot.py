# KG2 Recon Bot (discord.py v2)
# Sleek embeds • Auto-capture • History • AP Planner (per-hit) • Castle bonus
# Cav-vs-Pike tip • Export • Help menu • Auto Attack-Report DP estimate
#
# Commands:
#   !kg2help  (alias: !commands)
#   !watchhere on/off [DefaultKingdom]
#   !watchall on/off [DefaultKingdom]      (admin)
#   !savereport [message link]
#   !addspy <Kingdom>                      (admin guided paste)
#   !spy <Kingdom>
#   !ap <Kingdom> [hits]                   (hits defaults to 0)
#   !spyhistory <Kingdom> [N]
#   !spyid <Kingdom> <ID>
#   !exportspy <Kingdom> [N]               (admin)
#   !rescanlast <Kingdom>                  (admin)  re-parse latest raw row
#   !rescanrange <Kingdom> [N=5]           (admin)  re-parse last N rows
#   !checklast <Kingdom>                   quick sanity check for troop keys
# ------------------------------------------------------------------
# Setup:
# 1) pip install -U discord.py python-dotenv
# 2) Create .env file with: DISCORD_TOKEN=your_bot_token
# 3) In Discord Dev Portal, enable MESSAGE CONTENT INTENT
# 4) Run: py .\kg2bot.py

import os
import re
import json
import io
import asyncio
import sqlite3
from math import ceil
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple, List

import discord
from discord.ext import commands
from dotenv import load_dotenv

# Load .env for local dev
load_dotenv()

# Paths & tokens
DB_PATH = os.getenv("DB_PATH", "kg2_reports.sqlite3")  # default local file
TOKEN = os.getenv("DISCORD_TOKEN")

# Connect to SQLite
conn = sqlite3.connect(DB_PATH)
conn.execute("PRAGMA journal_mode=WAL;")  # optional but recommended

# ---------- Database (tables & indexes) ----------
SCHEMA = """
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
  resources_json TEXT,
  troops_json TEXT,
  movements_json TEXT,
  markets_json TEXT,
  tech_json TEXT,
  defense_power INTEGER,
  captured_at TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  author_id TEXT,
  raw TEXT
);
CREATE INDEX IF NOT EXISTS idx_spy_kingdom_captured
  ON spy_reports(kingdom, captured_at DESC);

CREATE TABLE IF NOT EXISTS channel_settings (
  guild_id TEXT,
  channel_id TEXT,
  autocapture INTEGER DEFAULT 0,
  default_kingdom TEXT,
  PRIMARY KEY (guild_id, channel_id)
);
"""
conn.executescript(SCHEMA)
conn.commit()

# ---------- Discord Client ----------
intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
bot = commands.Bot(command_prefix="!", intents=intents)

# ---------- Theme / Emojis ----------
THEME_COLOR = 0x5865F2  # Discord blurple
E = {
    "kingdom": "👑", "alliance": "🛡️", "honour": "🏅", "ranking": "#️⃣",
    "networth": "💰", "castles": "🏰", "dp": "🧮", "resources": "📦",
    "troops": "⚔️", "movement": "🚩", "market": "📈", "tech": "🔧",
}
RES_E = {
    "land": "🗺️", "gold": "🪙", "food": "🍞", "horses": "🐎",
    "stone": "🧱", "blue_gems": "💎", "green_gems": "🟢💎", "wood": "🪵",
}

# ---------- Helpers ----------
def human(n: Optional[float]) -> str:
    if n is None:
        return "-"
    try:
        f = float(n)
        if f.is_integer():
            return f"{int(f):,}"
        s = f"{f:.2f}".rstrip("0").rstrip(".")
        return s
    except Exception:
        return str(n)

def code_block(text: str) -> str:
    return f"```{text}```"

def table_from_dict(d: Dict[str, int], emoji_map: Dict[str, str]) -> str:
    if not d:
        return "-"
    items = []
    for k, v in d.items():
        k2 = k.replace("_", " ").title()
        emo = emoji_map.get(k.lower(), "•")
        items.append((f"{emo} {k2}", human(v)))
    left = max(len(a) for a, _ in items) if items else 0
    lines = [f"{a.ljust(left)}  {b}" for a, b in items]
    out = "\n".join(lines)
    if len(out) > 990:
        out = out[:990] + "\n…"
    return code_block(out)

# ---------- Channel settings ----------
def set_watch(guild_id: int, channel_id: int, on: bool, default_kingdom: Optional[str] = None):
    conn.execute(
        """
        INSERT INTO channel_settings (guild_id, channel_id, autocapture, default_kingdom)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(guild_id, channel_id)
        DO UPDATE SET autocapture=excluded.autocapture,
                      default_kingdom=COALESCE(excluded.default_kingdom, channel_settings.default_kingdom)
        """,
        (str(guild_id), str(channel_id), 1 if on else 0, default_kingdom),
    )
    conn.commit()

def get_watch(guild_id: int, channel_id: int) -> Tuple[bool, Optional[str]]:
    cur = conn.cursor()
    cur.execute(
        "SELECT autocapture, default_kingdom FROM channel_settings WHERE guild_id=? AND channel_id=?",
        (str(guild_id), str(channel_id)),
    )
    row = cur.fetchone()
    if not row:
        return False, None
    return bool(row[0]), row[1]

# ---------- Parsing ----------
pending_add: Dict[Tuple[int, int, int], Tuple[str, float]] = {}
ADD_TIMEOUT_SEC = 180

def looks_like_spy_report(text: str) -> bool:
    t = text.lower()
    if len(text) < 50:
        return False
    signals = [
        "target:", "spyreport was captured on", "spy report was captured on",
        "our spies also found", "approximate defensive power"
    ]
    return sum(1 for s in signals if s in t) >= 2

FIELD_PAT = re.compile(
    r"^(?P<k>Target|Alliance|Honour|Ranking|Networth|Spies Sent|Spies Lost|Result Level|Number of Castles|Approximate defensive power\*?)\s*[:：]\s*(?P<v>.+)$",
    re.I
)
NUM_PAT = re.compile(r"[-+]?\d+(?:\.\d+)?")

RESOURCE_KEYS = {
    "Land": "land", "Gold": "gold", "Food": "food", "Horses": "horses",
    "Stone": "stone", "Blue Gems": "blue_gems", "Green Gems": "green_gems", "Wood": "wood",
}

# allow "UnitName: 1,234" counts
TROOP_LINE_PAT = re.compile(r"^(?P<name>[A-Za-z ]+):\s*(?P<count>[-+]?\d[\d,]*)$", re.I)

CAPTURED_PAT = re.compile(r"(Spy\s*Report|SpyReport)\s+was\s+captured\s+on[:：\u2022\-\s]*([^\n]+)", re.I)
CAPTURED_PAT_ALT = re.compile(r"^Received[:：\-\s]*([^\n]+)$", re.I | re.M)

# ---- Robust multi-header matching (handles straight/curly quotes and short forms)
HEADERS = {
    "resources": [
        "Our spies also found the following information about the kingdom's resources",
        "Our spies also found the following information about the kingdom’s resources",
        "The following information was found regarding the kingdom's resources",
    ],
    "troops": [
        "Our spies also found the following information about the kingdom's troops",
        "Our spies also found the following information about the kingdom’s troops",
        "The following information was found regarding the kingdom's troops",
        "Troops discovered",
    ],
    "movements": [
        "The following information was found regarding troop movements",
        "The following information was found regarding the enemy's troop movements",
    ],
    "markets": [
        "The following recent market transactions were also discovered",
        "Recent market transactions",
    ],
    "tech": [
        "The following technology information was also discovered",
        "Technology information discovered",
    ],
}

def _find_any(haystack_lc: str, needles: List[str]) -> int:
    best = -1
    for n in needles:
        i = haystack_lc.find(n.lower())
        if i != -1 and (best == -1 or i < best):
            best = i
    return best

def extract_section(block: str, section_key: str) -> Optional[str]:
    hay = block
    hay_lc = hay.lower()
    starts = _find_any(hay_lc, HEADERS[section_key])
    if starts == -1:
        return None
    tail = hay[starts:]
    tail_lc = tail.lower()

    other_headers: List[str] = []
    for k, arr in HEADERS.items():
        if k != section_key:
            other_headers.extend(arr)
    other_headers.extend([
        "Target:", "KG2Bot", "The following information", "SpyReport was captured on",
        "Spy Report was captured on", "Received:", "Approximate defensive power"
    ])

    end_pos = len(tail)
    for h in other_headers:
        j = tail_lc.find(h.lower())
        if j != -1 and 0 < j < end_pos:
            end_pos = j
    return tail[:end_pos]

def parse_resources(section: str) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for line in section.splitlines():
        for k, key in RESOURCE_KEYS.items():
            if line.lower().startswith(k.lower()):
                nums = NUM_PAT.findall(line.replace(",", ""))
                if nums:
                    try:
                        out[key] = int(float(nums[0]))
                    except Exception:
                        pass
    return out

def parse_troops(section: str) -> Tuple[Dict[str, int], Optional[int]]:
    troops: Dict[str, int] = {}
    defense_power: Optional[int] = None
    for line in section.splitlines():
        s = line.strip()
        m = TROOP_LINE_PAT.match(s)
        if m:
            name = m.group("name").strip().title()
            count_txt = m.group("count").replace(",", "")
            count = int(float(count_txt))
            troops[name] = count
        if "Approximate defensive power" in s:
            nums = re.findall(r"\d+", s.replace(",", ""))
            if nums:
                defense_power = int(nums[0])
    return troops, defense_power

def parse_troops_globally(raw: str) -> Dict[str, int]:
    """Fallback: scan whole raw text for 'Unit: NNN' lines (skip Population)."""
    troops: Dict[str, int] = {}
    for line in raw.splitlines():
        s = line.strip()
        m = TROOP_LINE_PAT.match(s)
        if not m:
            continue
        name = m.group("name").strip().title()
        if name.lower().startswith("population"):
            continue
        count_txt = m.group("count").replace(",", "")
        try:
            troops[name] = int(float(count_txt))
        except Exception:
            pass
    return troops

def parse_bullets(section: str) -> list:
    out = []
    for line in section.splitlines():
        line = line.strip(" -*•")
        if not line:
            continue
        if any(w in line.lower() for w in ["bought", "sold", "attacked", "launched", " by "]):
            out.append(line)
    return out

def parse_datetime_fuzzy(s: str) -> Optional[str]:
    s = s.strip()
    fmts = [
        "%m/%d/%Y %I:%M %p", "%m/%d/%Y %H:%M",
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
        "%d/%m/%Y %H:%M", "%Y/%m/%d %H:%M:%S",
        "%b %d, %Y, %I:%M:%S %p", "%b %d, %Y %I:%M:%S %p",
        "%b %d, %Y, %I:%M %p", "%b %d, %Y %I:%M %p",
    ]
    for f in fmts:
        try:
            return datetime.strptime(s, f).isoformat()
        except Exception:
            pass
    try:
        return datetime.fromisoformat(s.replace("[mytime]", "").replace("[/mytime]", "").strip()).isoformat()
    except Exception:
        return None

def parse_spy_report(raw: str) -> Dict[str, Any]:
    data: Dict[str, Any] = {
        "kingdom": None, "alliance": None, "honour": None, "ranking": None,
        "networth": None, "spies_sent": None, "spies_lost": None, "result_level": None,
        "castles": None, "resources": {}, "troops": {}, "movements": [], "markets": [],
        "tech": [], "defense_power": None, "captured_at": None
    }
    # Key: value lines
    for line in raw.splitlines():
        m = FIELD_PAT.match(line.strip())
        if not m:
            continue
        k = m.group("k").lower()
        v = m.group("v").strip()
        nums = NUM_PAT.findall(v.replace(",", ""))
        if k.startswith("target"):
            data["kingdom"] = v
        elif k.startswith("alliance"):
            v = re.sub(r"\[/?url[^\]]*\]", "", v)
            data["alliance"] = v.strip()
        elif k.startswith("honour"):
            data["honour"] = float(nums[0]) if nums else None
        elif k.startswith("ranking"):
            data["ranking"] = int(float(nums[0])) if nums else None
        elif k.startswith("networth"):
            data["networth"] = int(float(nums[0])) if nums else None
        elif k.startswith("spies sent"):
            data["spies_sent"] = int(float(nums[0])) if nums else None
        elif k.startswith("spies lost"):
            data["spies_lost"] = int(float(nums[0])) if nums else None
        elif k.startswith("result level"):
            data["result_level"] = v
        elif k.startswith("number of castles"):
            data["castles"] = int(float(nums[0])) if nums else None
        elif k.startswith("approximate defensive power"):
            data["defense_power"] = int(float(nums[0])) if nums else None

    # Sections (robust)
    res_s  = extract_section(raw, "resources")
    trp_s  = extract_section(raw, "troops")
    mov_s  = extract_section(raw, "movements")
    mkt_s  = extract_section(raw, "markets")
    tech_s = extract_section(raw, "tech")

    if res_s:
        data["resources"] = parse_resources(res_s)
    if trp_s:
        troops, dp = parse_troops(trp_s)
        data["troops"] = troops
        if dp and not data["defense_power"]:
            data["defense_power"] = dp
    if mov_s:
        data["movements"] = parse_bullets(mov_s)
    if mkt_s:
        data["markets"] = parse_bullets(mkt_s)
    if tech_s:
        techs: List[str] = []
        for line in tech_s.splitlines():
            line = line.strip(" -*•")
            if not line:
                continue
            if re.search(r"lvl\s*\d+", line, re.I) or len(line.split()) <= 6:
                techs.append(line)
        data["tech"] = techs

    # Fallback if troops were missed by section parsing
    if not data["troops"]:
        data["troops"] = parse_troops_globally(raw)

    # Captured time
    m = CAPTURED_PAT.search(raw)
    if m:
        ts = parse_datetime_fuzzy(m.group(2))
        data["captured_at"] = ts
    if not data["captured_at"]:
        m2 = CAPTURED_PAT_ALT.search(raw)
        if m2:
            ts = parse_datetime_fuzzy(m2.group(1))
            data["captured_at"] = ts
    if not data["captured_at"]:
        data["captured_at"] = datetime.now(timezone.utc).isoformat()

    # Fallback kingdom detection ("Kingdom: X")
    if not data["kingdom"]:
        km = re.search(r"^\s*Kingdom\s*:\s*(.+)$", raw, re.I | re.M)
        if km:
            data["kingdom"] = km.group(1).strip()
    return data

# ---------- Storage ----------
def save_report(author_id: int, raw: str) -> Tuple[Optional[int], Optional[str]]:
    data = parse_spy_report(raw)
    if not data.get("kingdom"):
        return None, None
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO spy_reports (
          kingdom, alliance, honour, ranking, networth, spies_sent, spies_lost,
          result_level, castles, resources_json, troops_json, movements_json,
          markets_json, tech_json, defense_power, captured_at, author_id, raw
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            data.get("kingdom"), data.get("alliance"), data.get("honour"), data.get("ranking"),
            data.get("networth"), data.get("spies_sent"), data.get("spies_lost"),
            data.get("result_level"), data.get("castles"),
            json.dumps(data.get("resources")), json.dumps(data.get("troops")),
            json.dumps(data.get("movements")), json.dumps(data.get("markets")),
            json.dumps(data.get("tech")), data.get("defense_power"),
            data.get("captured_at"), str(author_id), raw
        ),
    )
    conn.commit()
    return cur.lastrowid, data.get("kingdom")

def fetch_latest(kingdom: str) -> Optional[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM spy_reports WHERE UPPER(kingdom)=UPPER(?) ORDER BY datetime(captured_at) DESC, id DESC LIMIT 1",
        (kingdom,),
    )
    return cur.fetchone()

def fetch_last_n(kingdom: str, n: int):
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM spy_reports WHERE UPPER(kingdom)=UPPER(?) ORDER BY datetime(captured_at) DESC, id DESC LIMIT ?",
        (kingdom, n),
    )
    return cur.fetchall()

def fetch_by_id(kingdom: str, rid: int):
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM spy_reports WHERE id=? AND UPPER(kingdom)=UPPER(?) LIMIT 1",
        (rid, kingdom),
    )
    return cur.fetchone()

# ---------- AP & Tips ----------
def ap_breakdown(defense_points: int) -> Dict[str, int]:
    def r(x: float) -> int: return int(round(x))
    return {
        "Minor Victory": r(defense_points * 1.20),
        "Victory":       r(defense_points * 1.55),
        "Major Victory": r(defense_points * 2.20),
        "Overwhelming":  r(defense_points * 8.00),
    }

def castle_bonus_percent(castles: int) -> float:
    """Castle DP bonus = sqrt(castles) / 100 (e.g., 10 castles ~ +3.16%)."""
    try:
        c = int(castles or 0)
        return (c ** 0.5) / 100.0
    except Exception:
        return 0.0

def cav_needed_vs_pike(troops: Dict[str, int], hits: int) -> str:
    """
    Deny Pike bonus (25% rule): send Cav > 4 * defender_pike.
    Shows: their Pike; omits attacker cav (unknown).
    """
    if not troops or not isinstance(troops, dict):
        return "No troop counts found in the last report."
    norm = {(k or "").strip().lower(): v for k, v in troops.items() if k is not None}
    pike_aliases = ["pikemen", "pikeman", "pike", "pikes"]

    pike = 0
    for name, val in norm.items():
        if any(alias in name for alias in pike_aliases):
            try:
                pike += int(val)
            except Exception:
                pass

    msg = [f"Their Pike: **{human(pike)}**"]
    if pike <= 0:
        msg.append("No Pike in the last report; nothing to deny.")
        return "\n".join(msg)

    needed_cav = 4 * pike + 1
    if hits > 0:
        per_hit = ceil(needed_cav / hits)
        msg.append(f"To deny Pike bonus, send **{human(needed_cav)}** Cavalry (≈{human(per_hit)}/hit).")
    else:
        msg.append(f"To deny Pike bonus, send **{human(needed_cav)}** Cavalry.")
    return "\n".join(msg)

# ---------- Attack report parsing & DP estimate ----------
ATTACK_HDR_PAT = re.compile(r"^Attack Report:\s*(?P<kingdom>.+?)\s*\(NW:\s*[+\-]?\s*[\d,]+\)\s*$", re.I | re.M)
# Accept: Stalemate | Minor Victory | Victory | Major Victory | Overwhelming Victory | Overwhelming
ATTACK_RESULT_PAT = re.compile(
    r"^Attack Result:\s*(?P<result>Stalemate|Minor Victory|Victory|Major Victory|Overwhelming(?:\s+Victory)?)\s*$",
    re.I | re.M
)
ATTACK_LOOT_LAND_PAT = re.compile(r"gained.*?\b(?P<land>\d[\d,]*)\s+Land\b", re.I)
ATTACK_CASUALTY_PAT = re.compile(r"casualties.*?:\s*(?P<lost>\d[\d,]*)\s*/\s*(?P<sent>\d[\d,]*)\s+(?P<unit>[A-Za-z ]+)", re.I)

def looks_like_attack_report(text: str) -> bool:
    t = text.strip()
    if len(t) < 40:
        return False
    return ("Attack Report:" in t and "Attack Result:" in t)

def parse_attack_report(raw: str) -> Optional[Dict[str, Any]]:
    m_k = ATTACK_HDR_PAT.search(raw)
    m_r = ATTACK_RESULT_PAT.search(raw)
    if not (m_k and m_r):
        return None
    kingdom = m_k.group("kingdom").strip()
    result_raw = m_r.group("result").strip().lower()
    # Normalize result tiers
    if result_raw.startswith("stalemate"):
        result = "Stalemate"
    elif result_raw.startswith("minor victory"):
        result = "Minor Victory"
    elif result_raw == "victory":
        result = "Victory"
    elif result_raw.startswith("major victory"):
        result = "Major Victory"
    elif result_raw.startswith("overwhelming"):
        result = "Overwhelming Victory"
    else:
        result = "Victory"  # sensible default

    land = 0
    m_land = ATTACK_LOOT_LAND_PAT.search(raw)
    if m_land:
        try:
            land = int(m_land.group("land").replace(",", ""))
        except Exception:
            land = 0
    lost = sent = None
    unit = None
    m_cas = ATTACK_CASUALTY_PAT.search(raw)
    if m_cas:
        try:
            lost = int(m_cas.group("lost").replace(",", ""))
            sent = int(m_cas.group("sent").replace(",", ""))
        except Exception:
            lost = sent = None
        unit = m_cas.group("unit").strip().title()
    return {"kingdom": kingdom, "result": result, "land": land, "att_lost": lost, "att_sent": sent, "att_unit": unit}

# Base defender loss % per tier (tune to your meta)
DEFENDER_LOSS_BASE = {
    "Stalemate":            0.02,  # smallest
    "Minor Victory":        0.04,
    "Victory":              0.08,
    "Major Victory":        0.13,
    "Overwhelming Victory": 0.22,  # largest
}
# Heuristic modifiers (casualties & land taken)
ALPHA_CASUALTY = 0.30      # scales with (att_lost / att_sent)
BETA_LAND      = 0.10      # scales with (land / 1000), capped to 1

def estimate_loss_pct(result_tier: str, att_lost: Optional[int], att_sent: Optional[int], land_gained: int) -> float:
    base = DEFENDER_LOSS_BASE.get(result_tier, 0.08)
    cas_ratio = 0.0
    if att_lost is not None and att_sent and att_sent > 0:
        cas_ratio = max(0.0, min(1.0, att_lost / att_sent))
    land_factor = max(0.0, min(1.0, land_gained / 1000.0))
    loss = base + ALPHA_CASUALTY * cas_ratio + BETA_LAND * land_factor
    return max(0.01, min(loss, 0.45))  # clamp to sane range

def estimate_dp_after_one_hit(base_dp: int, result_tier: str, att_lost: Optional[int], att_sent: Optional[int], land_gained: int) -> int:
    pct = estimate_loss_pct(result_tier, att_lost, att_sent, land_gained)
    return max(0, ceil(base_dp * (1.0 - pct)))

def build_dp_after_hit_embed(kingdom: str, base_dp: int, castles: Optional[int], result: str, land_gained: int, att_lost: Optional[int], att_sent: Optional[int]) -> discord.Embed:
    c_bonus = castle_bonus_percent(castles or 0)
    dp_with_castles = ceil(base_dp * (1.0 + c_bonus))
    dp_after = estimate_dp_after_one_hit(base_dp, result, att_lost, att_sent, land_gained)
    dp_after_with_castles = ceil(dp_after * (1.0 + c_bonus))
    est_pct = int(round((1 - dp_after / base_dp) * 100)) if base_dp > 0 else 0

    e = discord.Embed(title=f"AP Update • {kingdom}", color=THEME_COLOR, description="Estimated defender DP after 1 hit")
    e.add_field(name="🧮 Previous Base DP", value=human(base_dp), inline=True)
    e.add_field(name="🏰 Previous With Castles", value=human(dp_with_castles), inline=True)
    e.add_field(name="⚔️ Result Tier", value=result, inline=True)

    e.add_field(name="🧮 Est. Base DP (after 1 hit)", value=f"{human(dp_after)} (−{est_pct}%)", inline=True)
    e.add_field(name="🏰 Est. With Castles", value=f"{human(dp_after_with_castles)} (+{round(c_bonus*100):.0f}% castles)", inline=True)
    e.add_field(name="🗺️ Land Gained (attacker)", value=human(land_gained), inline=True)

    if att_lost is not None and att_sent:
        e.add_field(name="🪖 Attacker Casualties", value=f"{human(att_lost)}/{human(att_sent)}", inline=True)

    e.set_footer(text="Heuristic estimate using tier, attacker casualties, and land gained. Actual losses vary.")
    return e

# ---------- Embeds ----------
def fmt_embed_from_row(row: sqlite3.Row) -> discord.Embed:
    res = json.loads(row["resources_json"]) if row["resources_json"] else {}
    troops = json.loads(row["troops_json"]) if row["troops_json"] else {}
    mov = json.loads(row["movements_json"]) if row["movements_json"] else []
    mkt = json.loads(row["markets_json"]) if row["markets_json"] else []
    tech = json.loads(row["tech_json"]) if row["tech_json"] else []

    embed = discord.Embed(color=THEME_COLOR)
    embed.title = f"{E['kingdom']} {row['kingdom']}"
    embed.add_field(name=f"{E['alliance']} Alliance", value=row["alliance"] or "-", inline=True)
    embed.add_field(name=f"{E['honour']} Honour", value=human(row["honour"]) if row["honour"] is not None else "-", inline=True)
    embed.add_field(name=f"{E['ranking']} Ranking", value=human(row["ranking"]) if row["ranking"] is not None else "-", inline=True)
    embed.add_field(name=f"{E['networth']} Networth", value=human(row["networth"]), inline=True)
    embed.add_field(name="🕵️ Spies", value=f"{human(row['spies_sent'])}/{human(row['spies_lost'])}", inline=True)
    embed.add_field(name="📜 Result", value=row["result_level"] or "-", inline=True)
    embed.add_field(name=f"{E['castles']} Castles", value=human(row["castles"]) if row["castles"] is not None else "-", inline=True)

    if row["defense_power"] is not None:
        embed.add_field(name=f"{E['dp']} Def. Power", value=human(row["defense_power"]), inline=True)

    if troops:
        embed.add_field(name=f"{E['troops']} Troops", value=table_from_dict({k.lower(): v for k, v in troops.items()}, {}), inline=False)
    if res:
        embed.add_field(name=f"{E['resources']} Resources", value=table_from_dict(res, RES_E), inline=False)
    if mov:
        mv = "\n".join(mov[:5])
        embed.add_field(name=f"{E['movement']} Recent Activity", value=(mv[:1010] + "…" if len(mv)>1010 else mv), inline=False)
    if mkt:
        mk = "\n".join(mkt[:5])
        embed.add_field(name=f"{E['market']} Market", value=(mk[:1010] + "…" if len(mk)>1010 else mk), inline=False)
    if tech:
        tc = ", ".join(tech)
        embed.add_field(name=f"{E['tech']} Tech", value=(tc[:1010] + "…" if len(tc)>1010 else tc), inline=False)

    try:
        ts = datetime.fromisoformat(row["captured_at"]).strftime("%Y-%m-%d %H:%M")
    except Exception:
        ts = row["captured_at"]
    try:
        embed.set_thumbnail(url=bot.user.display_avatar.url)  # type: ignore
    except Exception:
        pass
    embed.set_footer(text=f"Captured: {ts} • ID {row['id']}")
    return embed

# ---------- Help ----------
def _build_help_embed() -> discord.Embed:
    e = discord.Embed(title="KG2 Recon • Commands", color=THEME_COLOR)
    e.add_field(
        name="Save + Watch",
        value=(
            "`!watchhere on [Kingdom]` — auto-capture this channel\n"
            "`!watchhere off` — disable in this channel\n"
            "`!watchall on [Kingdom]` — server-wide auto-capture (admin)\n"
            "`!watchall off` — disable server-wide (admin)\n"
            "`!savereport [link]` — manual save if needed"
        ),
        inline=False,
    )
    e.add_field(
        name="Lookup + Plan",
        value=(
            "`!spy <Kingdom>` — latest embed with DP, troops, activity\n"
            "`!ap <Kingdom> [hits]` — AP totals (castle bonus) + Cav-vs-Pike tip\n"
            "`!spyhistory <Kingdom> [N]` — last N reports\n"
            "`!spyid <Kingdom> <ID>` — open specific report"
        ),
        inline=False,
    )
    e.add_field(
        name="Repair & Debug",
        value=(
            "`!rescanlast <Kingdom>` — re-parse latest raw row (fix missing troops)\n"
            "`!rescanrange <Kingdom> [N]` — re-parse last N rows\n"
            "`!checklast <Kingdom>` — show troop keys/pike/DP seen by !ap"
        ),
        inline=False,
    )
    e.add_field(
        name="Export",
        value="`!exportspy <Kingdom> [N]` — export JSON (admin)",
        inline=False,
    )
    return e

@bot.command(name="kg2help", aliases=["commands"])
async def show_commands(ctx: commands.Context):
    await ctx.send(embed=_build_help_embed())

# ---------- Commands ----------
@bot.command(name="watchhere")
@commands.has_permissions(manage_guild=True)
async def watchhere(ctx: commands.Context, state: str, *, default_kingdom: Optional[str] = None):
    state_l = state.lower()
    if state_l not in ("on", "off"):
        await ctx.send("Usage: `!watchhere on [DefaultKingdom]` or `!watchhere off`")
        return
    set_watch(ctx.guild.id, ctx.channel.id, state_l == "on", default_kingdom)
    on, dk = get_watch(ctx.guild.id, ctx.channel.id)
    msg = f"Auto-capture is now **{'ON' if on else 'OFF'}** here."
    if dk:
        msg += f" Default kingdom: **{dk}**."
    await ctx.send(msg)

@bot.command(name="watchall")
@commands.has_permissions(manage_guild=True)
async def watchall(ctx: commands.Context, state: str, *, default_kingdom: Optional[str] = None):
    state_l = state.lower()
    if state_l not in ("on", "off"):
        await ctx.send("Usage: `!watchall on [DefaultKingdom]` or `!watchall off`")
        return
    toggled = 0
    for ch in ctx.guild.text_channels:
        try:
            set_watch(ctx.guild.id, ch.id, state_l == "on", default_kingdom)
            toggled += 1
        except Exception:
            pass
    await ctx.send(
        f"Auto-capture {'ENABLED' if state_l=='on' else 'DISABLED'} in {toggled} text channels."
        + (f" Default kingdom: **{default_kingdom}**." if default_kingdom else "")
    )

@bot.command(name="savereport")
@commands.has_permissions(manage_messages=True)
async def savereport(ctx: commands.Context, message_link: Optional[str] = None):
    target_message: Optional[discord.Message] = None
    if message_link:
        parts = message_link.strip().split('/')
        if len(parts) >= 3 and parts[-3].isdigit() and parts[-2].isdigit() and parts[-1].isdigit():
            g_id, ch_id, msg_id = map(int, parts[-3:])
        else:
            await ctx.send("That doesn't look like a valid message link.")
            return
        if g_id != ctx.guild.id:
            await ctx.send("That message link is for a different server.")
            return
        ch = ctx.guild.get_channel(ch_id)
        if not isinstance(ch, discord.TextChannel):
            await ctx.send("Couldn't access that channel.")
            return
        try:
            target_message = await ch.fetch_message(msg_id)
        except Exception:
            await ctx.send("Couldn't fetch that message (missing perms?).")
            return
    else:
        async for m in ctx.channel.history(limit=5, before=ctx.message):
            if not m.author.bot and looks_like_spy_report(m.content):
                target_message = m
                break
        if not target_message:
            await ctx.send("Couldn't find a report above. Or pass a link: `!savereport <link>`.")
            return

    raw = target_message.content
    report_id, parsed_kingdom = save_report(ctx.author.id, raw)

    if not report_id:
        on, dk = get_watch(ctx.guild.id, ctx.channel.id)
        if dk:
            injected = f"Target: {dk}\n" + raw
            report_id, parsed_kingdom = save_report(ctx.author.id, injected)

    if not report_id:
        await ctx.send("Could not parse that report (missing Target/Kingdom). Consider `!watchhere on <Kingdom>`.")
        return

    row = fetch_latest(parsed_kingdom)
    await ctx.send("Report saved via `!savereport`.", embed=fmt_embed_from_row(row))

@bot.command(name="addspy")
@commands.has_permissions(manage_guild=True)
async def addspy(ctx: commands.Context, *, kingdom: str):
    key = (ctx.guild.id, ctx.channel.id, ctx.author.id)
    deadline = asyncio.get_event_loop().time() + ADD_TIMEOUT_SEC
    pending_add[key] = (kingdom, deadline)
    await ctx.send(f"Paste the spy report for **{kingdom}** in your next message (within 3 minutes). Use the same channel.")

@bot.command(name="spy")
async def spy(ctx: commands.Context, *, kingdom: str):
    row = fetch_latest(kingdom)
    if not row:
        await ctx.send(f"No spy reports found for **{kingdom}**.")
        return
    await ctx.send(embed=fmt_embed_from_row(row))

@bot.command(name="ap")
async def ap(ctx: commands.Context, *, args: str = ""):
    """
    Usage: !ap <kingdom> [hits]
    If run with no args, will try this channel's default_kingdom.
    Hits default to 0 (no attacks posted yet).
    """
    parts = args.split()
    hits = 0  # default 0

    if not parts:
        on, dk = get_watch(ctx.guild.id, ctx.channel.id)
        if not dk:
            await ctx.send("Usage: `!ap <kingdom> [hits]` (or set a default with `!watchhere on <Kingdom>`)")
            return
        kingdom = dk
    else:
        if parts[-1].isdigit():
            hits = max(0, min(10, int(parts[-1])))
            kingdom = " ".join(parts[:-1]) or parts[0]
        else:
            kingdom = " ".join(parts)

    row = fetch_latest(kingdom)
    if not row:
        await ctx.send(f"No spy reports found for **{kingdom}**.")
        return
    dp = row["defense_power"]
    if dp is None:
        await ctx.send("This report has no defensive power. Re-add with a report that includes it.")
        return

    castles = row["castles"] if "castles" in row.keys() else None
    c_bonus = castle_bonus_percent(castles or 0)
    dp_with_bonus = ceil(int(dp) * (1.0 + c_bonus))

    br = ap_breakdown(int(dp_with_bonus))

    embed = discord.Embed(title=f"AP Planner • {row['kingdom']}", color=THEME_COLOR)
    embed.add_field(name="🧮 Base DP", value=human(dp), inline=True)
    if castles is not None:
        pct_txt = f"+{round(c_bonus*100):.0f}%"
        embed.add_field(name="🏰 With Castle Bonus", value=f"{human(dp_with_bonus)} ({pct_txt})", inline=True)
    else:
        embed.add_field(name="🏰 With Castle Bonus", value=f"{human(dp_with_bonus)}", inline=True)
    embed.add_field(name="🔢 Hits", value=str(hits), inline=True)

    def fmt(ap_total: int) -> str:
        if hits <= 1:
            return f"Total={human(ap_total)}"
        per = ceil(ap_total / hits)
        return f"Total={human(ap_total)} | Per-hit≈{human(per)}"

    embed.add_field(name="🟩 Minor Victory", value=fmt(br["Minor Victory"]), inline=False)
    embed.add_field(name="🟦 Victory",       value=fmt(br["Victory"]),       inline=False)
    embed.add_field(name="🟨 Major Victory", value=fmt(br["Major Victory"]), inline=False)
    embed.add_field(name="🟥 Overwhelming",  value=fmt(br["Overwhelming"]),  inline=False)

    # Cav vs Pike tip — ALWAYS SHOW (robust + length safety)
    def load_troops(r: sqlite3.Row) -> Dict[str, int]:
        try:
            t = json.loads(r["troops_json"]) if r["troops_json"] else {}
            return t if isinstance(t, dict) else {}
        except Exception:
            return {}
    troops = load_troops(row)
    if not troops:
        for older in fetch_last_n(kingdom, 5)[1:]:
            troops = load_troops(older)
            if troops:
                break

    tip = cav_needed_vs_pike(troops, hits)
    if len(tip) > 1000:
        tip = tip[:1000] + "…"
    embed.add_field(name="🐎 Cav vs Pike", value=tip, inline=False)

    try:
        ts = datetime.fromisoformat(row["captured_at"]).strftime("%Y-%m-%d %H:%M")
    except Exception:
        ts = row["captured_at"]
    foot = f"Based on spy report: {ts}"
    if castles is not None:
        foot += f" • Castles: {castles} • +{round(c_bonus*100):.0f}% castle bonus"
    embed.set_footer(text=foot)

    try:
        embed.set_thumbnail(url=bot.user.display_avatar.url)  # type: ignore
    except Exception:
        pass

    await ctx.send(embed=embed)

@bot.command(name="spyhistory")
async def spyhistory(ctx: commands.Context, kingdom: str, n: int = 5):
    n = max(1, min(n, 50))
    rows = fetch_last_n(kingdom, n)
    if not rows:
        await ctx.send(f"No spy reports found for **{kingdom}**.")
        return
    lines = []
    for r in rows:
        try:
            ts = datetime.fromisoformat(r["captured_at"]).strftime("%Y-%m-%d %H:%M")
        except Exception:
            ts = r["captured_at"]
        dp = r["defense_power"] if r["defense_power"] is not None else "-"
        net = r["networth"] if r["networth"] is not None else "-"
        rank = r["ranking"] if r["ranking"] is not None else "-"
        lines.append(f"[ID {r['id']}] {ts} | DP:{dp} | Net:{net} | Rank:{rank}")
    await ctx.send(f"Latest {len(rows)} report(s) for **{kingdom}**:\n" + "\n".join(lines))

@bot.command(name="spyid")
async def spyid(ctx: commands.Context, kingdom: str, report_id: int):
    row = fetch_by_id(kingdom, report_id)
    if not row:
        await ctx.send(f"No report with ID {report_id} for **{kingdom}**.")
        return
    await ctx.send(embed=fmt_embed_from_row(row))

@bot.command(name="exportspy")
@commands.has_permissions(manage_guild=True)
async def exportspy(ctx: commands.Context, kingdom: str, n: int = 0):
    cur = conn.cursor()
    if n and n > 0:
        cur.execute(
            "SELECT * FROM spy_reports WHERE UPPER(kingdom)=UPPER(?) ORDER BY datetime(captured_at) DESC, id DESC LIMIT ?",
            (kingdom, n),
        )
    else:
        cur.execute(
            "SELECT * FROM spy_reports WHERE UPPER(kingdom)=UPPER(?) ORDER BY datetime(captured_at) DESC, id DESC",
            (kingdom,),
        )
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    payload = [{cols[i]: r[i] for i in range(len(cols))} for r in rows]
    buf = io.BytesIO(json.dumps(payload, indent=2).encode("utf-8"))
    filename = f"{kingdom}_spy_archive_{len(payload)}.json"
    await ctx.send(file=discord.File(buf, filename))

# ---------- Repair / Debug Helpers ----------
@bot.command(name="rescanlast")
@commands.has_permissions(manage_messages=True)
async def rescanlast(ctx: commands.Context, *, kingdom: str):
    """Re-parse the latest saved report for this kingdom and update troops/DP in DB."""
    row = fetch_latest(kingdom)
    if not row:
        await ctx.send(f"No spy reports found for **{kingdom}**.")
        return
    raw = row["raw"]
    if not raw:
        await ctx.send("Latest row has no raw text stored; try saving a fresh report.")
        return

    parsed = parse_spy_report(raw)
    troops = parsed.get("troops") or {}
    dp = parsed.get("defense_power")

    cur = conn.cursor()
    cur.execute(
        "UPDATE spy_reports SET troops_json=?, defense_power=? WHERE id=?",
        (json.dumps(troops), dp if dp is not None else row["defense_power"], row["id"]),
    )
    conn.commit()

    pike = 0
    for k, v in (troops or {}).items():
        if any(pt in (k or "").strip().lower() for pt in ["pikemen", "pikeman", "pike", "pikes"]):
            try:
                pike += int(v)
            except Exception:
                pass
    await ctx.send(
        f"Rescanned ID {row['id']} for **{kingdom}**. "
        f"Troops keys: {len(troops)} • Pike: {pike} • DP: {parsed.get('defense_power') or row['defense_power']}"
    )

@bot.command(name="rescanrange")
@commands.has_permissions(manage_messages=True)
async def rescanrange(ctx: commands.Context, kingdom: str, n: int = 5):
    """Re-parse the last N reports for this kingdom and update troops/DP."""
    rows = fetch_last_n(kingdom, max(1, min(n, 50)))
    if not rows:
        await ctx.send(f"No spy reports found for **{kingdom}**.")
        return
    updated = 0
    for r in rows:
        raw = r["raw"]
        if not raw:
            continue
        parsed = parse_spy_report(raw)
        troops = parsed.get("troops") or {}
        dp = parsed.get("defense_power")
        if troops or dp:
            cur = conn.cursor()
            cur.execute(
                "UPDATE spy_reports SET troops_json=?, defense_power=? WHERE id=?",
                (json.dumps(troops), dp if dp is not None else r["defense_power"], r["id"]),
            )
            updated += 1
    conn.commit()
    await ctx.send(f"Rescanned {len(rows)}; updated {updated} for **{kingdom}**.")

@bot.command(name="checklast")
async def checklast(ctx: commands.Context, *, kingdom: str):
    row = fetch_latest(kingdom)
    if not row:
        await ctx.send(f"No spy reports found for **{kingdom}**.")
        return
    try:
        troops = json.loads(row["troops_json"]) if row["troops_json"] else {}
    except Exception:
        troops = {}
    keys = list(troops.keys())[:12]
    pike = sum(int(v) for k, v in troops.items() if any(pt in k.lower() for pt in ["pikemen","pikeman","pike","pikes"]))
    await ctx.send(f"Latest ID {row['id']} • Troop keys: {keys or 'NONE'} • Pike: {pike or 0} • DP: {row['defense_power'] or '-'}")

# ---------- Events ----------
@bot.event
async def on_ready():
    loaded = ", ".join(sorted(c.name for c in bot.commands))
    print(f"Loaded commands: {loaded}")
    try:
        await bot.change_presence(activity=discord.Game(name="!kg2help • KG2 Recon"))
    except Exception:
        pass
    print(f"Logged in as {bot.user} (ID: {bot.user.id})")

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        return  # silently ignore unknown commands
    raise error

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return
    await bot.process_commands(message)

    if not message.guild:
        return

    # handle pending !addspy
    key = (message.guild.id, message.channel.id, message.author.id)
    if key in pending_add:
        kingdom, deadline = pending_add[key]
        if asyncio.get_event_loop().time() <= deadline:
            raw = message.content
            if len(raw) >= 50:
                report_id, parsed_kingdom = save_report(message.author.id, raw)
                pending_add.pop(key, None)
                if not report_id:
                    await message.channel.send("Could not parse that report (missing Target/Kingdom). Please try again.")
                    return
                display_key = parsed_kingdom or kingdom
                if parsed_kingdom and parsed_kingdom.lower() != kingdom.lower():
                    await message.channel.send(
                        f"Saved report for **{parsed_kingdom}** (you requested **{kingdom}**). Use `!spy {parsed_kingdom}` to view."
                    )
                row = fetch_latest(display_key)
                await message.channel.send("Spy report saved.", embed=fmt_embed_from_row(row))
                return
        else:
            pending_add.pop(key, None)

    # auto-capture spy reports
    watch_on, default_kingdom = get_watch(message.guild.id, message.channel.id)
    if watch_on and looks_like_spy_report(message.content):
        raw = message.content
        report_id, parsed_kingdom = save_report(message.author.id, raw)
        if not report_id and default_kingdom:
            injected = f"Target: {default_kingdom}\n" + raw
            report_id, parsed_kingdom = save_report(message.author.id, injected)
        if report_id:
            row = fetch_latest(parsed_kingdom or (default_kingdom or ""))
            await message.channel.send("Auto-saved spy report for this channel.", embed=fmt_embed_from_row(row))

    # auto-reply to Attack Reports with an estimated defender DP-after-1-hit
    if looks_like_attack_report(message.content):
        ar = parse_attack_report(message.content)
        if ar:
            row = fetch_latest(ar["kingdom"])
            if row and row["defense_power"] is not None:
                base_dp = int(row["defense_power"])
                castles = row["castles"] if "castles" in row.keys() else None
                embed = build_dp_after_hit_embed(
                    kingdom=ar["kingdom"],
                    base_dp=base_dp,
                    castles=castles,
                    result=ar["result"],
                    land_gained=ar["land"] or 0,
                    att_lost=ar["att_lost"],
                    att_sent=ar["att_sent"]
                )
                try:
                    await message.channel.send(embed=embed)
                except Exception:
                    pass

# ---------- Lightweight Tests ----------
def _test_parse_examples():
    example = (
        "Target: Bazic\n"
        "Alliance: NWO-1\n"
        "Honour: 63.74\n"
        "Ranking: 73\n"
        "Networth: 10986\n"
        "Spies Sent: 700\n"
        "Spies Lost: 49\n"
        "Result Level: Complete Infiltration\n"
        "Number of Castles: 10\n\n"
        "Our spies also found the following information about the kingdom's troops:\n"
        "Population: 34149 / 58570\n"
        "Heavy Cavalry: 2,000\n"
        "Archers: 313\n"
        "Pikemen: 167\n"
        "Peasants: 31041\n"
        "Approximate defensive power*: 11586\n"
        "*(without skill/prayer modifiers)\n"
        "SpyReport was captured on: 2019-04-18 13:49:54\n"
    )
    d = parse_spy_report(example)
    assert d["kingdom"] == "Bazic" and d["defense_power"] == 11586 and d["troops"].get("Pikemen") == 167
    br = ap_breakdown(100)
    assert br["Minor Victory"] == 120 and br["Victory"] == 155 and br["Major Victory"] == 220 and br["Overwhelming"] == 800
    print("Parser/AP tests: OK")

if __name__ == "__main__":
    if os.getenv("RUN_TESTS") == "1":
        _test_parse_examples()
    else:
        if not TOKEN:
            raise SystemExit("DISCORD_TOKEN not found in environment. Set it in .env")
        bot.run(TOKEN)
