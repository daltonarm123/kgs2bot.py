# KG2 Recon Bot (discord.py v2)
# Sleek embeds • Auto-capture • History • AP Planner • Castle bonus • DP chaining • Calc feature
# Commands:
# !kg2help, !watchhere, !watchall, !savereport, !addspy, !spy, !ap, !spyhistory, !spyid, !exportspy, !rescanlast, !rescanrange, !checklast, !calc

import os, re, json, sqlite3, asyncio
from math import ceil
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple, List

import discord
from discord.ext import commands
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")
DB_PATH = os.getenv("DB_PATH", "kg2_reports.sqlite3")

# ---------- Database ----------
conn = sqlite3.connect(DB_PATH)
conn.execute("PRAGMA journal_mode=WAL;")
conn.execute("PRAGMA foreign_keys=ON;")

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

CREATE TABLE IF NOT EXISTS dp_sessions (
    id INTEGER PRIMARY KEY,
    target TEXT NOT NULL,
    spy_report_id INTEGER,
    captured_at TEXT NOT NULL,
    base_dp_start INTEGER NOT NULL,
    castles INTEGER NOT NULL,
    current_base_dp INTEGER NOT NULL,
    current_with_castles INTEGER NOT NULL,
    hits_applied INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (spy_report_id) REFERENCES spy_reports(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_dp_sessions_target_time
ON dp_sessions(target, captured_at DESC);

CREATE TABLE IF NOT EXISTS dp_hits (
    id INTEGER PRIMARY KEY,
    session_id INTEGER NOT NULL,
    hit_at TEXT NOT NULL,
    tier TEXT NOT NULL,
    attacker_casualties INTEGER,
    attacker_army_size INTEGER,
    land_gained INTEGER,
    FOREIGN KEY(session_id) REFERENCES dp_sessions(id) ON DELETE CASCADE
);
"""
conn.executescript(SCHEMA)
conn.commit()

# ---------- Discord Client ----------
intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
bot = commands.Bot(command_prefix="!", intents=intents)

# ---------- Theme & Emojis ----------
THEME_COLOR = 0x5865F2
E = {
    "kingdom": "👑", "alliance": "🛡️", "honour": "🏅", "ranking": "#️⃣",
    "networth": "💰", "castles": "🏰", "dp": "🧮", "resources": "📦",
    "troops": "⚔️", "movement": "🚩", "market": "📈", "tech": "🔧"
}
RES_E = {
    "land": "🗺️", "gold": "🪙", "food": "🍞", "horses": "🐎",
    "stone": "🧱", "blue_gems": "💎", "green_gems": "🟢💎", "wood": "🪵"
}

# ---------- Helpers ----------
def human(n: Optional[float]) -> str:
    if n is None: return "-"
    try:
        f = float(n)
        if f.is_integer(): return f"{int(f):,}"
        return f"{f:.2f}".rstrip("0").rstrip(".")
    except: return str(n)

def code_block(text: str) -> str:
    return f"```{text}```"

def table_from_dict(d: Dict[str, int], emoji_map: Dict[str, str]) -> str:
    if not d: return "-"
    items = [(f"{emoji_map.get(k.lower(),'•')} {k.replace('_',' ').title()}", human(v)) for k,v in d.items()]
    left = max(len(a) for a,_ in items) if items else 0
    lines = [f"{a.ljust(left)}  {b}" for a,b in items]
    out = "\n".join(lines)
    if len(out) > 990: out = out[:990] + "\n…"
    return code_block(out)

# ---------- Help Command ----------
@bot.command(name="kg2help", aliases=["commands"])
async def kg2help(ctx):
    embed = discord.Embed(title="KG2 Recon Bot Commands", color=0x5865F2)
    embed.add_field(name="!kg2help / !commands", value="Shows this help message", inline=False)
    embed.add_field(name="!watchhere on/off [Kingdom]", value="Start/stop watching this channel for spy reports", inline=False)
    embed.add_field(name="!watchall on/off [Kingdom]", value="Admin: watch all channels in the guild", inline=False)
    embed.add_field(name="!savereport [message link]", value="Save a spy report from a message link", inline=False)
    embed.add_field(name="!addspy <Kingdom>", value="Admin: add a spy manually", inline=False)
    embed.add_field(name="!spy <Kingdom>", value="Show last spy report for the kingdom", inline=False)
    embed.add_field(name="!ap <Kingdom> [hits]", value="Calculate attack points for a kingdom", inline=False)
    embed.add_field(name="!spyhistory <Kingdom> [N]", value="Show last N spy reports", inline=False)
    embed.add_field(name="!spyid <Kingdom> <ID>", value="Show spy report by ID", inline=False)
    embed.add_field(name="!exportspy <Kingdom> [N]", value="Admin: export spy report data", inline=False)
    embed.add_field(name="!rescanlast <Kingdom>", value="Admin: re-parse latest raw row", inline=False)
    embed.add_field(name="!rescanrange <Kingdom> [N]", value="Admin: re-parse last N rows", inline=False)
    embed.add_field(name="!checklast <Kingdom>", value="Quick sanity check for troop keys", inline=False)
    embed.add_field(name="!calc", value="Interactive combat calculator against a spy report", inline=False)
    await ctx.send(embed=embed)

# ---------- Channel Settings ----------
def set_watch(guild_id:int, channel_id:int, on:bool, default_kingdom:Optional[str]=None):
    conn.execute("""
    INSERT INTO channel_settings (guild_id, channel_id, autocapture, default_kingdom)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(guild_id, channel_id)
    DO UPDATE SET autocapture=excluded.autocapture,
                  default_kingdom=COALESCE(excluded.default_kingdom, channel_settings.default_kingdom)
    """, (str(guild_id), str(channel_id), 1 if on else 0, default_kingdom))
    conn.commit()

def get_watch(guild_id:int, channel_id:int) -> Tuple[bool, Optional[str]]:
    cur = conn.cursor()
    cur.execute("SELECT autocapture, default_kingdom FROM channel_settings WHERE guild_id=? AND channel_id=?", (str(guild_id), str(channel_id)))
    row = cur.fetchone()
    if not row: return False, None
    return bool(row[0]), row[1]

# ---------- Parsing ----------
FIELD_PAT = re.compile(
    r"^(?P<k>Target|Alliance|Honour|Ranking|Networth|Spies Sent|Spies Lost|Result Level|Number of Castles|Approximate defensive power\*?)\s*[:：]\s*(?P<v>.+)$",
    re.I
)
NUM_PAT = re.compile(r"[-+]?\d+(?:\.\d+)?")
RESOURCE_KEYS = {
    "Land":"land","Gold":"gold","Food":"food","Horses":"horses",
    "Stone":"stone","Blue Gems":"blue_gems","Green Gems":"green_gems","Wood":"wood"
}
TROOP_LINE_PAT = re.compile(r"^(?P<name>[A-Za-z ]+):\s*(?P<count>[-+]?\d[\d,]*)$", re.I)
CAPTURED_PAT = re.compile(r"(Spy\s*Report|SpyReport)\s+was\s+captured\s+on[:：\u2022\-\s]*([^\n]+)", re.I)
CAPTURED_PAT_ALT = re.compile(r"^Received[:：\-\s]*([^\n]+)$", re.I | re.M)

HEADERS = {
    "resources":["Our spies also found the following information about the kingdom's resources","Our spies also found the following information about the kingdom’s resources","The following information was found regarding the kingdom's resources"],
    "troops":["Our spies also found the following information about the kingdom's troops","The following information was found regarding the kingdom's troops","Troops discovered"],
    "movements":["The following information was found regarding troop movements","The following information was found regarding the enemy's troop movements"],
    "markets":["The following recent market transactions were also discovered","Recent market transactions"],
    "tech":["The following technology information was also discovered","Technology information discovered"]
}

def _find_any(haystack_lc:str, needles:List[str]) -> int:
    best = -1
    for n in needles:
        i = haystack_lc.find(n.lower())
        if i != -1 and (best==-1 or i<best): best=i
    return best

def extract_section(block:str, section_key:str) -> Optional[str]:
    hay = block
    starts = _find_any(hay.lower(), HEADERS[section_key])
    if starts==-1: return None
    tail = hay[starts:]
    tail_lc = tail.lower()
    other_headers = []
    for k, arr in HEADERS.items():
        if k != section_key: other_headers.extend(arr)
    other_headers.extend(["Target:","KG2Bot","The following information","SpyReport was captured on","Spy Report was captured on","Received:","Approximate defensive power"])
    end_pos = len(tail)
    for h in other_headers:
        j = tail_lc.find(h.lower())
        if j!=-1 and 0<j<end_pos: end_pos=j
    return tail[:end_pos]

def parse_resources(section:str) -> Dict[str,int]:
    out={}
    for line in section.splitlines():
        for k,key in RESOURCE_KEYS.items():
            if line.lower().startswith(k.lower()):
                nums = NUM_PAT.findall(line.replace(",",""))
                if nums: out[key]=int(float(nums[0]))
    return out

def parse_troops(section:str) -> Tuple[Dict[str,int],Optional[int]]:
    troops={}
    dp=None
    for line in section.splitlines():
        s=line.strip()
        m=TROOP_LINE_PAT.match(s)
        if m:
            name = m.group("name").strip().title()
            count=int(float(m.group("count").replace(",","")))
            troops[name]=count
        if "Approximate defensive power" in s:
            nums=NUM_PAT.findall(s.replace(",",""))
            if nums: dp=int(nums[0])
    return troops, dp

def parse_datetime_fuzzy(s:str) -> Optional[str]:
    s=s.strip()
    fmts=["%m/%d/%Y %I:%M %p","%m/%d/%Y %H:%M","%Y-%m-%d %H:%M:%S","%Y-%m-%d %H:%M","%d/%m/%Y %H:%M","%Y/%m/%d %H:%M:%S","%b %d, %Y, %I:%M:%S %p","%b %d, %Y %I:%M:%S %p","%b %d, %Y, %I:%M %p","%b %d, %Y %I:%M %p"]
    for f in fmts:
        try: return datetime.strptime(s,f).isoformat()
        except: pass
    try: return datetime.fromisoformat(s.replace("[mytime]","").replace("[/mytime]","").strip()).isoformat()
    except: return None

def parse_spy_report(raw:str) -> Dict[str,Any]:
    data={"kingdom":None,"alliance":None,"honour":None,"ranking":None,"networth":None,"spies_sent":None,"spies_lost":None,"result_level":None,"castles":None,"resources":{},"troops":{},"movements":[],"markets":[],"tech":[],"defense_power":None,"captured_at":None}
    for line in raw.splitlines():
        m=FIELD_PAT.match(line.strip())
        if not m: continue
        k,v=m.group("k").lower(),m.group("v").strip()
        nums=NUM_PAT.findall(v.replace(",",""))
        if k.startswith("target"): data["kingdom"]=v
        elif k.startswith("alliance"): data["alliance"]=re.sub(r"\[/?url[^\]]*\]","",v).strip()
        elif k.startswith("honour") and nums: data["honour"]=float(nums[0])
        elif k.startswith("ranking") and nums: data["ranking"]=int(float(nums[0]))
        elif k.startswith("networth") and nums: data["networth"]=int(float(nums[0]))
        elif k.startswith("spies sent") and nums: data["spies_sent"]=int(float(nums[0]))
        elif k.startswith("spies lost") and nums: data["spies_lost"]=int(float(nums[0]))
        elif k.startswith("result level"): data["result_level"]=v
        elif k.startswith("number of castles") and nums: data["castles"]=int(float(nums[0]))
        elif k.startswith("approximate defensive power") and nums: data["defense_power"]=int(float(nums[0]))

    res_s = extract_section(raw,"resources")
    trp_s = extract_section(raw,"troops")
    mov_s = extract_section(raw,"movements")
    mkt_s = extract_section(raw,"markets")
    tech_s = extract_section(raw,"tech")

    if res_s: data["resources"]=parse_resources(res_s)
    if trp_s: troops, dp=parse_troops(trp_s); data["troops"]=troops; data["defense_power"]=dp or data["defense_power"]
    if mov_s: data["movements"]= [line.strip(" -*•") for line in mov_s.splitlines() if line.strip()]
    if mkt_s: data["markets"]= [line.strip(" -*•") for line in mkt_s.splitlines() if line.strip()]
    if tech_s: data["tech"]= [line.strip(" -*•") for line in tech_s.splitlines() if line.strip()]

    m=CAPTURED_PAT.search(raw)
    if m: data["captured_at"]=parse_datetime_fuzzy(m.group(2))
    if not data["captured_at"]: 
        m2=CAPTURED_PAT_ALT.search(raw)
        if m2: data["captured_at"]=parse_datetime_fuzzy(m2.group(1))
    if not data["captured_at"]: data["captured_at"]=datetime.now(timezone.utc).isoformat()
    if not data["kingdom"]:
        km=re.search(r"^\s*Kingdom\s*:\s*(.+)$", raw, re.I|re.M)
        if km: data["kingdom"]=km.group(1).strip()
    return data

# ---------- Save Report ----------
def castle_bonus_percent(castles:int)->float:
    try: return (int(castles or 0)**0.5)/100
    except: return 0.0

def upsert_dp_session(*, target:str, spy_report_id:int, captured_at:str, base_dp:int, castles:int, with_castles:int):
    conn.execute("""
    INSERT INTO dp_sessions(target, spy_report_id, captured_at, base_dp_start, castles, current_base_dp, current_with_castles, hits_applied)
    VALUES (?, ?, ?, ?, ?, ?, ?, 0)
    """, (target, spy_report_id, captured_at, base_dp, castles, base_dp, with_castles))
    conn.commit()

def save_report(author_id:int, raw:str)->Tuple[Optional[int],Optional[str]]:
    data=parse_spy_report(raw)
    if not data.get("kingdom"): return None, None
    cur=conn.cursor()
    cur.execute("""
    INSERT INTO spy_reports(kingdom,alliance,honour,ranking,networth,spies_sent,spies_lost,result_level,castles,resources_json,troops_json,movements_json,markets_json,tech_json,defense_power,captured_at,author_id,raw)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """,(data.get("kingdom"),data.get("alliance"),data.get("honour"),data.get("ranking"),data.get("networth"),data.get("spies_sent"),data.get("spies_lost"),data.get("result_level"),data.get("castles"),json.dumps(data.get("resources")),json.dumps(data.get("troops")),json.dumps(data.get("movements")),json.dumps(data.get("markets")),json.dumps(data.get("tech")),data.get("defense_power"),data.get("captured_at"),str(author_id),raw))
    conn.commit()
    rid=cur.lastrowid
    kingdom=data.get("kingdom")
    if kingdom and data.get("defense_power"):
        base_dp=int(data["defense_power"])
        castles=int(data.get("castles") or 0)
        with_castles=ceil(base_dp*(1+castle_bonus_percent(castles)))
        upsert_dp_session(target=kingdom, spy_report_id=rid, captured_at=data.get("captured_at") or datetime.now(timezone.utc).isoformat(), base_dp=base_dp, castles=castles, with_castles=with_castles)
    return rid, kingdom

# ---------- Troop Attack Values ----------
TROOP_ATTACK_VALUES = {
    "pikemen":5, "footmen":5, "archers":7, "crossbowmen":8, "heavy cavalry":15, "knights":20
}

# ---------- !calc Command with Realistic Ideal Troops ----------
@bot.command(name="calc")
async def calc(ctx):
    await ctx.send("Please send the spy report you want to calculate against.")

    try:
        spy_msg = await bot.wait_for(
            'message',
            check=lambda m: m.author==ctx.author and m.channel==ctx.channel,
            timeout=300
        )
    except asyncio.TimeoutError:
        await ctx.send("Timed out. Please run !calc again.")
        return

    spy_data = parse_spy_report(spy_msg.content)
    if not spy_data.get("kingdom") or not spy_data.get("defense_power"):
        await ctx.send("Failed to parse spy report. Make sure it's in the correct KG2 format.")
        return

    base_dp = int(spy_data["defense_power"])
    castles = int(spy_data.get("castles") or 0)
    defender_dp = ceil(base_dp * (1 + castle_bonus_percent(castles)))

    # Function to calculate ideal attack for Major Victory (ignoring archers)
    def calc_ideal_attack(def_dp:int) -> Dict[str,int]:
        factor = 1.75  # aiming for Major Victory
        needed_power = int(def_dp * factor)
        # ignore archers
        usable_troops = {k:v for k,v in TROOP_ATTACK_VALUES.items() if k != "archers"}
        total_weight = sum(usable_troops.values())
        ideal = {}
        for t, v in usable_troops.items():
            # proportional to attack value
            count = ceil((v / total_weight) * needed_power / v)
            ideal[t] = max(count, 0)
        return ideal

    ideal_hit = calc_ideal_attack(defender_dp)

    # Format recommendation for user
    rec_lines = [f"{t.title()}: {human(c)}" for t,c in ideal_hit.items()]
    rec_text = "\n".join(rec_lines)
    await ctx.send(f"Based on this report, the recommended ideal troops to send for best result are:\n```{rec_text}```\nNow send the troops you have available in this format:\n`Pikemen 1000, Archers 500, Knights 50`")

    try:
        troops_msg = await bot.wait_for(
            'message',
            check=lambda m: m.author==ctx.author and m.channel==ctx.channel,
            timeout=300
        )
    except asyncio.TimeoutError:
        await ctx.send("Timed out. Please run !calc again.")
        return

    # Parse user troops
    attacker_power = 0
    user_troops = {}
    for part in troops_msg.content.split(","):
        if not part.strip(): continue
        try:
            name, count = part.strip().rsplit(" ",1)
            name = name.lower()
            count = int(count.replace(",",""))
            atk_val = TROOP_ATTACK_VALUES.get(name)
            if atk_val:
                attacker_power += atk_val * count
                user_troops[name] = count
        except:
            continue

    if attacker_power == 0:
        await ctx.send("Failed to parse your troops. Make sure the format is correct and troop names are valid.")
        return

    ratio = attacker_power / defender_dp
    if ratio >= 1.75: result="Major Victory (OV)"
    elif ratio >= 1.55: result="Victory (V)"
    elif ratio >= 1.25: result="Minor Victory (MV)"
    elif ratio < 0.9: result="Flee"
    else: result="Stalemate"

    # Suggest adjustments based on ideal hit
    adjust_lines = []
    for t, ideal_count in ideal_hit.items():
        user_count = user_troops.get(t,0)
        diff = ideal_count - user_count
        if diff > 0:
            adjust_lines.append(f"Consider adding {diff} {t.title()}")
        elif diff < 0:
            adjust_lines.append(f"Consider removing {abs(diff)} {t.title()}")

    adjust_text = "\n".join(adjust_lines) if adjust_lines else "Your troop composition matches the ideal distribution closely."

    await ctx.send(
        f"**Attacker Power:** {attacker_power}\n"
        f"**Defender Power (with castle bonus):** {defender_dp}\n"
        f"**Ratio:** {ratio:.2f}\n"
        f"**Expected Result:** {result}\n\n"
        f"**Suggestions for adjusting troops:**\n{adjust_text}"
    )


    # --- Display Results ---
    user_text = "\n".join([f"{k.title()}: {v}" for k,v in user_troops.items()])
    await ctx.send(f"Your troops:\n{code_block(user_text)}\nExpected result: **{result}**\n\nSuggestions:\n{code_block(sug_text)}")

# ---------- Run Bot ----------
bot.run(TOKEN)
