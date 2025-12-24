# ---------- KG2 Recon Bot (Complete Version) ----------
# Discord.py v2 • Auto-capture • Deduplication • Fuzzy matching • AP Planner • Troop Calc
# Commands: !kg2help, !watchhere, !watchall, !watchstatus, !savereport, !addspy, !spy, !ap, !spyhistory, !spyid, !exportspy, !rescanlast, !rescanrange, !checklast, !calc

import os, re, json, sqlite3, asyncio, csv, difflib
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
intents.messages = True
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

def castle_bonus_percent(castles:int)->float:
    try: return (int(castles or 0)**0.5)/100
    except: return 0.0

def parse_datetime_fuzzy(s:str) -> Optional[str]:
    s=s.strip()
    fmts=["%m/%d/%Y %I:%M %p","%m/%d/%Y %H:%M","%Y-%m-%d %H:%M:%S","%Y-%m-%d %H:%M","%d/%m/%Y %H:%M","%Y/%m/%d %H:%M:%S","%b %d, %Y, %I:%M:%S %p","%b %d, %Y %I:%M:%S %p","%b %d, %Y, %I:%M %p"]
    for f in fmts:
        try: return datetime.strptime(s,f).isoformat()
        except: pass
    try: return datetime.fromisoformat(s.replace("[mytime]","").replace("[/mytime]","").strip()).isoformat()
    except: return None

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

def get_close_kingdom_match(query:str) -> Optional[str]:
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT kingdom FROM spy_reports")
    kingdoms = [r[0] for r in cur.fetchall()]
    if not kingdoms: return None
    match = difflib.get_close_matches(query, kingdoms, n=1, cutoff=0.5)
    return match[0] if match else None

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

def save_report(author_id:int, raw:str)->Tuple[Optional[int],Optional[str]]:
    data=parse_spy_report(raw)
    if not data.get("kingdom"): return None, None
    cur=conn.cursor()
    cur.execute("""INSERT INTO spy_reports(kingdom,alliance,honour,ranking,networth,spies_sent,spies_lost,result_level,castles,resources_json,troops_json,movements_json,markets_json,tech_json,defense_power,captured_at,author_id,raw)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
    (data.get("kingdom"),data.get("alliance"),data.get("honour"),data.get("ranking"),data.get("networth"),data.get("spies_sent"),data.get("spies_lost"),data.get("result_level"),data.get("castles"),json.dumps(data.get("resources")),json.dumps(data.get("troops")),json.dumps(data.get("movements")),json.dumps(data.get("markets")),json.dumps(data.get("tech")),data.get("defense_power"),data.get("captured_at"),str(author_id),raw))
    conn.commit()
    rid=cur.lastrowid
    kingdom=data.get("kingdom")
    if kingdom and data.get("defense_power"):
        base_dp=int(data["defense_power"])
        castles=int(data.get("castles") or 0)
        with_castles=ceil(base_dp*(1+castle_bonus_percent(castles)))
        conn.execute("""INSERT INTO dp_sessions(target, spy_report_id, captured_at, base_dp_start, castles, current_base_dp, current_with_castles, hits_applied)
        VALUES(?,?,?,?,?,?,?,0)""", (kingdom, rid, data.get("captured_at") or datetime.now(timezone.utc).isoformat(), base_dp, castles, base_dp, with_castles))
        conn.commit()
    return rid, kingdom

TROOP_ATTACK_VALUES = {"pikemen":5, "footmen":5, "archers":7, "crossbowmen":8, "heavy cavalry":15, "knights":20}

# ---------- Bot Event: Auto-capture ----------
@bot.event
async def on_message(message):
    if message.author.bot: return
    watch, default_kingdom = get_watch(message.guild.id, message.channel.id)
    if watch:
        rid, kingdom = save_report(message.author.id, message.content)
        if rid and kingdom:
            await message.channel.send(f"📥 Spy report for **{kingdom}** saved (ID {rid}).")
    await bot.process_commands(message)

# ---------- Commands ----------
@bot.command(name="kg2help", aliases=["commands"])
async def kg2help(ctx):
    embed = discord.Embed(title="KG2 Recon Bot Commands", color=THEME_COLOR)
    embed.add_field(name="!watchhere on/off [Kingdom]", value="Start/stop watching this channel", inline=False)
    embed.add_field(name="!watchall on/off [Kingdom]", value="Admin: watch all channels in the guild", inline=False)
    embed.add_field(name="!watchstatus", value="Shows watch status", inline=False)
    embed.add_field(name="!spy <Kingdom>", value="Show last spy report for kingdom (fuzzy supported)", inline=False)
    embed.add_field(name="!ap <Kingdom> [hits]", value="Calculate attack points for a kingdom", inline=False)
    embed.add_field(name="!calc", value="Interactive combat calculator against a spy report", inline=False)
    embed.add_field(name="!spyhistory <Kingdom> [N]", value="Show last N spy reports", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="watchhere")
async def watchhere(ctx, mode:str, kingdom:Optional[str]=None):
    mode = mode.lower()
    if mode not in ["on","off"]: return await ctx.send("Usage: !watchhere on/off [Kingdom]")
    set_watch(ctx.guild.id, ctx.channel.id, mode=="on", kingdom)
    await ctx.send(f"{'Watching' if mode=='on' else 'Stopped watching'} this channel{' for '+kingdom if kingdom else ''}.")

@bot.command(name="watchall")
async def watchall(ctx, mode:str, kingdom:Optional[str]=None):
    mode = mode.lower()
    if mode not in ["on","off"]: return await ctx.send("Usage: !watchall on/off [Kingdom]")
    for ch in ctx.guild.text_channels:
        set_watch(ctx.guild.id, ch.id, mode=="on", kingdom)
    await ctx.send(f"{'Watching all channels' if mode=='on' else 'Stopped watching all channels'}{' for '+kingdom if kingdom else ''}.")

@bot.command(name="watchstatus")
async def watchstatus(ctx):
    cur = conn.cursor()
    cur.execute("SELECT channel_id, autocapture, default_kingdom FROM channel_settings WHERE guild_id=?", (str(ctx.guild.id),))
    rows = cur.fetchall()
    if not rows: return await ctx.send("No channels being watched.")
    msg = "\n".join([f"<#{r[0]}>: {'ON' if r[1] else 'OFF'}" + (f" | Kingdom: {r[2]}" if r[2] else "") for r in rows])
    await ctx.send(code_block(msg))

@bot.command(name="spy")
async def spy(ctx, *, kingdom:str):
    cur = conn.cursor()
    cur.execute("SELECT id, kingdom, captured_at FROM spy_reports WHERE kingdom=? ORDER BY captured_at DESC LIMIT 1", (kingdom,))
    row = cur.fetchone()
    if not row:
        match = get_close_kingdom_match(kingdom)
        if not match: return await ctx.send("No spy reports found.")
        cur.execute("SELECT id, kingdom, captured_at FROM spy_reports WHERE kingdom=? ORDER BY captured_at DESC LIMIT 1", (match,))
        row = cur.fetchone()
        if not row: return await ctx.send("No spy reports found.")
    await ctx.send(f"Spy report for **{row[1]}** captured at {row[2]} (ID {row[0]})")

@bot.command(name="ap")
async def ap(ctx, kingdom:str):
    cur=conn.cursor()
    cur.execute("SELECT * FROM dp_sessions WHERE target=? ORDER BY captured_at DESC LIMIT 1",(kingdom,))
    row=cur.fetchone()
    if not row: return await ctx.send("No DP session found for kingdom.")
    base_dp=row[5]
    castles=row[4]
    with_castles=row[6]
    hits=row[7]
    await ctx.send(f"AP Planner • {kingdom}\nBase DP: {base_dp}\nWith Castle Bonus: {with_castles}\nCastles: {castles}\nHits Applied: {hits}")

@bot.command(name="calc")
async def calc(ctx):
    await ctx.send("Please paste the spy report to calculate against.")

    try:
        spy_msg = await bot.wait_for(
            'message',
            check=lambda m: m.author==ctx.author and m.channel==ctx.channel,
            timeout=300
        )
    except asyncio.TimeoutError:
        return await ctx.send("Timeout waiting for spy report.")

    data=parse_spy_report(spy_msg.content)
    if not data.get("kingdom"):
        return await ctx.send("Failed to parse spy report.")

    # Suggest troops
    hc_suggestion = ceil((data.get("defense_power") or 0)/TROOP_ATTACK_VALUES.get("heavy cavalry",15))
    await ctx.send(f"Suggested heavy cavalry to attack {data['kingdom']}: {hc_suggestion}")

    await ctx.send("Enter the troops you are sending as JSON (e.g., {\"Heavy Cavalry\":50,\"Archers\":200}):")
    try:
        troops_msg = await bot.wait_for(
            'message',
            check=lambda m: m.author==ctx.author and m.channel==ctx.channel,
            timeout=300
        )
    except asyncio.TimeoutError:
        return await ctx.send("Timeout waiting for troops input.")

    try:
        sent_troops = json.loads(troops_msg.content)
    except:
        return await ctx.send("Failed to parse troops input as JSON.")

    dp = data.get("defense_power") or 0
    total_attack = sum([TROOP_ATTACK_VALUES.get(k.lower(),0)*v for k,v in sent_troops.items()])
    ratio = total_attack/dp if dp else 0
    outcome = "💀 Likely loss" if ratio<1 else "⚔️ Likely win" if ratio<2 else "🏆 Overkill"
    await ctx.send(f"Total attack power: {total_attack}\nDefensive power: {dp}\nOutcome suggestion: {outcome}")

# ---------- Auto-watch new channels ----------
@bot.event
async def on_guild_channel_create(channel):
    if not isinstance(channel, discord.TextChannel): return
    guild_id = channel.guild.id
    watchall_on = any(get_watch(guild_id, ch.id)[0] for ch in channel.guild.text_channels)
    if watchall_on:
        set_watch(guild_id, channel.id, True)
        await channel.send("📡 Auto-watching new channel.")

# ---------- Bot Run ----------
bot.run(TOKEN)
