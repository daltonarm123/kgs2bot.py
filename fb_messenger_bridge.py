#!/usr/bin/env python3
import hashlib
import json
import os
import re
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(str(raw).strip())
    except Exception:
        return default


def _env_text(name: str, default: str = "") -> str:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw)


FB_EMAIL = _env_text("FB_MESSENGER_EMAIL", "").strip()
FB_PASSWORD = _env_text("FB_MESSENGER_PASSWORD", "").strip()
FB_PROFILE_DIR = _env_text("FB_MESSENGER_PROFILE_DIR", ".fb_messenger_profile").strip() or ".fb_messenger_profile"
FB_HEADLESS = _env_bool("FB_MESSENGER_HEADLESS", False)
FB_POLL_SECONDS = _env_int("FB_MESSENGER_POLL_SECONDS", 15)
FB_LOOKBACK_MESSAGES = _env_int("FB_MESSENGER_LOOKBACK_MESSAGES", 40)
FB_SEEN_REPORT_LIMIT = max(50, _env_int("FB_MESSENGER_SEEN_REPORT_LIMIT", 300))
FB_POST_LATEST_ONLY = _env_bool("FB_MESSENGER_POST_LATEST_ONLY", True)
FB_LOGIN_ONLY = _env_bool("FB_MESSENGER_LOGIN_ONLY", False)
FB_DEBUG_REPORTS = _env_bool("FB_MESSENGER_DEBUG_REPORTS", False)
FB_CHAT_NAMES = [
    name.strip()
    for name in _env_text(
        "FB_MESSENGER_CHAT_NAMES",
        "Mom's Knights In Training,A Team Only",
    ).split(",")
    if name.strip()
]

BRIDGE_URL = _env_text("BRIDGE_HTTP_URL", "https://kg2recon-production.up.railway.app/api/bridge/report").strip()
BRIDGE_TOKEN = _env_text("BRIDGE_HTTP_TOKEN", "").strip()
BRIDGE_SOURCE = _env_text("BRIDGE_HTTP_SOURCE", "facebook-messenger").strip() or "facebook-messenger"

STATE_PATH = _env_text("FB_MESSENGER_STATE_PATH", ".fb_messenger_bridge_state.json").strip() or ".fb_messenger_bridge_state.json"


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def _preview(text: str, limit: int = 220) -> str:
    compact = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3] + "..."


_REPORT_BREAK_BEFORE = (
    "Received:",
    "From:",
    "Date:",
    "To:",
    "Subject:",
    "Attack Report:",
    "Attack Result:",
    "You have gained the following during the attack:",
    "We regret to inform you of the following casualties during the attack:",
    "You have also been awarded",
    "Target:",
    "Alliance:",
    "Honour:",
    "Ranking:",
    "Networth:",
    "Spies Sent:",
    "Spies Lost:",
    "Result Level:",
    "Number of Castles:",
    "Our spies also found the following information about the kingdom's resources:",
    "Our spies also found the following information about the kingdoms resources:",
    "Our spies also found the following information about the kingdom's troops:",
    "Our spies also found the following information about the kingdoms troops:",
    "The following information was found regarding troop movements around this kingdom:",
    "The following recent market transactions were also discovered:",
    "The following technology information was also discovered:",
    "Approximate defensive power*:",
    "Population:",
    "Knights:",
    "Heavy Cavalry:",
    "Archers:",
    "Peasants:",
    "Pikemen:",
    "Footmen:",
    "Crossbowmen:",
    "Horses:",
    "Green Gems:",
    "Blue Gems:",
    "Stone:",
    "Land:",
    "Food:",
    "Wood:",
    "Gold:",
    "Attacked by ",
    "Launched an attack on ",
    "Bought ",
    "Sold ",
)


_MOVEMENT_MARKET_LINE_RE = re.compile(r"^(Launched an attack on |Attacked by |Bought |Sold )", re.IGNORECASE)


def _trim_report_tail_lines(lines: List[str]) -> List[str]:
    trimmed: List[str] = []
    in_tech_section = False
    chrome_re = re.compile(r"^(Mute|Search|Chat info|Customize chat|Chat members|Media, files and links|Privacy & support|@everyone|@here)\b", re.IGNORECASE)
    tech_re = re.compile(r"^(.+?\blvl\s+\d+)\b.*$", re.IGNORECASE)

    for raw_line in lines:
        line = str(raw_line or "").strip()
        if not line:
            continue
        if chrome_re.search(line):
            break

        # Drop trailing chat noise glued after movement/market rows ending in ")".
        if _MOVEMENT_MARKET_LINE_RE.match(line):
            idx = line.rfind(")")
            if idx >= 0 and idx + 1 < len(line):
                line = line[: idx + 1].strip()

        if in_tech_section:
            match = tech_re.match(line)
            if not match:
                break
            line = match.group(1).strip()

        trimmed.append(line)

        if "the following technology information was also discovered" in line.lower():
            match = tech_re.match(line)
            if match:
                trimmed[-1] = match.group(1).strip()
            in_tech_section = True

    return trimmed


def _canonical_report_text(text: str) -> str:
    formatted = _format_report_text(text)
    lines = []
    for raw_line in formatted.splitlines():
        line = raw_line.strip()
        ll = line.lower()
        if not line:
            continue
        if ll in {"pulled spy report from fb", "pulled attack report from fb"}:
            continue
        if re.fullmatch(r"fb-(?:spy|attack)-report\.txt\s+\d+\s*kb", ll):
            continue
        line = re.sub(r"\s+fb-(?:spy|attack)-report\.txt\s+\d+\s*kb\b.*$", "", line, flags=re.IGNORECASE).strip()
        if not line:
            continue
        lines.append(line)

    start_markers = ("received:", "from:", "date:", "to:", "subject:", "attack report:", "target:")
    while lines and not lines[0].lower().startswith(start_markers):
        lines.pop(0)
    return "\n".join(lines).strip()


def _format_report_text(text: str) -> str:
    value = re.sub(r"\r\n?", "\n", str(text or "")).strip()
    if not value:
        return ""

    value = re.sub(r"\s+", " ", value).strip()
    value = re.sub(r"^Enter,\s*Message sent .*? by [^:]{1,80}:\s*", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\s+Enter,\s*Message sent .*? by [^:]{1,80}:\s*", "\n", value, flags=re.IGNORECASE)
    for marker in sorted(_REPORT_BREAK_BEFORE, key=len, reverse=True):
        value = re.sub(rf"\s+(?={re.escape(marker)})", "\n", value)

    # Force chat noise / messenger chrome onto its own line so tail trim can drop it.
    for noise in ("Mute", "Search", "Chat info", "Customize chat", "Chat members", "Media, files and links", "Privacy & support", "@everyone", "@here"):
        value = re.sub(rf"\s+(?={re.escape(noise)}\b)", "\n", value)

    value = re.sub(r"(?<=\d)\*\(without skill/prayer modifiers\)", "\n*(without skill/prayer modifiers)", value)
    value = re.sub(r"Subject:\nAttack Report:", "Subject: Attack Report:", value)
    value = re.sub(r"(?<=\))\s+(?=The following|Our spies|From:|Target:|Attack Report:)", "\n\n", value)
    value = re.sub(r"(?<=\d)\s+(?=[A-Z][A-Za-z ]+ lvl \d+)", "\n", value)
    value = re.sub(r"\s+(?=\d+ of your footsoldiers)", "\n", value, flags=re.IGNORECASE)
    value = re.sub(r"(?<=\))\s+(?=Attacked by |Launched an attack on |Bought |Sold )", "\n", value)
    value = re.sub(r"(but were unable to take the town from the defending forces\.)(?:[^\S\n]+(?!A number of buildings were damaged during the battle\.)[^\n]*)", r"\1", value, flags=re.IGNORECASE)
    value = re.sub(r"(A number of buildings were damaged during the battle\.)(?:[^\S\n]+[^\n]*)", r"\1", value, flags=re.IGNORECASE)
    value = re.sub(r"((?:Large|Medium|Small) Town .+?\(level \d+ settlement\)\.)(?:[^\S\n]+[^\n]*)", r"\1", value, flags=re.IGNORECASE)
    value = re.sub(r"\n{3,}", "\n\n", value)
    lines = _trim_report_tail_lines([line.strip() for line in value.splitlines() if line.strip()])
    return "\n".join(lines).strip()


def _split_report_blob(text: str) -> List[str]:
    formatted = _format_report_text(text)
    if not formatted:
        return []

    chunks: List[str] = []
    current: List[str] = []
    for line in formatted.splitlines():
        ll = line.lower()
        starts_new = ll.startswith("target:") or ll.startswith("from:")
        if current and starts_new:
            chunks.append("\n".join(current).strip())
            current = []
        current.append(line)
    if current:
        chunks.append("\n".join(current).strip())
    return chunks or [formatted]


def _chat_name_pattern(chat_name: str) -> re.Pattern:
    pieces: List[str] = []
    for char in str(chat_name or ""):
        if char in {"'", "\u2019", "\u2018"}:
            pieces.append(r"['\u2019\u2018]")
        elif char.isspace():
            pieces.append(r"\s+")
        else:
            pieces.append(re.escape(char))
    return re.compile("".join(pieces), re.IGNORECASE)


def _page_debug_state(page) -> str:
    try:
        data = page.evaluate(
            """
() => ({
  url: location.href,
  title: document.title,
  text: (document.body && document.body.innerText || '').slice(0, 1200),
})
"""
        )
        return f"url={data.get('url')} title={data.get('title')} text={_preview(data.get('text'), 500)}"
    except Exception as e:
        return f"page_state_error={e}"


def _page_text(page) -> str:
    try:
        return str(page.evaluate("() => document.body && document.body.innerText || ''") or "")
    except Exception:
        return ""


def _is_report_text(text: str) -> bool:
    t = str(text or "").strip()
    if len(t) < 20:
        return False

    ll = t.lower()

    # Attack reports from FB should contain the core header + result sections.
    looks_attack = (
        ("subject: attack report:" in ll or "attack report:" in ll)
        and "attack result:" in ll
        and (
            "you have gained the following during the attack" in ll
            or "casualties during the attack" in ll
        )
    )

    # Spy reports should contain target/spy metadata and troop/resource sections.
    looks_spy = (
        "target:" in ll
        and "spies sent:" in ll
        and "spies lost:" in ll
        and (
            "number of castles:" in ll
            or "approximate defensive power" in ll
        )
        and "our spies also found" in ll
    )

    return looks_attack or looks_spy


def _load_state() -> Dict[str, Any]:
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return {str(k): v for k, v in data.items()}
    except Exception:
        pass
    return {}


def _save_state(state: Dict[str, Any]) -> None:
    try:
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"WARN: failed to save state file: {e}")


def _chat_state(state: Dict[str, Any], chat_name: str) -> Dict[str, Any]:
    raw = state.get(chat_name)
    if isinstance(raw, dict):
        seen = raw.get("seen_report_hashes")
        if not isinstance(seen, list):
            seen = []
        chat_state = {
            "initialized": bool(raw.get("initialized")),
            "last_report_hash": str(raw.get("last_report_hash") or raw.get("last_hash") or ""),
            "seen_report_hashes": [str(v) for v in seen if str(v or "").strip()],
        }
    else:
        legacy_hash = str(raw or "").strip()
        chat_state = {
            "initialized": False,
            "last_report_hash": legacy_hash,
            "seen_report_hashes": [legacy_hash] if legacy_hash else [],
        }
    state[chat_name] = chat_state
    return chat_state


def _remember_report_hashes(chat_state: Dict[str, Any], report_hashes: List[str]) -> None:
    seen = [str(v) for v in chat_state.get("seen_report_hashes", []) if str(v or "").strip()]
    seen_set = set(seen)
    for report_hash in report_hashes:
        h = str(report_hash or "").strip()
        if not h:
            continue
        if h in seen_set:
            continue
        seen.append(h)
        seen_set.add(h)
    if len(seen) > FB_SEEN_REPORT_LIMIT:
        seen = seen[-FB_SEEN_REPORT_LIMIT:]
    chat_state["seen_report_hashes"] = seen
    chat_state["initialized"] = True
    if report_hashes:
        chat_state["last_report_hash"] = str(report_hashes[-1] or "").strip()


def _bridge_post(raw_text: str, external_id: str) -> dict:
    payload = {
        "source": BRIDGE_SOURCE,
        "external_id": external_id,
        "raw_text": raw_text,
    }
    req = urllib.request.Request(
        BRIDGE_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "X-Bridge-Token": BRIDGE_TOKEN,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=25) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return {
                "ok": True,
                "status": resp.getcode(),
                "body": body,
            }
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        return {
            "ok": False,
            "status": e.code,
            "body": body,
        }
    except Exception as e:
        return {
            "ok": False,
            "status": None,
            "body": str(e),
        }


def _wait_for_inbox(page) -> bool:
    checks = [
        "[aria-label='Chats']",
        "[role='navigation']",
        "div[role='grid']",
        "[aria-label*='Messenger']",
    ]
    for _ in range(30):
        for selector in checks:
            try:
                if page.locator(selector).first.is_visible(timeout=300):
                    return True
            except Exception:
                continue
        time.sleep(1)
    return False


def _attempt_login(page) -> None:
    page.goto("https://www.facebook.com/messages/t/", wait_until="domcontentloaded", timeout=60000)

    if not page.locator("input[name='email']").count():
        return

    if FB_EMAIL and FB_PASSWORD:
        print("INFO: login form detected; submitting FB_MESSENGER_EMAIL/FB_MESSENGER_PASSWORD")
        page.fill("input[name='email']", FB_EMAIL)
        page.fill("input[name='pass']", FB_PASSWORD)
        clicked = False
        login_selectors = [
            "button[name='login']",
            "#loginbutton",
            "div[aria-label='Log in']",
            "div[role='button'][aria-label='Log in']",
            "button:has-text('Log in')",
        ]
        for sel in login_selectors:
            try:
                loc = page.locator(sel).first
                if loc.count() and loc.is_visible(timeout=800):
                    loc.click(timeout=2500)
                    clicked = True
                    break
            except Exception:
                continue
        if not clicked:
            try:
                page.press("input[name='pass']", "Enter")
                clicked = True
            except Exception:
                pass
        if not clicked:
            raise RuntimeError("Could not submit Facebook login form")
        page.wait_for_timeout(2000)

    if _wait_for_inbox(page):
        return

    if FB_HEADLESS:
        raise RuntimeError("Messenger login requires interactive approval (checkpoint/2FA). Re-run with FB_MESSENGER_HEADLESS=false.")

    print("ACTION NEEDED: Complete Facebook login/checkpoint in the opened browser window. Waiting for inbox...")
    deadline = time.time() + 600
    while time.time() < deadline:
        if _wait_for_inbox(page):
            print("INFO: inbox detected after manual login")
            return
        time.sleep(2)

    raise RuntimeError("Messenger inbox did not load within 10 minutes.")


def _ensure_messages_home(page) -> None:
    if "/messages/" in str(page.url):
        return
    page.goto("https://www.facebook.com/messages/t/", wait_until="domcontentloaded", timeout=60000)
    _wait_for_inbox(page)


def _looks_like_open_thread(page) -> bool:
    try:
        url = str(page.url)
        if "/search/" in url:
            return False
        if "/messages/" not in url:
            return False
        if re.search(r"/messages/t/\d+", url):
            return True
        if page.locator("[contenteditable='true'], textarea, [aria-label*='Message']").first.count():
            return True
        return page.locator("[role='main'] div[dir='auto'], [role='main'] span[dir='auto']").first.count() > 0
    except Exception:
        return False


def _current_thread_has_chat_report_preview(page, chat_name: str) -> bool:
    text = _page_text(page)
    if not text:
        return False
    for match in _chat_name_pattern(chat_name).finditer(text):
        nearby = text[match.end() : match.end() + 420].lower()
        if "unread message" not in nearby and "subject:" not in nearby and "target:" not in nearby:
            continue
        report_pos = min([pos for pos in (nearby.find("subject:"), nearby.find("target:")) if pos >= 0] or [-1])
        if report_pos >= 0:
            return True
    return False


def _open_from_search_results(page, chat_name: str) -> bool:
    if "/search/" not in str(page.url):
        return False
    try:
        messenger_filter = page.get_by_text("Messenger", exact=True).first
        if messenger_filter.count() and messenger_filter.is_visible(timeout=1200):
            messenger_filter.click(timeout=2500)
            page.wait_for_timeout(1200)
    except Exception:
        pass
    for get_locator in (
        lambda: page.get_by_role("link", name=_chat_name_pattern(chat_name)).first,
        lambda: page.get_by_text(_chat_name_pattern(chat_name)).first,
    ):
        try:
            result = get_locator()
            if result.count() and result.is_visible(timeout=1200):
                result.click(timeout=2500)
                page.wait_for_timeout(1200)
                return _looks_like_open_thread(page)
        except Exception:
            continue
    return False


def _click_visible_chat_row(page, chat_name: str) -> bool:
    try:
        clicked = page.evaluate(
            r"""
(chatName) => {
    const normalize = (value) => String(value || '')
        .toLowerCase()
        .replace(/[\u2018\u2019]/g, "'")
        .replace(/\s+/g, ' ')
        .trim();
    const wanted = normalize(chatName);
    const nodes = Array.from(document.querySelectorAll('a, [role="link"], [role="button"], [tabindex], div[dir="auto"], span[dir="auto"]'));
    const matches = nodes
        .filter((node) => normalize(node.innerText || node.textContent).includes(wanted))
        .filter((node) => {
            const rect = node.getBoundingClientRect();
            return rect.width > 0 && rect.height > 0;
        })
        .sort((a, b) => {
            const ar = a.getBoundingClientRect();
            const br = b.getBoundingClientRect();
            return (ar.width * ar.height) - (br.width * br.height);
        });
    for (const node of matches) {
        let clickable = node.closest('a, [role="link"], [role="button"], [tabindex]') || node;
        const rect = clickable.getBoundingClientRect();
        if (!rect.width || !rect.height) continue;
        clickable.click();
        return true;
    }
    return false;
}
""",
            chat_name,
        )
        if clicked:
            page.wait_for_timeout(1200)
            return _looks_like_open_thread(page)
    except Exception:
        pass
    return False


def _open_chat(page, chat_name: str) -> bool:
    _ensure_messages_home(page)
    chat_pattern = _chat_name_pattern(chat_name)

    strategies = [
        lambda: page.get_by_role("link", name=chat_pattern).first,
        lambda: page.get_by_text(chat_pattern).first,
    ]
    for get_locator in strategies:
        try:
            locator = get_locator()
            if locator and locator.is_visible(timeout=1500):
                locator.click(timeout=3000)
                page.wait_for_timeout(700)
                if _looks_like_open_thread(page):
                    return True
        except Exception:
            continue

    if _click_visible_chat_row(page, chat_name):
        return True

    # Fallback: use Messenger search to surface the conversation, then open first hit.
    search_selectors = [
        "input[aria-label*='Search Messenger']",
        "input[placeholder*='Search Messenger']",
        "[aria-label='Search Messenger']",
        "[aria-label*='Search Messenger']",
    ]
    for sel in search_selectors:
        try:
            search_box = page.locator(sel).first
            if not search_box.count():
                continue
            if FB_DEBUG_REPORTS:
                print(f"DEBUG: using messenger search selector={sel} chat={chat_name}")
            search_box.click(timeout=1200)
            search_box.fill("")
            search_box.fill(chat_name)
            page.wait_for_timeout(1200)

            # Try matching a visible result row/link containing the chat name.
            result = page.get_by_role("link", name=chat_pattern).first
            if result.count() and result.is_visible(timeout=1200):
                result.click(timeout=2500)
                page.wait_for_timeout(700)
                if _looks_like_open_thread(page):
                    return True

            result = page.get_by_text(chat_pattern).first
            if result.count() and result.is_visible(timeout=1200):
                result.click(timeout=2500)
                page.wait_for_timeout(700)
                if _looks_like_open_thread(page):
                    return True

            # Some UIs let Enter open top search result.
            search_box.press("Enter")
            page.wait_for_timeout(1200)
            if _looks_like_open_thread(page):
                return True
            if _open_from_search_results(page, chat_name):
                return True
        except Exception:
            continue
    if FB_DEBUG_REPORTS:
        print(f"DEBUG: open chat failed chat={chat_name}: {_page_debug_state(page)}")
    return False


def _extract_recent_messages(page, max_items: int) -> List[str]:
    js = """
() => {
  const root = document.querySelector('[role="main"]') || document.body;
  const nodes = Array.from(root.querySelectorAll('div[dir="auto"], span[dir="auto"]'));
  const out = [];
  for (const n of nodes) {
    const txt = (n.innerText || '').trim();
    if (!txt) continue;
    if (txt.length < 2) continue;
    if (/^(Messenger|Write a message|Type a message|Search Messenger)$/i.test(txt)) continue;
    out.push(txt);
  }
  return out.slice(-200);
}
"""
    try:
        texts = page.evaluate(js)
    except Exception:
        return []

    cleaned: List[str] = []
    seen = set()
    for txt in texts:
        t = re.sub(r"\s+", " ", str(txt or "")).strip()
        if not t:
            continue
        if t in seen:
            continue
        seen.add(t)
        cleaned.append(t)

    if not cleaned:
        body_text = _page_text(page)
        for txt in re.split(r"[\r\n]+", body_text):
            t = re.sub(r"\s+", " ", str(txt or "")).strip()
            if not t or len(t) < 2:
                continue
            if t in seen:
                continue
            seen.add(t)
            cleaned.append(t)

    return cleaned[-max_items:]


def _build_report_candidates(snippets: List[str]) -> List[str]:
    if not snippets:
        return []

    expanded: List[str] = []
    for snippet in snippets:
        parts = _split_report_blob(snippet)
        if parts:
            expanded.extend(parts)
        else:
            expanded.append(snippet)
    snippets = expanded

    starters = (
        "subject:",
        "attack report",
        "magic spy report",
        "target:",
    )

    out: List[str] = []
    seen = set()

    # Keep original snippets too, then add stitched multiline candidates.
    for s in snippets:
        formatted = _format_report_text(s)
        h = _sha(formatted) if formatted else ""
        if formatted and h not in seen:
            seen.add(h)
            out.append(formatted)

    n = len(snippets)
    for i, line in enumerate(snippets):
        ll = line.lower()
        if not any(tok in ll for tok in starters):
            continue

        # Stitch forward lines to rebuild full report blobs from fragmented nodes.
        joined = [line]
        for j in range(i + 1, min(n, i + 45)):
            nxt = snippets[j]
            # New report likely starts here; stop current stitch.
            nxt_ll = nxt.lower()
            if j > i + 1 and ("subject:" in nxt_ll or "target:" in nxt_ll):
                break
            joined.append(nxt)

        candidate = _format_report_text("\n".join(joined))
        if len(candidate) < 40:
            continue
        h = _sha(candidate)
        if h in seen:
            continue
        seen.add(h)
        out.append(candidate)

    return out


def _report_score(text: str) -> int:
    ll = text.lower()
    score = 0
    for token in (
        "subject: attack report",
        "attack result:",
        "you have gained the following during the attack",
        "target:",
        "spies sent:",
        "spies lost:",
        "our spies also found",
        "approximate defensive power",
        "number of castles:",
    ):
        if token in ll:
            score += 1
    return score


def _unseen_report_batch(report_candidates: List[str], seen_hashes: set) -> List[tuple[str, str]]:
    unseen_reports: List[tuple[str, str]] = []
    batch_hashes = set()
    for candidate in report_candidates:
        # Some Messenger snapshots collapse multiple full reports into one blob.
        # Split again here so we never forward a doubled "Target: ... Target: ..." payload.
        parts = _split_report_blob(candidate)
        for part in (parts or [candidate]):
            formatted = _format_report_text(part)
            if not _is_report_text(formatted):
                continue
            canonical = _canonical_report_text(formatted) or formatted
            candidate_hash = _sha(canonical)
            if candidate_hash in seen_hashes or candidate_hash in batch_hashes:
                continue
            unseen_reports.append((formatted, candidate_hash))
            batch_hashes.add(candidate_hash)
    return unseen_reports


def main() -> int:
    if not BRIDGE_TOKEN:
        print("ERROR: BRIDGE_HTTP_TOKEN is required")
        return 2
    if not BRIDGE_URL:
        print("ERROR: BRIDGE_HTTP_URL is required")
        return 2
    if not FB_CHAT_NAMES:
        print("ERROR: FB_MESSENGER_CHAT_NAMES is empty")
        return 2

    state = _load_state()

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=FB_PROFILE_DIR,
            headless=FB_HEADLESS,
            viewport={"width": 1366, "height": 900},
        )
        page = context.pages[0] if context.pages else context.new_page()

        try:
            _attempt_login(page)
        except PlaywrightTimeoutError as e:
            print(f"ERROR: timeout while loading Facebook: {e}")
            context.close()
            return 3
        except Exception as e:
            print(f"ERROR: login failed: {e}")
            context.close()
            return 3

        if FB_LOGIN_ONLY:
            print("INFO: login-only mode complete; persistent profile is ready")
            context.close()
            return 0

        print(f"INFO: watching {len(FB_CHAT_NAMES)} chat(s): {', '.join(FB_CHAT_NAMES)}")

        try:
            while True:
                for chat_name in FB_CHAT_NAMES:
                    if not _open_chat(page, chat_name):
                        print(f"WARN: could not locate chat: {chat_name}")
                        continue

                    messages = _extract_recent_messages(page, FB_LOOKBACK_MESSAGES)
                    print(f"INFO: scanned chat={chat_name} messages={len(messages)}")
                    if FB_DEBUG_REPORTS and not messages:
                        print(f"DEBUG: no messages chat={chat_name}: {_page_debug_state(page)}")
                    if not messages:
                        continue

                    candidates = _build_report_candidates(messages)
                    report_candidates = [m for m in candidates if _is_report_text(m)]
                    if FB_DEBUG_REPORTS:
                        print(
                            f"DEBUG: report scan chat={chat_name} "
                            f"snippets={len(messages)} candidates={len(candidates)} "
                            f"matches={len(report_candidates)}"
                        )
                        for idx, snippet in enumerate(messages[-8:], start=max(1, len(messages) - 7)):
                            print(f"DEBUG: snippet[{idx}] chat={chat_name}: {_preview(snippet)}")
                        for idx, candidate in enumerate(candidates[-8:], start=max(1, len(candidates) - 7)):
                            print(
                                f"DEBUG: candidate[{idx}] chat={chat_name} "
                                f"score={_report_score(candidate)} match={_is_report_text(candidate)}: "
                                f"{_preview(candidate)}"
                            )
                    if not report_candidates:
                        continue

                    chat_state = _chat_state(state, chat_name)
                    report_hashes = [_sha(_canonical_report_text(m) or _format_report_text(m)) for m in report_candidates]
                    if not chat_state.get("initialized"):
                        _remember_report_hashes(chat_state, report_hashes)
                        _save_state(state)
                        print(f"INFO: baseline chat={chat_name} reports={len(report_hashes)}")
                        continue

                    seen_hashes = set(chat_state.get("seen_report_hashes", []))
                    unseen_reports = _unseen_report_batch(report_candidates, seen_hashes)
                    if not unseen_reports:
                        continue

                    reports_to_post = unseen_reports[-1:] if FB_POST_LATEST_ONLY else unseen_reports
                    if FB_POST_LATEST_ONLY and len(unseen_reports) > 1:
                        print(
                            f"INFO: chat={chat_name} collapsing {len(unseen_reports)} unseen reports "
                            "to latest-only post"
                        )

                    posted_hashes = []
                    for best, best_hash in reports_to_post:
                        mid = _sha(f"{chat_name}|{best}")[:24]
                        external_id = f"{chat_name}:{mid}"
                        result = _bridge_post(best, external_id)
                        print(f"INFO: bridge {chat_name} status={result.get('status')} ok={result.get('ok')}")
                        if result.get("ok"):
                            posted_hashes.append(best_hash)

                    # Mark older unseen history as seen when latest-only mode is enabled,
                    # so periodic rescans never replay backlog reports.
                    if FB_POST_LATEST_ONLY:
                        seen_hashes_now = [h for _, h in unseen_reports]
                        if posted_hashes:
                            _remember_report_hashes(chat_state, seen_hashes_now)
                            _save_state(state)
                        else:
                            print(f"WARN: latest report post failed for chat={chat_name}; keeping unseen state for retry")
                        continue

                    _remember_report_hashes(chat_state, posted_hashes)
                    _save_state(state)

                time.sleep(max(3, FB_POLL_SECONDS))
        except KeyboardInterrupt:
            print("INFO: stopping bridge worker")
        finally:
            context.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
