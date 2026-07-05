#!/usr/bin/env python3
import hashlib
import json
import os
import re
import time
import urllib.error
import urllib.request
from typing import Dict, List

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


def _load_state() -> Dict[str, str]:
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return {str(k): str(v) for k, v in data.items()}
    except Exception:
        pass
    return {}


def _save_state(state: Dict[str, str]) -> None:
    try:
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"WARN: failed to save state file: {e}")


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
        if page.locator("[contenteditable='true'], textarea, [aria-label*='Message']").first.count():
            return True
        return page.locator("[role='main'] div[dir='auto'], [role='main'] span[dir='auto']").first.count() > 0
    except Exception:
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
        lambda: page.get_by_role("link", name=re.compile(re.escape(chat_name), re.IGNORECASE)).first,
        lambda: page.get_by_text(chat_name, exact=False).first,
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


def _open_chat(page, chat_name: str) -> bool:
    _ensure_messages_home(page)

    strategies = [
        lambda: page.get_by_role("link", name=re.compile(re.escape(chat_name), re.IGNORECASE)).first,
        lambda: page.get_by_text(chat_name, exact=False).first,
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

    # Fallback: use Messenger search to surface the conversation, then open first hit.
    search_selectors = [
        "input[aria-label*='Search']",
        "input[placeholder*='Search']",
        "input[type='search']",
    ]
    for sel in search_selectors:
        try:
            search_box = page.locator(sel).first
            if not search_box.count():
                continue
            search_box.click(timeout=1200)
            search_box.fill("")
            search_box.fill(chat_name)
            page.wait_for_timeout(1200)

            # Try matching a visible result row/link containing the chat name.
            result = page.get_by_role("link", name=re.compile(re.escape(chat_name), re.IGNORECASE)).first
            if result.count() and result.is_visible(timeout=1200):
                result.click(timeout=2500)
                page.wait_for_timeout(700)
                if _looks_like_open_thread(page):
                    return True

            result = page.get_by_text(chat_name, exact=False).first
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

    return cleaned[-max_items:]


def _build_report_candidates(snippets: List[str]) -> List[str]:
    if not snippets:
        return []

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
        if s not in seen:
            seen.add(s)
            out.append(s)

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

        candidate = "\n".join(joined).strip()
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

                    # Select the strongest current report candidate to avoid order-related misses.
                    best = max(report_candidates, key=lambda m: (_report_score(m), len(m)))
                    best_hash = _sha(best)
                    if best_hash == state.get(chat_name, ""):
                        continue

                    mid = _sha(f"{chat_name}|{best}")[:24]
                    external_id = f"{chat_name}:{mid}"
                    result = _bridge_post(best, external_id)
                    print(f"INFO: bridge {chat_name} status={result.get('status')} ok={result.get('ok')}")
                    state[chat_name] = best_hash
                    _save_state(state)

                time.sleep(max(3, FB_POLL_SECONDS))
        except KeyboardInterrupt:
            print("INFO: stopping bridge worker")
        finally:
            context.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
