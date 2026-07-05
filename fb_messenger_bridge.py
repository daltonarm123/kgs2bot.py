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
FB_CHAT_NAMES = [
    name.strip()
    for name in _env_text(
        "FB_MESSENGER_CHAT_NAMES",
        "mom's knights in training,a team only",
    ).split(",")
    if name.strip()
]

BRIDGE_URL = _env_text("BRIDGE_HTTP_URL", "https://kg2recon-production.up.railway.app/api/bridge/report").strip()
BRIDGE_TOKEN = _env_text("BRIDGE_HTTP_TOKEN", "").strip()
BRIDGE_SOURCE = _env_text("BRIDGE_HTTP_SOURCE", "facebook-messenger").strip() or "facebook-messenger"

STATE_PATH = _env_text("FB_MESSENGER_STATE_PATH", ".fb_messenger_bridge_state.json").strip() or ".fb_messenger_bridge_state.json"


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def _is_report_text(text: str) -> bool:
    t = str(text or "").strip()
    if len(t) < 20:
        return False

    report_markers = [
        r"Approximate defensive power",
        r"Number of Castles",
        r"The following technology information was also discovered",
        r"Subject:\s*Attack Report",
        r"Attack Report",
        r"Attack Result",
        r"Target:",
        r"You have been attacked by",
        r"attacked\s+.*\s+for\s+\d+\s+(?:land|acres)",
        r"Stalemate",
        r"Minor Victory",
        r"Major Victory",
        r"Overwhelming Victory",
        r"Attacker Losses",
        r"Defender Losses",
        r"Land Taken",
    ]
    return any(re.search(pat, t, flags=re.IGNORECASE) for pat in report_markers)


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


def _open_chat(page, chat_name: str) -> bool:
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
                return True

            result = page.get_by_text(chat_name, exact=False).first
            if result.count() and result.is_visible(timeout=1200):
                result.click(timeout=2500)
                page.wait_for_timeout(700)
                return True

            # Some UIs let Enter open top search result.
            search_box.press("Enter")
            page.wait_for_timeout(1000)
            if page.locator("[role='main']").first.count():
                return True
        except Exception:
            continue
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
                    if not messages:
                        continue

                    last_hash = state.get(chat_name, "")
                    pending: List[str] = []
                    for m in messages:
                        h = _sha(m)
                        if h == last_hash:
                            pending = []
                        else:
                            pending.append(m)

                    for m in pending:
                        if not _is_report_text(m):
                            continue
                        mid = _sha(f"{chat_name}|{m}")[:24]
                        external_id = f"{chat_name}:{mid}"
                        result = _bridge_post(m, external_id)
                        print(f"INFO: bridge {chat_name} status={result.get('status')} ok={result.get('ok')}")
                        state[chat_name] = _sha(m)
                        _save_state(state)

                time.sleep(max(3, FB_POLL_SECONDS))
        except KeyboardInterrupt:
            print("INFO: stopping bridge worker")
        finally:
            context.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
