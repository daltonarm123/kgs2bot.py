# KG2 Recon Bot

KG2 Recon Bot is a Discord worker for KingdomGame recon, planning, and alerts.

## What It Does

Auto-captures spy reports when players paste them in chat and saves them into a database.

Parses the reports to extract key info like castles, resources, troops (including Pike counts), defensive power (DP), and market/movement logs.

Provides planning tools:

!ap → shows the defender’s DP with castle bonus and how much attack power (AP) is needed for Minor, Victory, Major, or Overwhelming outcomes.

Always includes a Cavalry vs Pike tip, telling you how many Cavalry would be needed to deny Pike bonuses.

Tracks report history so you can look up past spy reports or export them as JSON.

Watches for attack reports in chat and automatically posts an estimated updated DP for the defender after one hit, using result tier (Stalemate, Minor, Victory, Major, Overwhelming) plus attacker losses and land taken to estimate troop attrition.

Includes admin/debug tools to rescan reports if parsing missed anything and to toggle auto-capture per channel or server.

New: NW jump alerts from live rankings.

The bot can poll KingdomGame rankings and alert your Discord when a kingdom gains net worth by a configured amount (default 5,000+) between polls.

The rankings poller runs automatically on the configured cadence and keeps the top 100 kingdoms in scope for history, alerts, and live intel.

Rankings alerts can also notify when the game shows pie status next to a kingdom name, which is useful for spotting kingdoms that were recently hit.

Live kingdom intelligence:

The bot now stores rankings snapshots over time so you can inspect a target's current rank, net worth, pie state, recent attack activity, and how those changed over a chosen lookback window.

Command usage:

!kingdomlive <kingdom>
!kingdomlive <kingdom> <hours>
!rankingsrefresh

Examples:

!kingdomlive Seven
!kingdomlive Seven 6
!rankingsrefresh

Oven/training inference:

When the bot has at least two recent SR reports for a kingdom, it can compare missing Peasants/Population against a NW jump and estimate likely troops in training. It uses the report Date/Received timestamp when present, so stale reports pasted later do not count as fresh oven data.

Command usage:

!oven <kingdom>

Example:

!oven Seven

NW jump alerts also append a compact oven guess when a positive NW jump matches recent SR history.

Tuning variables for inference constants:

OVEN_ESTIMATOR_ENABLED=true
OVEN_LOOKBACK_HOURS=36
OVEN_MAX_ALERT_LINES=3
OVEN_LIGHT_CAVALRY_PEASANTS=1
OVEN_LIGHT_CAVALRY_NW=0.50
OVEN_LIGHT_CAVALRY_MINUTES_PER_1000=90
OVEN_PIKEMEN_PEASANTS=1
OVEN_PIKEMEN_NW=0.50
OVEN_ARCHERS_PEASANTS=1
OVEN_ARCHERS_NW=0.50
OVEN_CROSSBOWMEN_PEASANTS=1
OVEN_CROSSBOWMEN_NW=0.38
OVEN_FOOTMEN_PEASANTS=1
OVEN_FOOTMEN_NW=0.38
OVEN_HEAVY_CAVALRY_PEASANTS=1
OVEN_HEAVY_CAVALRY_NW=0.63
OVEN_HEAVY_CAVALRY_MINUTES_PER_1000=120
OVEN_KNIGHTS_PEASANTS=1
OVEN_KNIGHTS_NW=1.63

Notes:
- The default constants are useful starting points, not guaranteed game truth.
- If several units have the same Peasant/NW footprint, the bot intentionally lists multiple likely outcomes.
- Accuracy is best when SRs are fresh and the target did not dump NW, fight, or receive returning armies between reports.
- The default oven lookback is 36 hours; set OVEN_LOOKBACK_HOURS higher/lower if your alert cycle needs it.

Command usage:

!nwjumpalerts status
!nwjumpalerts on [threshold]
!nwjumpalerts off
!nwjumpcheck

Example:

!nwjumpalerts on 5000

When enabled, alerts are posted in the channel where you ran the command.

Environment variables for this feature:

NW_JUMP_ALERTS_ENABLED=true
NW_JUMP_ALERT_POLL_SECONDS=60
NW_JUMP_ALERT_DEFAULT_THRESHOLD=5000
KG_GAME_PIE_ALERTS_ENABLED=true
KINGDOM_LIVE_DEFAULT_LOOKBACK_HOURS=1
KINGDOM_LIVE_ATTACK_WINDOW_HOURS=24
KG_GAME_RANKINGS_CONTINENT_ID=-1

Notes:
- `NW_JUMP_ALERT_POLL_SECONDS` controls how often the bot refreshes the live top-100 rankings automatically.
- `!rankingsrefresh` lets an admin force an immediate rankings refresh into current state/history and trigger any NW/pie alerts right away.

Optional SMS fanout (Twilio):

ALERT_SMS_ENABLED=true
ALERT_SMS_TWILIO_ACCOUNT_SID=...
ALERT_SMS_TWILIO_AUTH_TOKEN=...
ALERT_SMS_TWILIO_API_KEY_SID=...
ALERT_SMS_TWILIO_API_KEY_SECRET=...
ALERT_SMS_TWILIO_FROM=+1XXXXXXXXXX
ALERT_SMS_TO=+1XXXXXXXXXX,+1YYYYYYYYYY
ALERT_SMS_WATCHLIST=+15551234567=Magic Dude|Northeast|Galileo;+15557654321=623|565
ALERT_SMS_MAX_PER_ALERT=10

Notes:
- You must set ALERT_SMS_TWILIO_ACCOUNT_SID (starts with AC...).
- Auth can be either ALERT_SMS_TWILIO_AUTH_TOKEN or ALERT_SMS_TWILIO_API_KEY_SID + ALERT_SMS_TWILIO_API_KEY_SECRET.
- If ALERT_SMS_WATCHLIST is set, each phone only receives alerts for matching watched kingdoms (by exact name or kingdom ID).

## Railway Deployment

This repo includes [railway.json](railway.json) so Railway runs the bot as a worker with:

```bash
python kg2bot.py
```

## Local Testing Before Push

Use the local smoke tests before pushing bot logic changes. These tests do not connect to Discord or Postgres; they import the bot with dummy env vars and exercise pure parser/filtering logic.

```bash
python -m unittest discover -s tests
```

For real Discord command testing, use a separate test bot and test server/channel instead of the production bot:

1. Create a second Discord application/bot token for testing.
2. Invite that test bot to a private test server.
3. Use a separate test Postgres database or schema.
4. Set test env vars locally or in a separate Railway/Render test service:

```bash
DISCORD_TOKEN=<test bot token>
DATABASE_URL=<test database url>
TARGET_GUILD_ID=<test server id>
UPDATES_CHANNEL_ID=<test updates channel id>
LIVE_BATTLE_CHANNEL_ID=<test battle channel id>
RECON_INGEST_ENABLED=false
BACKFILL_FORWARD_ENABLED=false
```

Then run the worker against that test environment only:

```bash
python kg2bot.py
```

Use the production bot only after the local smoke tests pass and the change has been checked in the test Discord server when it affects command output or live alerts.

Required Railway variables:

```text
DISCORD_TOKEN=...
DATABASE_URL=...
```

Messenger bridge receiver variables (for automated forwarding from a local Messenger watcher):

```text
BRIDGE_HTTP_ENABLED=true
BRIDGE_HTTP_TOKEN=<long-random-secret>
BRIDGE_HTTP_PATH=/api/bridge/report
BRIDGE_HTTP_REQUIRE_RECON_MATCH=true
```

Optional bridge variables:

```text
BRIDGE_HTTP_BIND=0.0.0.0
BRIDGE_HTTP_PORT=8080
```

Bridge endpoint behavior:

- `POST /api/bridge/report` accepts JSON with `raw_text` (or `text`), plus optional `source`, `external_id`, and `sent_at`.
- Auth header: `X-Bridge-Token: <token>` (or `Authorization: Bearer <token>`).
- Incoming events are deduped in `bridge_ingest_events` before processing.
- If the text looks like a recon report, it is saved locally and forwarded to `RECON_INGEST_URL`.
- Health endpoint: `GET /healthz`.

## Facebook Messenger Bridge Worker

Use the local worker in [fb_messenger_bridge.py](fb_messenger_bridge.py) to watch Messenger chats and post report text into the Railway bridge endpoint.

Install dependency and browser once:

```bash
pip install -r requirements.txt
python -m playwright install chromium
```

Recommended environment variables:

```text
BRIDGE_HTTP_URL=https://kg2recon-production.up.railway.app/api/bridge/report
BRIDGE_HTTP_TOKEN=<same token set in Railway>
FB_MESSENGER_EMAIL=<your fb login email>
FB_MESSENGER_PASSWORD=<your fb login password>
FB_MESSENGER_CHAT_NAMES=mom's knights in training,a team only
FB_MESSENGER_HEADLESS=false
FB_MESSENGER_POLL_SECONDS=15
FB_MESSENGER_PROFILE_DIR=.fb_messenger_profile
```

Run the watcher:

```bash
python fb_messenger_bridge.py
```

Notes:
- First run may require manual checkpoint/2FA approval in the browser.
- The worker keeps a local browser profile dir so you do not need to log in every time.
- Only report-like messages are forwarded (spy/recon and attack-style markers).

Recommended variables for this bot:

```text
TARGET_GUILD_ID=1405247393112395866
UPDATES_CHANNEL_ID=...
LIVE_BATTLE_CHANNEL_ID=...
KG_REPORT_DEFAULT_TZ=UTC
NW_JUMP_ALERTS_ENABLED=true
```

To link this checkout to the existing Railway project, run:

```bash
railway link
railway status
```

Then deploy with:

```bash
railway up
```

The local `.railway/` folder is ignored because it contains machine-specific project link metadata. Configure secrets in Railway, not in git.

⚙️ How it works:

It listens for messages in Discord.

If a message looks like a spy or attack report, it parses the text with regex patterns, stores structured data in PostgreSQL, then posts a formatted embed with calculations.

Estimates (like post-attack DP) are heuristic—based on result tiers, casualty ratios, and land gained—so they’re guidance, not exact.
