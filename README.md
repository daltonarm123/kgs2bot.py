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

Oven/training inference:

When the bot has at least two recent SR troop snapshots for a kingdom, it can compare missing Peasants/Population against a NW jump and estimate likely troops in training.

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
OVEN_LIGHT_CAVALRY_NW=0.25
OVEN_LIGHT_CAVALRY_MINUTES_PER_1000=90
OVEN_PIKEMEN_PEASANTS=1
OVEN_PIKEMEN_NW=0.25
OVEN_FOOTMEN_PEASANTS=1
OVEN_FOOTMEN_NW=0.10
OVEN_HEAVY_CAVALRY_PEASANTS=1
OVEN_HEAVY_CAVALRY_NW=0.40
OVEN_HEAVY_CAVALRY_MINUTES_PER_1000=120

Notes:
- The default constants are useful starting points, not guaranteed game truth.
- If several units have the same Peasant/NW footprint, the bot intentionally lists multiple likely outcomes.
- Accuracy is best when SRs are fresh and the target did not dump NW, fight, or receive returning armies between reports.

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
KG_GAME_RANKINGS_CONTINENT_ID=-1

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

Required Railway variables:

```text
DISCORD_TOKEN=...
DATABASE_URL=...
```

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
