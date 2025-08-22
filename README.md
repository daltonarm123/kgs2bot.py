KG2 Recon Bot is a Discord tool for your game that:

Auto-captures spy reports when players paste them in chat and saves them into a database.

Parses the reports to extract key info like castles, resources, troops (including Pike counts), defensive power (DP), and market/movement logs.

Provides planning tools:

!ap → shows the defender’s DP with castle bonus and how much attack power (AP) is needed for Minor, Victory, Major, or Overwhelming outcomes.

Always includes a Cavalry vs Pike tip, telling you how many Cavalry would be needed to deny Pike bonuses.

Tracks report history so you can look up past spy reports or export them as JSON.

Watches for attack reports in chat and automatically posts an estimated updated DP for the defender after one hit, using result tier (Stalemate, Minor, Victory, Major, Overwhelming) plus attacker losses and land taken to estimate troop attrition.

Includes admin/debug tools to rescan reports if parsing missed anything and to toggle auto-capture per channel or server.

⚙️ How it works:

It listens for messages in Discord.

If a message looks like a spy or attack report, it parses the text with regex patterns, stores structured data in SQLite, then posts a formatted embed with calculations.

Estimates (like post-attack DP) are heuristic—based on result tiers, casualty ratios, and land gained—so they’re guidance, not exact.
