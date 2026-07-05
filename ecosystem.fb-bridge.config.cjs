module.exports = {
  apps: [
    {
      name: 'fb-messenger-bridge',
      script: 'fb_messenger_bridge.py',
      interpreter: '.venv/bin/python',
      cwd: '/workspaces/kgs2bot.py',
      autorestart: true,
      max_restarts: 20,
      restart_delay: 5000,
      env_file: '.env.fbbridge'
    }
  ]
};
