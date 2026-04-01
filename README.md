  
  The full first-run sequence is:

  # 1. Fill in .env, then run setup (validates config, seeds all data, checks APIs)
  uv run python scripts/setup.py

  # 2. First manual optimizer run
  uv run octoopt2

  # 3. Install cron job
  crontab -e
  # Add: */5 * * * * cd /path/to/octoopt2 && uv run octoopt2 >> /var/log/octoopt2.log 2>&1
