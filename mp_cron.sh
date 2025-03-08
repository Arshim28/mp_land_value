#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_DIR=$(dirname "$SCRIPT_DIR")
SCRAPER_SCRIPT="$PROJECT_DIR/main.py"
WATCHDOG_SCRIPT="$PROJECT_DIR/watchdog.py"
LOG_DIR="$PROJECT_DIR/logs"
CRON_LOG="$LOG_DIR/cron_execution.log"

chmod +x "$SCRAPER_SCRIPT"
chmod +x "$WATCHDOG_SCRIPT"

mkdir -p "$LOG_DIR"

DAILY_CMD="0 3 * * * cd $PROJECT_DIR && /usr/bin/python3 $SCRAPER_SCRIPT >> $CRON_LOG 2>&1"

WATCHDOG_CMD="0 * * * * cd $PROJECT_DIR && /usr/bin/python3 $WATCHDOG_SCRIPT >> $LOG_DIR/watchdog_cron.log 2>&1"

(crontab -l 2>/dev/null | grep -v "$SCRAPER_SCRIPT" | grep -v "$WATCHDOG_SCRIPT") | crontab -
(crontab -l 2>/dev/null; echo "$DAILY_CMD"; echo "$WATCHDOG_CMD") | crontab -

echo "Cron jobs installed:"
echo "1. Main script will run once. This should be completed in one run"
echo "2. Watchdog script will run every hour to check if the main script is running"
echo "To view or modify the cron jobs, use: crontab -e"