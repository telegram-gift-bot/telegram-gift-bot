#!/bin/bash
BOT_DIR="/root/richi_gift_bot"
VENV="$BOT_DIR/venv"
LOG_FILE="$BOT_DIR/bot_output.log"
source "$VENV/bin/activate"
cd "$BOT_DIR"
nohup python3 main.py >> "$LOG_FILE" 2>&1 &
echo "Бот запущен. Логи: $LOG_FILE"