#!/bin/bash
BOT_DIR="/root/richi_gift_bot"
BACKUP_DIR="/root/richi_gift_bot_backup_$(date +%F_%T)"
systemctl stop giftbot.service 2>/dev/null || true
if [ -d "$BOT_DIR" ]; then
    echo "[+] Бэкап текущей папки в: $BACKUP_DIR"
    mv "$BOT_DIR" "$BACKUP_DIR"
fi
mkdir -p "$BOT_DIR"
cd "$BOT_DIR"
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
chmod +x start.sh
./start.sh
echo "✅ Новый бот развернут. Старый в: $BACKUP_DIR"
