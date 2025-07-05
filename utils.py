from datetime import datetime
from urllib.parse import urlparse
import re

def shorten_date(date_str: str, lang: str = "ru") -> str:
    try:
        dt = datetime.fromisoformat(date_str)
        return dt.strftime("%d.%m") if lang == "ru" else dt.strftime("%m/%d")
    except Exception:
        return "??.??"

def normalize_shop_name(raw_shop: str) -> str:
    raw_shop = raw_shop.strip()
    if raw_shop.startswith("http://") or raw_shop.startswith("https://"):
        netloc = urlparse(raw_shop).netloc
    elif "." in raw_shop and " " not in raw_shop:
        netloc = urlparse("https://" + raw_shop).netloc
    else:
        return raw_shop.capitalize()

    domain = netloc.replace("www.", "")
    parts = domain.split(".")
    if len(parts) >= 2:
        return parts[-2].capitalize()
    return domain.capitalize()

def format_comment(raw_note: str) -> str:
    note = raw_note.strip()
    if note.lower().startswith("almost"):
        digits = ''.join(filter(str.isdigit, note))
        return f"${digits}" if digits else note

    if 'x' in note.lower():
        parts = note.lower().split('x')
        if len(parts) == 2 and all(p.strip().isdigit() for p in parts):
            return f"{parts[0].strip()} * ${parts[1].strip()}"

    if note.startswith("$") or note.startswith("€"):
        return note.strip()

    if note.isdigit():
        return f"${note.strip()}"

    return note

def normalize_amount(raw_amount: str) -> str:
    raw_amount = raw_amount.strip().replace(" ", "")
    raw_amount_lower = raw_amount.lower()

    if re.fullmatch(r"(\$|€)\d{2,5}", raw_amount) or re.fullmatch(r"\d{2,5}€", raw_amount):
        return raw_amount.replace("€", "€").replace("$", "$")

    match = re.fullmatch(r"(\d{1,2})[x\*](\d{1,4})", raw_amount_lower)
    if match:
        count, value = match.groups()
        return f"{count} * ${value}"

    match = re.search(r"\d{2,5}", raw_amount)
    if match:
        value = match.group(0)
        if value == "00000" or int(value) < 10:
            return ""
        return f"${value}"

    return ""
