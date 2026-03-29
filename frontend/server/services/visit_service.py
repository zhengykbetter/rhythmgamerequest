import hashlib
from datetime import date
from server.config import Config

def get_visit_count():
    with open(Config.COUNT_PATH, "r") as f:
        count = int(f.read().strip())
    count += 1
    with open(Config.COUNT_PATH, "w") as f:
        f.write(str(count))
    return count

def get_luck(ip):
    today = str(date.today())
    unique_key = f"{ip}_{today}"
    hash_obj = hashlib.md5(unique_key.encode())
    hash_num = int(hash_obj.hexdigest(), 16)
    return "恭喜你，你的运势是100分" if (hash_num % 10 < 8) else "恭喜你，你的运势是10分"