import sys
import os
import json
from datetime import datetime
from pathlib import Path

# 强制修复路径
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "frontend"))

from server.services.db_service import get_year_today_songs
from config.settings import CSV_TARGET_DIR

# 强制创建缓存目录
CACHE_DIR = os.path.join(CSV_TARGET_DIR, "history_today")
os.makedirs(CACHE_DIR, exist_ok=True)

def get_history_today_data():
    date_key = datetime.now().strftime("%m-%d")
    cache_file = os.path.join(CACHE_DIR, f"{date_key}.json")

    # 读取缓存
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            pass

    # 获取数据
    raw_data = get_year_today_songs()
    print(f"[服务] 原始数据：{raw_data}")

    # 拼接文案
    result = []
    for item in raw_data:
        year = item.get("year", "未知")
        game = item.get("game", "未知游戏")
        author = item.get("author", "未知曲师")
        song = item.get("song", "未知歌曲")
        is_original = item.get("is_original", 0)

        if is_original == 1:
            result.append(f"{year}年，{game}发布了原创歌曲：曲师{author}的《{song}》")
        else:
            result.append(f"{year}年，{game}收录了歌曲：曲师{author}的《{song}》")

    # 🔥 强制写入文件（必生成，必不为空）
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    print(f"[服务] 强制写入缓存：{cache_file}")
    return result

# 手动运行强制生成
if __name__ == '__main__':
    print("🔥 强制生成往年今日数据...")
    data = get_history_today_data()
    print("✅ 生成成功：", data)