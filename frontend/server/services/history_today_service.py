import sys
import os
import json
from datetime import datetime
from pathlib import Path

# 【修复】保留你原有的路径逻辑，确保能找到 db_service 和 settings
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "frontend"))

from server.services.db_service import get_year_today_songs
# 【新增】同时导入 Config 来管理路径
if str(CURRENT_FILE.parents[1]) not in sys.path:
    sys.path.insert(0, str(CURRENT_FILE.parents[1]))
from server.config import Config

def get_history_today_data():
    # 【修复】使用 Config 的缓存目录，但逻辑完全不变
    cache_dir = Config.HISTORY_TODAY_CACHE_DIR
    # 双重保险，防止目录未创建
    os.makedirs(cache_dir, exist_ok=True)
    
    date_key = datetime.now().strftime("%m%d")
    cache_file = os.path.join(cache_dir, f"{date_key}.json")

    # 读取缓存 (完全保留你原有的逻辑)
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"[缓存错误] {e}")

    # 【关键】这里还是调用你原来的 db_service 函数
    raw_data = get_year_today_songs()

    # 拼接文案 (完全保留你原有的逻辑)
    result = []
    if raw_data:
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
    else:
        result = ["暂无历史数据"]

    # 写入缓存
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    return result

if __name__ == '__main__':
    print("🔥 测试往年今日")
    data = get_history_today_data()
    print("✅ 结果：", data)