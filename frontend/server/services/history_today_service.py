import sys
import os
import json
from datetime import datetime
from pathlib import Path

# 路径修复
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "frontend"))

from server.services.db_service import get_year_today_songs
from config.settings import CSV_TARGET_DIR

# 缓存配置
CACHE_DIR = os.path.join(CSV_TARGET_DIR, "history_today")
os.makedirs(CACHE_DIR, exist_ok=True)

def get_history_today_data():
    # 文件名：0402（匹配你的文件格式）
    date_key = datetime.now().strftime("%m%d")
    cache_file = os.path.join(CACHE_DIR, f"{date_key}.json")

    # 读取缓存
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"[缓存错误] {e}")

    # 软编码：查询数据库
    raw_data = get_year_today_songs()
    print(f"[服务] 数据库返回原始数据：{raw_data}")

    # 拼接文案
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

# 手动测试
if __name__ == '__main__':
    print("🔥 软编码测试：查询数据库生成往年今日")
    data = get_history_today_data()
    print("✅ 最终数据：", data)