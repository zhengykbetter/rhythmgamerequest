import sys
import json
from datetime import datetime
from pathlib import Path

# 1. 导入 Config (移除对 config.settings.CSV_TARGET_DIR 的依赖，改用 Config)
CURRENT_FILE = Path(__file__).resolve()
if str(CURRENT_FILE.parents[1]) not in sys.path:
    sys.path.insert(0, str(CURRENT_FILE.parents[1]))

from server.config import Config
from server.services.db_service import get_year_today_songs

def get_history_today_data():
    # 2. 使用 Config 中的缓存目录
    cache_dir = Config.HISTORY_TODAY_CACHE_DIR
    
    date_key = datetime.now().strftime("%m%d")
    cache_file = cache_dir / f"{date_key}.json"

    # 读取缓存
    if cache_file.exists():
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"[缓存错误] {e}")

    # 查询数据库
    raw_data = get_year_today_songs()

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
    print("🔥 配置中心化测试：查询数据库生成往年今日")
    data = get_history_today_data()
    print("✅ 最终数据：", data)