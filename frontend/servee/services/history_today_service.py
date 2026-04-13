import sys
import os
import json
from datetime import datetime
from pathlib import Path

# 路径修复（完全保留原有可用逻辑）
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "frontend"))

from servee.services.db_service import get_year_today_songs
from config.settings import CSV_TARGET_DIR

# 缓存配置
CACHE_DIR = os.path.join(CSV_TARGET_DIR, "history_today")
os.makedirs(CACHE_DIR, exist_ok=True)

def get_history_today_data():
    date_key = datetime.now().strftime("%m%d")
    cache_file = os.path.join(CACHE_DIR, f"{date_key}.json")

    # ===================== 【改动开始】 =====================
    # 1. 先标记：是否需要重新生成（默认False）
    need_refresh = False
    
    # 2. 缓存存在 → 读取并判断是否为无效缓存
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                cache_data = json.load(f)
            
            # ✅ 关键：如果缓存是【暂无历史数据】，强制刷新
            if cache_data == ["暂无历史数据"]:
                need_refresh = True
            else:
                # 有效缓存，直接返回
                return cache_data
        except Exception as e:
            print(f"[缓存错误] {e}")
            need_refresh = True

    # 3. 无缓存 / 无效缓存 → 重新查询数据库
    # ===================== 【改动结束】 =====================

    # 数据库查询（原有逻辑不变）
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

    # 写入新缓存（覆盖旧无效缓存）
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    return result

# 手动测试
if __name__ == '__main__':
    print("🔥 测试往年今日（强制刷新无效缓存）")
    data = get_history_today_data()
    print("✅ 最终数据：", data)