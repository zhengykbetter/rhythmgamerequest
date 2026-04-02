import os
import json
from datetime import datetime
from server.services.db_service import get_year_today_songs
from config.settings import CSV_TARGET_DIR

# 缓存目录
CACHE_DIR = os.path.join(CSV_TARGET_DIR, "history_today")
os.makedirs(CACHE_DIR, exist_ok=True)

def get_history_today_data():
    # 日期标识
    date_key = datetime.now().strftime("%m-%d")
    cache_file = os.path.join(CACHE_DIR, f"{date_key}.json")
    
    print(f"[缓存] 尝试读取：{cache_file}")

    # 读缓存
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                print(f"[缓存] 读取成功，数据：{data}")
                return data
        except Exception as e:
            print(f"[缓存错误] {e}")

    # 查数据库
    print("[缓存] 无文件，查询数据库")
    raw_data = get_year_today_songs()

    # 无数据处理
    if not raw_data:
        empty_data = ["暂无今日历史数据"]
        save_cache(cache_file, empty_data)
        return empty_data

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

    # 保存缓存
    print(f"[缓存] 保存文件：{cache_file}")
    save_cache(cache_file, result)
    return result

def save_cache(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# ===================== 强制生成缓存（手动运行，必创建文件） =====================
if __name__ == '__main__':
    print("🔥 手动强制生成往年今日缓存...")
    result = get_history_today_data()
    print(f"✅ 生成完成！数据：{result}")
    print(f"📁 缓存文件已自动创建")