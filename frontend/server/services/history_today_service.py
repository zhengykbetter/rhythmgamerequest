# ===================== 强制路径修复（解决ModuleNotFoundError） =====================
import sys
import os
from pathlib import Path
# 自动添加项目根目录到Python路径
CURRENT_FILE = Path(__file__).resolve()
# 向上找3级，定位到项目根目录
PROJECT_ROOT = CURRENT_FILE.parents[3]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "frontend"))

# ===================== 正常导入 =====================
import json
from datetime import datetime
from server.services.db_service import get_year_today_songs
from config.settings import CSV_TARGET_DIR

# ===================== 缓存配置（强制创建） =====================
CACHE_DIR = os.path.join(CSV_TARGET_DIR, "history_today")
os.makedirs(CACHE_DIR, exist_ok=True)
print(f"📁 缓存目录已确认：{CACHE_DIR}")

# ===================== 核心业务逻辑 =====================
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

    # 查询数据库
    raw_data = get_year_today_songs()
    print(f"🗄️ 数据库返回数据：{raw_data}")

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

    # 强制保存缓存（必生成JSON！）
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"✅ JSON缓存已生成：{cache_file}")

    return result

# ===================== 手动强制运行（必生成文件） =====================
if __name__ == '__main__':
    print("🔥 开始强制生成往年今日缓存...")
    get_history_today_data()
    print("🎉 执行完成！")