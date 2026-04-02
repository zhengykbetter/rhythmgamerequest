import os
import json
from datetime import datetime
from server.services.db_service import get_year_today_songs
# 导入你项目的配置（和现有代码保持一致）
from config.settings import CSV_TARGET_DIR

# ===================== 缓存配置 =====================
# 缓存文件根目录：沿用项目的CSV目录，新建history_today文件夹
CACHE_DIR = os.path.join(CSV_TARGET_DIR, "history_today")
# 自动创建缓存目录（不存在则创建）
os.makedirs(CACHE_DIR, exist_ok=True)

# ===================== 核心服务函数 =====================
def get_history_today_data():
    """
    往年今日主逻辑：
    1. 读取缓存 → 有则直接返回
    2. 无缓存 → 查询数据库 → 拼接文案 → 保存缓存
    3. 返回最终展示数据
    """
    # 1. 生成当前日期标识：MM-DD（按月日区分缓存）
    today = datetime.now()
    date_key = today.strftime("%m-%d")
    cache_file = os.path.join(CACHE_DIR, f"{date_key}.json")

    # 2. 优先读取本地缓存（存在直接返回，不重复查询）
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"[缓存读取失败] {e}，将重新查询数据库")

    # 3. 无缓存 → 调用数据库查询
    raw_data = get_year_today_songs()
    if not raw_data:
        # 无数据时保存空缓存，避免重复查询
        save_cache(cache_file, ["暂无历史上的今日歌曲数据"])
        return ["暂无历史上的今日歌曲数据"]

    # 4. 按你的规则拼接文案（核心逻辑）
    result_list = []
    for item in raw_data:
        year = item["收录年份"]
        game = item["游戏名称"]
        author = item["曲师"]
        song = item["歌曲名称"]
        is_original = item["是否原创"]

        if is_original == 1:
            # 原创歌曲文案
            text = f"{year}年，{game}发布了原创歌曲：曲师{author}的《{song}》"
        else:
            # 收录歌曲文案
            text = f"{year}年，{game}收录了歌曲：曲师{author}的《{song}》"
        
        result_list.append(text)

    # 5. 保存缓存（永久保存，不删除）
    save_cache(cache_file, result_list)

    # 6. 返回最终数据
    return result_list

# ===================== 工具函数 =====================
def save_cache(file_path, data):
    """保存数据到JSON缓存文件"""
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[缓存保存失败] {e}")