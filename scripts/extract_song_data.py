#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
歌曲数据提取脚本：从原始song_info CSV生成各表同步用CSV
核心功能：
1. 读取原始song_info（含song_id/歌名/别名/作者/来源/本家/更新时间/真实作者）
2. 生成song_info/author_info/game_song_rel/song_author_rel/game_linkage_rel的CSV
3. 路径软编码（复用settings.py的CSV_TARGET_DIR）
4. 自动去重、格式标准化、空值处理
"""
import os
import sys
import pandas as pd
from datetime import datetime

# ===================== 路径配置（软编码，复用settings.py） =====================
# 获取当前脚本所在目录
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# 主项目根目录（脚本目录的上一级，对应config目录的同级）
MAIN_PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
# 将主项目根目录加入Python路径，导入settings.py
sys.path.insert(0, MAIN_PROJECT_ROOT)

# 导入settings.py中的CSV_TARGET_DIR（和game_info同路径）
from config.settings import CSV_TARGET_DIR

# 定义输入输出路径
RAW_SONG_CSV_PATH = os.path.join(CSV_TARGET_DIR, "song_info_raw.csv")  # 原始输入CSV
OUTPUT_PATHS = {
    "song_info": os.path.join(CSV_TARGET_DIR, "song_info.csv"),
    "author_info": os.path.join(CSV_TARGET_DIR, "author_info.csv"),
    "game_song_rel": os.path.join(CSV_TARGET_DIR, "game_song_rel.csv"),
    "song_author_rel": os.path.join(CSV_TARGET_DIR, "song_author_rel.csv"),
    "game_linkage_rel": os.path.join(CSV_TARGET_DIR, "game_linkage_rel.csv")
}

# ===================== 核心工具函数 =====================
def clean_string(val):
    """清洗字符串：空值转空字符串，去除首尾空格"""
    if pd.isna(val) or val == "":
        return ""
    return str(val).strip()

def get_8digit_date(date_str):
    """将日期转换为8位格式（YYYYMMDD），失败返回空"""
    if pd.isna(date_str) or date_str == "":
        return ""
    try:
        # 兼容常见日期格式：2026-03-23 / 2026/03/23 / 2026.03.23
        date_obj = pd.to_datetime(date_str, errors="coerce")
        if pd.notna(date_obj):
            return date_obj.strftime("%Y%m%d")
        return ""
    except:
        return ""

def get_standard_date(date_str):
    """将日期转换为标准格式（YYYY-MM-DD），失败返回空"""
    if pd.isna(date_str) or date_str == "":
        return ""
    try:
        date_obj = pd.to_datetime(date_str, errors="coerce")
        if pd.notna(date_obj):
            return date_obj.strftime("%Y-%m-%d")
        return ""
    except:
        return ""

def generate_author_id(author_name, author_map, start_num=1):
    """生成自增作者编码（A001/A002...），去重"""
    if author_name == "":
        return ""
    if author_name not in author_map:
        author_id = f"A{str(start_num + len(author_map)).zfill(3)}"
        author_map[author_name] = author_id
    return author_map[author_name]

# ===================== 数据提取主逻辑 =====================
def extract_song_data():
    # 1. 校验原始文件是否存在
    if not os.path.exists(RAW_SONG_CSV_PATH):
        print(f"❌ 错误：原始song_info文件不存在 → {RAW_SONG_CSV_PATH}")
        print(f"⚠️  请将原始song_info CSV命名为 song_info_raw.csv 并放到 {CSV_TARGET_DIR} 目录下")
        return False

    # 2. 读取原始数据并清洗
    print(f"📖 读取原始song_info数据 → {RAW_SONG_CSV_PATH}")
    df_raw = pd.read_csv(
        RAW_SONG_CSV_PATH,
        encoding="utf-8",
        dtype=str,
        na_filter=True
    )
    # 确保必要列存在
    required_cols = ["song_id", "歌名", "别名", "作者", "来源", "本家", "更新时间", "真实作者"]
    missing_cols = [col for col in required_cols if col not in df_raw.columns]
    if missing_cols:
        print(f"❌ 错误：原始CSV缺少必要列 → {missing_cols}")
        return False

    # 清洗所有字段
    df_raw["song_id"] = df_raw["song_id"].apply(clean_string)
    df_raw["歌名"] = df_raw["歌名"].apply(clean_string)
    df_raw["别名"] = df_raw["别名"].apply(clean_string)
    df_raw["作者"] = df_raw["作者"].apply(clean_string)
    df_raw["来源"] = df_raw["来源"].apply(clean_string)
    df_raw["本家"] = df_raw["本家"].apply(clean_string)
    df_raw["更新时间"] = df_raw["更新时间"].apply(clean_string)
    df_raw["真实作者"] = df_raw["真实作者"].apply(clean_string)

    # 过滤无效行（song_id为空的行）
    df_raw = df_raw[df_raw["song_id"] != ""].reset_index(drop=True)
    if len(df_raw) == 0:
        print("❌ 错误：原始数据中无有效song_id行")
        return False

    # 3. 处理author_info表（自增ID，去重）
    print("🔧 处理作者表（author_info）...")
    author_map = {}  # 真实作者名 → 自增ID
    author_data = []
    for idx, row in df_raw.iterrows():
        # 真实作者为空则用名义作者
        real_author = row["真实作者"] if row["真实作者"] != "" else row["作者"]
        if real_author == "":
            continue
        # 生成自增ID
        author_id = generate_author_id(real_author, author_map)
        # 构建作者数据（作者本名/别称/擅长风格/备注）
        author_data.append({
            "author_id": author_id,
            "作者本名": real_author,
            "作者别称": "",  # 无相关信息
            "擅长风格": "",  # 无相关信息
            "备注": ""       # 无相关信息
        })
    # 去重并保存
    df_author = pd.DataFrame(author_data).drop_duplicates(subset=["author_id"]).reset_index(drop=True)
    df_author.to_csv(
        OUTPUT_PATHS["author_info"],
        encoding="utf-8",
        index=False
    )
    print(f"✅ 生成author_info CSV → {OUTPUT_PATHS['author_info']}（共{len(df_author)}条作者）")

    # 4. 处理song_info表（歌名/别名/作者/更新时间）
    print("🔧 处理歌曲表（song_info）...")
    song_data = []
    for idx, row in df_raw.iterrows():
        song_data.append({
            "song_id": row["song_id"],
            "歌名": row["歌名"],
            "别名": row["别名"],
            "更新时间": get_standard_date(row["更新时间"])  # 标准日期格式
        })
    # 去重并保存
    df_song = pd.DataFrame(song_data).drop_duplicates(subset=["song_id"]).reset_index(drop=True)
    df_song.to_csv(
        OUTPUT_PATHS["song_info"],
        encoding="utf-8",
        index=False
    )
    print(f"✅ 生成song_info CSV → {OUTPUT_PATHS['song_info']}（共{len(df_song)}条歌曲）")

    # 5. 处理game_song_rel表（歌-游戏关联，来源=游戏编号）
    print("🔧 处理歌-游戏关联表（game_song_rel）...")
    game_song_data = []
    for idx, row in df_raw.iterrows():
        song_id = row["song_id"]
        game_id = row["来源"]  # 来源外联到game_info的游戏编号
        update_time = row["更新时间"]
        if game_id == "":
            continue
        # 生成rel_id（song_id_游戏编号_8位日期）
        date_8digit = get_8digit_date(update_time)
        rel_id = f"{song_id}_{game_id}_{date_8digit}" if date_8digit != "" else f"{song_id}_{game_id}"
        # 构建歌-游戏数据
        game_song_data.append({
            "rel_id": rel_id,
            "游戏编号": game_id,
            "song_id": song_id,
            "收录版本": "",  # 无相关信息
            "收录时间": get_standard_date(update_time)
        })
    # 去重并保存
    df_game_song = pd.DataFrame(game_song_data).drop_duplicates(subset=["rel_id"]).reset_index(drop=True)
    df_game_song.to_csv(
        OUTPUT_PATHS["game_song_rel"],
        encoding="utf-8",
        index=False
    )
    print(f"✅ 生成game_song_rel CSV → {OUTPUT_PATHS['game_song_rel']}（共{len(df_game_song)}条关联）")

    # 6. 处理song_author_rel表（歌-作者关联，用真实作者）
    print("🔧 处理歌-作者关联表（song_author_rel）...")
    song_author_data = []
    for idx, row in df_raw.iterrows():
        song_id = row["song_id"]
        # 真实作者为空则用名义作者
        real_author = row["真实作者"] if row["真实作者"] != "" else row["作者"]
        if real_author == "" or real_author not in author_map:
            continue
        author_id = author_map[real_author]
        # 生成rel_id（song_id_author_id）
        rel_id = f"{song_id}_{author_id}"
        # 构建歌-作者数据
        song_author_data.append({
            "rel_id": rel_id,
            "song_id": song_id,
            "author_id": author_id,
            "合作类型": "",  # 无相关信息
            "备注": ""       # 无相关信息
        })
    # 去重并保存
    df_song_author = pd.DataFrame(song_author_data).drop_duplicates(subset=["rel_id"]).reset_index(drop=True)
    df_song_author.to_csv(
        OUTPUT_PATHS["song_author_rel"],
        encoding="utf-8",
        index=False
    )
    print(f"✅ 生成song_author_rel CSV → {OUTPUT_PATHS['song_author_rel']}（共{len(df_song_author)}条关联）")

    # 7. 处理game_linkage_rel表（联动信息：来源/本家/更新时间非空且不同）
    print("🔧 处理游戏联动表（game_linkage_rel）...")
    linkage_data = []
    for idx, row in df_raw.iterrows():
        source = row["来源"]
        home = row["本家"]
        update_time = row["更新时间"]
        # 筛选条件：来源/本家/更新时间非空，且来源≠本家
        if source == "" or home == "" or update_time == "" or source == home:
            continue
        # 生成rel_id（来源_本家_8位日期）
        date_8digit = get_8digit_date(update_time)
        rel_id = f"{source}_{home}_{date_8digit}" if date_8digit != "" else f"{source}_{home}"
        # 构建联动数据（本家不外联，游戏2编号填0）
        linkage_data.append({
            "rel_id": rel_id,
            "游戏1编号": source,
            "游戏2编号": "0",  # 本家不外联到game_info
            "游戏1名称": "",   # 需从game_info查询，暂空
            "游戏2名称": home,
            "联动名称": "",    # 无相关信息
            "联动时间": get_standard_date(update_time),
            "联动版本": "",    # 无相关信息
            "说明": ""         # 无相关信息
        })
    # 去重并保存
    df_linkage = pd.DataFrame(linkage_data).drop_duplicates(subset=["rel_id"]).reset_index(drop=True)
    df_linkage.to_csv(
        OUTPUT_PATHS["game_linkage_rel"],
        encoding="utf-8",
        index=False
    )
    print(f"✅ 生成game_linkage_rel CSV → {OUTPUT_PATHS['game_linkage_rel']}（共{len(df_linkage)}条联动）")

    # 8. 最终汇总
    print("\n🎉 所有表CSV生成完成！")
    for table, path in OUTPUT_PATHS.items():
        if os.path.exists(path):
            df = pd.read_csv(path, encoding="utf-8")
            print(f"   • {table}.csv → {path}（{len(df)}条数据）")
    return True

# ===================== 主函数 =====================
if __name__ == "__main__":
    # 创建输出目录（确保存在）
    os.makedirs(CSV_TARGET_DIR, exist_ok=True)
    # 执行数据提取
    extract_song_data()