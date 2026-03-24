#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
歌曲数据提取脚本：从原始song_info CSV生成各表同步用CSV
核心功能：
1. 读取原始song_info（含song_id/歌名/别名/作者/来源/本家/更新时间/真实作者）
2. 生成song_info/author_info/game_song_rel/song_author_rel/game_linkage_rel的CSV
3. 路径+文件名全软编码（复用settings.py的配置，无硬编码）
4. 自动去重、格式标准化、空值处理
5. 所有表添加「最新更新时间」字段（脚本处理数据的时间）
"""
import os
import sys
import pandas as pd
from datetime import datetime

# ===================== 路径+文件名配置（全软编码，无硬编码） =====================
# 获取当前脚本所在目录
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# 主项目根目录（脚本目录的上一级）
MAIN_PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
# 将主项目根目录加入Python路径，导入settings.py
sys.path.insert(0, MAIN_PROJECT_ROOT)

# 从settings导入所有配置（路径+文件名）
from config.settings import (
    CSV_TARGET_DIR,
    RAW_SONG_CSV_FILENAME,  # 原始歌曲文件名（从配置读取）
    OUTPUT_CSV_FILENAMES    # 输出表文件名（从配置读取）
)

# 定义输入输出路径（仅拼接，无硬编码）
RAW_SONG_CSV_PATH = os.path.join(CSV_TARGET_DIR, RAW_SONG_CSV_FILENAME)  # 原始输入路径
OUTPUT_PATHS = {
    # 输出路径 = 目标目录 + 配置中的文件名
    "song_info": os.path.join(CSV_TARGET_DIR, OUTPUT_CSV_FILENAMES["song_info"]),
    "author_info": os.path.join(CSV_TARGET_DIR, OUTPUT_CSV_FILENAMES["author_info"]),
    "game_song_rel": os.path.join(CSV_TARGET_DIR, OUTPUT_CSV_FILENAMES["game_song_rel"]),
    "song_author_rel": os.path.join(CSV_TARGET_DIR, OUTPUT_CSV_FILENAMES["song_author_rel"]),
    "game_linkage_rel": os.path.join(CSV_TARGET_DIR, OUTPUT_CSV_FILENAMES["game_linkage_rel"])
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

def get_current_datetime():
    """获取当前时间（标准格式YYYY-MM-DD HH:MM:SS），作为数据最新更新时间"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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
    # 1. 校验原始文件是否存在（提示文案也适配配置的文件名）
    if not os.path.exists(RAW_SONG_CSV_PATH):
        print(f"❌ 错误：原始song_info文件不存在 → {RAW_SONG_CSV_PATH}")
        print(f"⚠️  请将原始song_info CSV命名为 {RAW_SONG_CSV_FILENAME} 并放到 {CSV_TARGET_DIR} 目录下")
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

    # 3. 处理author_info表（新增：最新更新时间）
    print("🔧 处理作者表（author_info）...")
    author_map = {}
    author_data = []
    current_time = get_current_datetime()  # 统一当前时间（所有数据用同一时间戳）
    for idx, row in df_raw.iterrows():
        real_author = row["真实作者"] if row["真实作者"] != "" else row["作者"]
        if real_author == "":
            continue
        author_id = generate_author_id(real_author, author_map)
        # 新增：最新更新时间字段
        author_data.append({
            "author_id": author_id,
            "作者本名": real_author,
            "作者别称": "",
            "擅长风格": "",
            "备注": "",
            "最新更新时间": current_time  # 新增字段
        })
    df_author = pd.DataFrame(author_data).drop_duplicates(subset=["author_id"]).reset_index(drop=True)
    df_author.to_csv(OUTPUT_PATHS["author_info"], encoding="utf-8", index=False)
    print(f"✅ 生成author_info CSV → {OUTPUT_PATHS['author_info']}（共{len(df_author)}条作者）")

    # 4. 处理song_info表（重命名：歌曲更新时间 + 新增：最新更新时间）
    print("🔧 处理歌曲表（song_info）...")
    song_data = []
    current_time = get_current_datetime()
    for idx, row in df_raw.iterrows():
        song_data.append({
            "song_id": row["song_id"],
            "歌名": row["歌名"],
            "别名": row["别名"],
            "歌曲更新时间": get_standard_date(row["更新时间"]),  # 重命名：区分原始歌曲时间
            "最新更新时间": current_time  # 新增：脚本处理时间
        })
    df_song = pd.DataFrame(song_data).drop_duplicates(subset=["song_id"]).reset_index(drop=True)
    df_song.to_csv(OUTPUT_PATHS["song_info"], encoding="utf-8", index=False)
    print(f"✅ 生成song_info CSV → {OUTPUT_PATHS['song_info']}（共{len(df_song)}条歌曲）")

    # 5. 处理game_song_rel表（新增：最新更新时间）
    print("🔧 处理歌-游戏关联表（game_song_rel）...")
    game_song_data = []
    current_time = get_current_datetime()
    for idx, row in df_raw.iterrows():
        song_id = row["song_id"]
        game_id = row["来源"]
        update_time = row["更新时间"]
        if game_id == "":
            continue
        date_8digit = get_8digit_date(update_time)
        rel_id = f"{song_id}_{game_id}_{date_8digit}" if date_8digit != "" else f"{song_id}_{game_id}"
        game_song_data.append({
            "rel_id": rel_id,
            "游戏编号": game_id,
            "song_id": song_id,
            "收录版本": "",
            "收录时间": get_standard_date(update_time),
            "最新更新时间": current_time  # 新增字段
        })
    df_game_song = pd.DataFrame(game_song_data).drop_duplicates(subset=["rel_id"]).reset_index(drop=True)
    df_game_song.to_csv(OUTPUT_PATHS["game_song_rel"], encoding="utf-8", index=False)
    print(f"✅ 生成game_song_rel CSV → {OUTPUT_PATHS['game_song_rel']}（共{len(df_game_song)}条关联）")

    # 6. 处理song_author_rel表（新增：最新更新时间）
    print("🔧 处理歌-作者关联表（song_author_rel）...")
    song_author_data = []
    current_time = get_current_datetime()
    for idx, row in df_raw.iterrows():
        song_id = row["song_id"]
        real_author = row["真实作者"] if row["真实作者"] != "" else row["作者"]
        if real_author == "" or real_author not in author_map:
            continue
        author_id = author_map[real_author]
        rel_id = f"{song_id}_{author_id}"
        song_author_data.append({
            "rel_id": rel_id,
            "song_id": song_id,
            "author_id": author_id,
            "合作类型": "",
            "备注": "",
            "最新更新时间": current_time  # 新增字段
        })
    df_song_author = pd.DataFrame(song_author_data).drop_duplicates(subset=["rel_id"]).reset_index(drop=True)
    df_song_author.to_csv(OUTPUT_PATHS["song_author_rel"], encoding="utf-8", index=False)
    print(f"✅ 生成song_author_rel CSV → {OUTPUT_PATHS['song_author_rel']}（共{len(df_song_author)}条关联）")

    # 7. 处理game_linkage_rel表（新增：最新更新时间）
    print("🔧 处理游戏联动表（game_linkage_rel）...")
    linkage_data = []
    current_time = get_current_datetime()
    for idx, row in df_raw.iterrows():
        source = row["来源"]
        home = row["本家"]
        update_time = row["更新时间"]
        if source == "" or home == "" or update_time == "" or source == home:
            continue
        date_8digit = get_8digit_date(update_time)
        rel_id = f"{source}_{home}_{date_8digit}" if date_8digit != "" else f"{source}_{home}"
        linkage_data.append({
            "rel_id": rel_id,
            "游戏1编号": source,
            "游戏2编号": "0",
            "游戏1名称": "",
            "游戏2名称": home,
            "联动名称": "",
            "联动时间": get_standard_date(update_time),
            "联动版本": "",
            "说明": "",
            "最新更新时间": current_time  # 新增字段
        })
    df_linkage = pd.DataFrame(linkage_data).drop_duplicates(subset=["rel_id"]).reset_index(drop=True)
    df_linkage.to_csv(OUTPUT_PATHS["game_linkage_rel"], encoding="utf-8", index=False)
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