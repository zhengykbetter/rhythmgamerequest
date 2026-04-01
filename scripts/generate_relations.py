#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
关联表生成脚本 V2.0（适配新查重逻辑）
✅ 功能：
1. 读取核心数据脚本的输出（歌曲表、作者表、中间映射）
2. 生成所有关联表（游戏-歌曲、歌曲-作者、游戏联动）
"""
import os
import sys
import ast
import pandas as pd
import logging
from datetime import datetime

# ===================== 日志系统 =====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ===================== 路径配置 =====================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MAIN_PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, MAIN_PROJECT_ROOT)

from config.settings import (
    CSV_TARGET_DIR,
    RAW_SONG_CSV_FILENAME,
    OUTPUT_CSV_FILENAMES
)

RAW_SONG_CSV_PATH = os.path.join(CSV_TARGET_DIR, RAW_SONG_CSV_FILENAME)
SONG_TOKEN_CSV_PATH = os.path.join(CSV_TARGET_DIR, "songtoken.csv")

# 输入路径（核心数据脚本的输出）
INPUT_PATHS = {
    "song_info": os.path.join(CSV_TARGET_DIR, OUTPUT_CSV_FILENAMES["song_info"]),
    "author_info": os.path.join(CSV_TARGET_DIR, OUTPUT_CSV_FILENAMES["author_info"]),
    "temp_song_key_map": os.path.join(CSV_TARGET_DIR, "temp_song_key_map.csv")
}

# 输出路径
OUTPUT_PATHS = {
    "game_song_rel": os.path.join(CSV_TARGET_DIR, OUTPUT_CSV_FILENAMES["game_song_rel"]),
    "song_author_rel": os.path.join(CSV_TARGET_DIR, OUTPUT_CSV_FILENAMES["song_author_rel"]),
    "game_linkage_rel": os.path.join(CSV_TARGET_DIR, OUTPUT_CSV_FILENAMES["game_linkage_rel"])
}

# ===================== 工具函数 =====================
def clean_string(val):
    if pd.isna(val) or val == "":
        return ""
    return str(val).strip()

def get_standard_date(date_str):
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
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def parse_real_authors(real_author_str):
    s = clean_string(real_author_str)
    if not s:
        return []
    try:
        parsed = ast.literal_eval(s)
        if isinstance(parsed, list):
            return [clean_string(x) for x in parsed if clean_string(x) != ""]
        return []
    except:
        return [s]

def parse_author_tokens(token_str):
    s = clean_string(token_str)
    if not s:
        return []
    try:
        parsed = ast.literal_eval(s)
        return [clean_string(x) for x in parsed] if isinstance(parsed, list) else []
    except:
        return []

# ===================== 核心业务逻辑 =====================
def generate_relations():
    # 1. 读取所有必要文件
    logger.info("读取输入文件...")
    for path in INPUT_PATHS.values():
        if not os.path.exists(path):
            logger.error(f"输入文件不存在 → {path}")
            return False
    if not os.path.exists(RAW_SONG_CSV_PATH):
        logger.error(f"原始文件不存在 → {RAW_SONG_CSV_PATH}")
        return False
    if not os.path.exists(SONG_TOKEN_CSV_PATH):
        logger.error(f"Token文件不存在 → {SONG_TOKEN_CSV_PATH}")
        return False

    # 读取核心数据
    df_song = pd.read_csv(INPUT_PATHS["song_info"], encoding="utf-8-sig", dtype=str)
    df_author = pd.read_csv(INPUT_PATHS["author_info"], encoding="utf-8-sig", dtype=str)
    df_key_map = pd.read_csv(INPUT_PATHS["temp_song_key_map"], encoding="utf-8", dtype=str)

    # 读取原始数据和Token
    df_raw = pd.read_csv(RAW_SONG_CSV_PATH, encoding="utf-8", dtype=str, na_filter=True)
    df_token = pd.read_csv(SONG_TOKEN_CSV_PATH, encoding="utf-8-sig", dtype=str)
    df_raw = df_raw.merge(df_token[["song_id", "歌名token", "作者token"]], on="song_id", how="left")

    # 清洗
    for col in ["song_id", "歌名", "作者", "真实作者", "歌名token", "作者token"]:
        df_raw[col] = df_raw[col].apply(clean_string)
    df_raw = df_raw[df_raw["song_id"] != ""].reset_index(drop=True)

    # 2. 构建映射字典（简化版）
    logger.info("构建映射字典...")
    # 原始song_id → 内部song_id
    raw_sid_to_internal_sid = dict(zip(df_key_map["raw_song_id"], df_key_map["internal_song_id"]))
    # 歌曲id→本家映射
    song_id_to_home = dict(zip(df_song["song_id"], df_song["本家"]))
    # 作者token→id映射（重新构建）
    author_token_map = {}
    for _, row in df_raw.iterrows():
        auth_tokens = parse_author_tokens(row["作者token"])
        real_authors = parse_real_authors(row["真实作者"]) or [row["作者"]]
        for a_name, a_tok in zip(real_authors, auth_tokens):
            if a_tok and a_name:
                author_token_map[a_tok] = a_name
    # 关联作者id
    author_token_to_id = {}
    for a_tok, a_name in author_token_map.items():
        match = df_author[df_author["作者名"] == a_name]
        if not match.empty:
            author_token_to_id[a_tok] = match.iloc[0]["author_id"]

    # 3. 初始化关联表存储
    current_time = get_current_datetime()
    game_song_rel = []
    song_author_rel = []
    rel_id_counter = 1000001
    sa_pairs = set()

    # ===================== 生成关联表 =====================
    logger.info("生成关联表...")
    for _, row in df_raw.iterrows():
        raw_sid = row["song_id"]
        game = row["来源"]
        date = row["更新时间"]
        auth_tokens = parse_author_tokens(row["作者token"])
        real_authors = parse_real_authors(row["真实作者"]) or [row["作者"]]

        # 直接通过原始song_id获取内部song_id
        internal_sid = raw_sid_to_internal_sid.get(raw_sid)
        if not internal_sid:
            continue

        # ============== 游戏-歌曲关联 ==============
        if game:
            game_song_rel.append({
                "rel_id": raw_sid,
                "游戏编号": game,
                "song_id": internal_sid,
                "本家": song_id_to_home.get(internal_sid, ""),
                "收录时间": get_standard_date(date),
                "最新更新时间": current_time
            })

        # ============== 歌曲-作者关联 ==============
        for a_name, a_tok in zip(real_authors, auth_tokens):
            if a_tok not in author_token_to_id:
                continue
            aid = author_token_to_id[a_tok]
            if (internal_sid, aid) not in sa_pairs:
                sa_pairs.add((internal_sid, aid))
                song_author_rel.append({
                    "rel_id": f"{rel_id_counter:07d}",
                    "song_id": internal_sid,
                    "author_id": aid,
                    "曲名": row["歌名"],
                    "作者名": a_name,
                    "最新更新时间": current_time
                })
                rel_id_counter += 1

    # ===================== 导出文件 =====================
    # 游戏-歌曲关联表
    df_gs_rel = pd.DataFrame(game_song_rel)
    df_gs_rel = df_gs_rel[["rel_id", "游戏编号", "song_id", "本家", "收录时间", "最新更新时间"]]
    df_gs_rel.to_csv(OUTPUT_PATHS["game_song_rel"], encoding="utf-8", index=False)

    # 歌曲-作者关联表
    pd.DataFrame(song_author_rel).to_csv(OUTPUT_PATHS["song_author_rel"], encoding="utf-8", index=False)

    # 游戏联动关联表（空表）
    pd.DataFrame(columns=["rel_id","游戏1编号","游戏2编号","联动名称","最新更新时间"]).to_csv(OUTPUT_PATHS["game_linkage_rel"], index=False)

    # ===================== 最终输出 =====================
    logger.info("="*60)
    logger.info(f"✅ 游戏歌曲关联表(game_song_rel)：{len(df_gs_rel):,} 条")
    logger.info(f"✅ 歌曲作者关联表(song_author_rel)：{len(song_author_rel):,} 条")
    logger.info("="*60)
    logger.info("🎉 关联表生成完成！")
    return True

if __name__ == "__main__":
    os.makedirs(CSV_TARGET_DIR, exist_ok=True)
    generate_relations()