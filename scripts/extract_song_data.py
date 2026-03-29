#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
歌曲数据提取脚本 V3.0 (Token去重版)
最小改动：仅替换去重规则为Token模糊匹配，其余逻辑完全复用
1. Author去重：基于 作者Token
2. Song去重：基于 (歌名Token + 作者Token集合)
3. 自动关联songtoken.csv，保留原有所有功能
"""
import os
import sys
import ast
import pandas as pd
import logging
from datetime import datetime

# ===================== 日志系统配置（无改动） =====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ===================== 路径配置（无改动 + 新增Token路径） =====================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MAIN_PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, MAIN_PROJECT_ROOT)

from config.settings import (
    CSV_TARGET_DIR,
    RAW_SONG_CSV_FILENAME,
    OUTPUT_CSV_FILENAMES
)

RAW_SONG_CSV_PATH = os.path.join(CSV_TARGET_DIR, RAW_SONG_CSV_FILENAME)
# 新增：Token文件路径（固定名称）
SONG_TOKEN_CSV_PATH = os.path.join(CSV_TARGET_DIR, "songtoken.csv")

OUTPUT_PATHS = {
    "song_info": os.path.join(CSV_TARGET_DIR, OUTPUT_CSV_FILENAMES["song_info"]),
    "author_info": os.path.join(CSV_TARGET_DIR, OUTPUT_CSV_FILENAMES["author_info"]),
    "game_song_rel": os.path.join(CSV_TARGET_DIR, OUTPUT_CSV_FILENAMES["game_song_rel"]),
    "song_author_rel": os.path.join(CSV_TARGET_DIR, OUTPUT_CSV_FILENAMES["song_author_rel"]),
    "game_linkage_rel": os.path.join(CSV_TARGET_DIR, OUTPUT_CSV_FILENAMES["game_linkage_rel"])
}

# ===================== 工具函数（无改动 + 新增Token解析） =====================
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

# 新增：解析作者Token列表
def parse_author_tokens(token_str):
    s = clean_string(token_str)
    if not s:
        return []
    try:
        parsed = ast.literal_eval(s)
        return [clean_string(x) for x in parsed] if isinstance(parsed, list) else []
    except:
        return []

def merge_aliases(existing, new):
    if existing == "":
        return new
    if new == "":
        return existing
    set_existing = set([x.strip() for x in existing.split("/") if x.strip()])
    set_new = set([x.strip() for x in new.split("/") if x.strip()])
    merged = sorted(list(set_existing.union(set_new)))
    return " / ".join(merged)

# ===================== 数据提取主逻辑（最小改动） =====================
def extract_song_data_v3():
    # 1. 基础校验与读取（无改动）
    if not os.path.exists(RAW_SONG_CSV_PATH):
        logger.error(f"原始song_info文件不存在 → {RAW_SONG_CSV_PATH}")
        return False
    if not os.path.exists(SONG_TOKEN_CSV_PATH):
        logger.error(f"Token文件不存在 → {SONG_TOKEN_CSV_PATH}")
        return False

    logger.info(f"读取原始数据 → {RAW_SONG_CSV_PATH}")
    df_raw = pd.read_csv(RAW_SONG_CSV_PATH, encoding="utf-8", dtype=str, na_filter=True)
    logger.info(f"读取Token数据 → {SONG_TOKEN_CSV_PATH}")
    df_token = pd.read_csv(SONG_TOKEN_CSV_PATH, encoding="utf-8-sig", dtype=str)

    # 关联Token数据（核心新增）
    df_raw = df_raw.merge(df_token[["song_id", "歌名token", "作者token"]], on="song_id", how="left")

    required_cols = ["song_id", "歌名", "别名", "作者", "来源", "本家", "更新时间", "真实作者", "歌名token", "作者token"]
    missing_cols = [col for col in required_cols if col not in df_raw.columns]
    if missing_cols:
        logger.error(f"缺少必要列 → {missing_cols}")
        return False

    # 清洗数据（无改动）
    for col in ["song_id", "歌名", "作者", "真实作者", "歌名token", "作者token"]:
        df_raw[col] = df_raw[col].apply(clean_string)
    df_raw = df_raw[df_raw["song_id"] != ""].reset_index(drop=True)
    if len(df_raw) == 0:
        logger.error("无有效数据")
        return False

    # ===================== 2. 核心改动：Token去重聚合 =====================
    logger.info("开始Token模糊去重聚合...")
    
    song_map = {}
    # 🔴 改动1：作者映射 Key=作者Token（同Token=同作者）
    author_token_map = {}
    author_name_map = {}

    song_id_counter = 1
    author_id_counter = 999999
    current_time = get_current_datetime()

    for idx, row in df_raw.iterrows():
        # 基础字段
        song_name = row["歌名"]
        nominal_author = row["作者"]
        source_csv_id = row["song_id"]
        song_token = row["歌名token"]
        author_token_list = parse_author_tokens(row["作者token"])
        real_authors_list = parse_real_authors(row["真实作者"]) or [nominal_author]

        # 校验Token
        if not song_token or not author_token_list:
            logger.warning(f"跳过无Token行 → {source_csv_id}")
            continue

        # 🔴 改动2：Song唯一Key = (歌名Token, 作者Token集合)（无序匹配）
        song_key = (song_token, frozenset(author_token_list))

        # --- Song 聚合（仅替换key，逻辑完全不变） ---
        if not song_name or not nominal_author:
            logger.warning(f"跳过无效行(CSV_ID:{source_csv_id})")
            continue
            
        if song_key not in song_map:
            internal_song_id = f"{str(song_id_counter).zfill(6)}"
            song_id_counter += 1
            song_map[song_key] = {
                "song_id": internal_song_id,
                "歌名": song_name,
                "别名": row["别名"],
                "作者": nominal_author,
                "本家": row["本家"],
                "最新更新时间": current_time,
                "_source_csv_id": source_csv_id 
            }
        else:
            existing_song = song_map[song_key]
            new_alias = merge_aliases(existing_song["别名"], row["别名"])
            existing_song["别名"] = new_alias
            existing_home, new_home = existing_song["本家"], row["本家"]
            if existing_home != new_home and existing_home and new_home:
                logger.error(f"[本家冲突] {song_name} | {existing_home} vs {new_home}")
            elif not existing_home and new_home:
                existing_song["本家"] = new_home

        # --- Author 聚合（仅替换key，逻辑完全不变） ---
        for i, author_name in enumerate(real_authors_list):
            # 取当前作者对应的Token
            if i < len(author_token_list):
                auth_token = author_token_list[i]
            else:
                auth_token = author_token_list[-1] if author_token_list else ""

            if not auth_token:
                continue
            # 同Token = 同一个作者
            if auth_token not in author_token_map:
                auth_id = f"{str(author_id_counter).zfill(6)}"
                author_id_counter -= 1
                author_token_map[auth_token] = {"id": auth_id, "name": author_name}
            # 记录作者名映射（用于关联）
            author_name_map[author_name] = auth_token

    # ===================== 3. 生成关联表（无改动） =====================
    logger.info("开始生成关联表...")
    game_song_rel_data = []
    song_author_rel_data = []
    linkage_data = []
    song_author_rel_id_counter = 1000001
    generated_song_author_pairs = set()

    for idx, row in df_raw.iterrows():
        csv_rel_id = row["song_id"]
        source_game = row["来源"]
        update_time = row["更新时间"]
        song_token = row["歌名token"]
        author_token_list = parse_author_tokens(row["作者token"])
        song_key = (song_token, frozenset(author_token_list))

        if song_key not in song_map:
            continue
        internal_song_id = song_map[song_key]["song_id"]
        current_home = song_map[song_key]["本家"]

        # GameSongRel（无改动）
        if source_game:
            game_song_rel_data.append({
                "rel_id": csv_rel_id, "游戏编号": source_game, "song_id": internal_song_id,
                "本家": current_home, "收录时间": get_standard_date(update_time),
                "最新更新时间": current_time
            })

        # SongAuthorRel（无改动）
        real_authors_list = parse_real_authors(row["真实作者"]) or [row["作者"]]
        for author_name in real_authors_list:
            if author_name not in author_name_map:
                continue
            auth_token = author_name_map[author_name]
            auth_id = author_token_map[auth_token]["id"]
            pair_key = (internal_song_id, auth_id)
            if pair_key in generated_song_author_pairs:
                continue
            generated_song_author_pairs.add(pair_key)

            rel_id = f"{str(song_author_rel_id_counter).zfill(7)}"
            song_author_rel_id_counter += 1
            song_author_rel_data.append({
                "rel_id": rel_id, "song_id": internal_song_id, "author_id": auth_id,
                "曲名": row["歌名"], "作者名": author_name, "最新更新时间": current_time
            })

    # ===================== 4. 导出 CSV（无改动） =====================
    # 导出Song
    song_list = [{k:v for k,v in s.items() if k != "_source_csv_id"} for s in song_map.values()]
    df_song = pd.DataFrame(song_list).sort_values("song_id").reset_index(drop=True)
    df_song[["song_id", "歌名", "别名", "作者", "本家", "最新更新时间"]].to_csv(
        OUTPUT_PATHS["song_info"], encoding="utf-8-sig", index=False)

    # 导出Author
    author_list = [{
        "author_id": v["id"], "作者名": v["name"], "别名": "", "备注": "", "最新更新时间": current_time
    } for v in author_token_map.values()]
    df_author = pd.DataFrame(author_list).sort_values("author_id", ascending=False).reset_index(drop=True)
    df_author[["author_id", "作者名", "别名", "备注", "最新更新时间"]].to_csv(
        OUTPUT_PATHS["author_info"], encoding="utf-8-sig", index=False)

    # 导出GameSongRel
    df_game_song = pd.DataFrame(game_song_rel_data)
    gs_cols = ["rel_id", "游戏编号", "song_id", "本家", "收录时间", "最新更新时间"]
    df_game_song = df_game_song.reindex(columns=gs_cols).fillna("")
    df_game_song.to_csv(OUTPUT_PATHS["game_song_rel"], encoding="utf-8", index=False)

    # 导出其余表（无改动）
    pd.DataFrame(song_author_rel_data).reindex(columns=["rel_id", "song_id", "author_id", "曲名", "作者名", "最新更新时间"])\
      .to_csv(OUTPUT_PATHS["song_author_rel"], encoding="utf-8", index=False)
    pd.DataFrame(columns=["rel_id", "游戏1编号", "游戏2编号", "游戏1名称", "游戏2名称", "联动名称", "联动时间", "联动版本", "说明", "最新更新时间"])\
      .to_csv(OUTPUT_PATHS["game_linkage_rel"], encoding="utf-8", index=False)

    # ===================== 5. 输出要求：3张表数据量 =====================
    logger.info("="*50)
    logger.info(f"✅ 作者表(author_info)：{len(df_author)} 条")
    logger.info(f"✅ 歌曲表(song_info)：{len(df_song)} 条")
    logger.info(f"✅ 游戏歌曲关联表(game_song_rel)：{len(df_game_song)} 条")
    logger.info("="*50)
    logger.info("🎉 Token去重版处理完成！")
    return True

if __name__ == "__main__":
    os.makedirs(CSV_TARGET_DIR, exist_ok=True)
    extract_song_data_v3()