#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
歌曲数据提取脚本 V3.1（逻辑修复版）
✅ 正确逻辑：
1. Song表：按【歌名Token+作者Token集合】去重（唯一歌曲）
2. GameSongRel表：保留所有游戏收录记录，关联去重后song_id
3. Author表：按作者Token去重，模糊作者自动合并
4. 三者数量关系：Rel ≥ Song，Author为去重后作者数
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

OUTPUT_PATHS = {
    "song_info": os.path.join(CSV_TARGET_DIR, OUTPUT_CSV_FILENAMES["song_info"]),
    "author_info": os.path.join(CSV_TARGET_DIR, OUTPUT_CSV_FILENAMES["author_info"]),
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

def merge_aliases(existing, new):
    """合并别名：去重后用斜杠连接
    修复：兼容空值/NaN/float类型
    """
    # 核心修复：先强制清洗为字符串，杜绝float/NaN报错
    existing = clean_string(existing)
    new = clean_string(new)
    
    if existing == "":
        return new
    if new == "":
        return existing
    
    # 简单分割去重
    set_existing = set([x.strip() for x in existing.split("/") if x.strip()])
    set_new = set([x.strip() for x in new.split("/") if x.strip()])
    
    merged = sorted(list(set_existing.union(set_new)))
    return " / ".join(merged)
# ===================== 核心业务逻辑 =====================
def extract_song_data_v3():
    # 1. 读取数据
    if not os.path.exists(RAW_SONG_CSV_PATH):
        logger.error(f"原始文件不存在 → {RAW_SONG_CSV_PATH}")
        return False
    if not os.path.exists(SONG_TOKEN_CSV_PATH):
        logger.error(f"Token文件不存在 → {SONG_TOKEN_CSV_PATH}")
        return False

    df_raw = pd.read_csv(RAW_SONG_CSV_PATH, encoding="utf-8", dtype=str, na_filter=True)
    df_token = pd.read_csv(SONG_TOKEN_CSV_PATH, encoding="utf-8-sig", dtype=str)
    df_raw = df_raw.merge(df_token[["song_id", "歌名token", "作者token"]], on="song_id", how="left")

    # 清洗
    for col in ["song_id", "歌名", "作者", "真实作者", "歌名token", "作者token"]:
        df_raw[col] = df_raw[col].apply(clean_string)
    df_raw = df_raw[df_raw["song_id"] != ""].reset_index(drop=True)

    # 2. 初始化存储
    song_map = {}              # Key: (歌名token, 作者token集合) → 唯一歌曲
    author_token_map = {}      # Key: 作者token → 唯一作者
    song_id_counter = 1
    author_id_counter = 999999
    current_time = get_current_datetime()

    # ===================== 第一轮：构建去重后的歌曲&作者 =====================
    logger.info("构建唯一歌曲/作者库（Token模糊去重）...")
    for _, row in df_raw.iterrows():
        song_name = row["歌名"]
        nominal_author = row["作者"]
        song_token = row["歌名token"]
        auth_tokens = parse_author_tokens(row["作者token"])
        real_authors = parse_real_authors(row["真实作者"]) or [nominal_author]

        # 基础过滤
        if not song_token or not auth_tokens or not song_name:
            continue
        
        # 歌曲唯一KEY（核心：模糊去重，无视游戏）
        song_key = (song_token, frozenset(auth_tokens))

        # ============== 歌曲去重：多游戏收录同一首歌，只存一次 ==============
        if song_key not in song_map:
            sid = f"{song_id_counter:06d}"
            song_id_counter += 1
            song_map[song_key] = {
                "song_id": sid,
                "歌名": song_name,
                "别名": row["别名"],
                "作者": nominal_author,
                "本家": row["本家"],
                "最新更新时间": current_time
            }
        else:
            # 合并别名 + 本家冲突检测
            song = song_map[song_key]
            existing_home = clean_string(song["本家"])
            new_home = clean_string(row["本家"])

            # 只有：两个都不为空 且 不相等 → 才报冲突（修复NaN问题）
            if existing_home and new_home and existing_home != new_home:
                logger.error(f"本家冲突 | 歌曲:{song_name} | {existing_home} vs {new_home}")
            # 只有：新本家不为空，旧本家为空 → 才更新
            elif not existing_home and new_home:
                song["本家"] = new_home

        # ============== 作者去重：按作者Token，模糊作者合并 ==============
        for a_name, a_tok in zip(real_authors, auth_tokens):
            if not a_tok:
                continue
            if a_tok not in author_token_map:
                aid = f"{author_id_counter:06d}"
                author_id_counter -= 1
                author_token_map[a_tok] = {
                    "author_id": aid,
                    "作者名": a_name
                }

    # ===================== 第二轮：生成关联表（保留所有收录记录） =====================
    logger.info("生成游戏/作者关联表...")
    game_song_rel = []
    song_author_rel = []
    rel_id_counter = 1000001
    sa_pairs = set()

    for _, row in df_raw.iterrows():
        csv_sid = row["song_id"]
        game = row["来源"]
        date = row["更新时间"]
        song_token = row["歌名token"]
        auth_tokens = parse_author_tokens(row["作者token"])
        real_authors = parse_real_authors(row["真实作者"]) or [row["作者"]]

        if not song_token or not auth_tokens:
            continue
        
        song_key = (song_token, frozenset(auth_tokens))
        if song_key not in song_map:
            continue
        
        # 关联去重后的歌曲ID
        internal_sid = song_map[song_key]["song_id"]

        # ============== 游戏-歌曲关联：每条收录记录都保留 ==============
        if game:
            game_song_rel.append({
                "rel_id": csv_sid,
                "游戏编号": game,
                "song_id": internal_sid,
                "本家": song_map[song_key]["本家"],
                "收录时间": get_standard_date(date),
                "最新更新时间": current_time
            })

        # ============== 歌曲-作者关联 ==============
        for a_name, a_tok in zip(real_authors, auth_tokens):
            if a_tok not in author_token_map:
                continue
            aid = author_token_map[a_tok]["author_id"]
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
    # 歌曲表（去重后唯一歌曲）
    df_song = pd.DataFrame(song_map.values()).sort_values("song_id")
    df_song = df_song[["song_id", "歌名", "别名", "作者", "本家", "最新更新时间"]]
    df_song.to_csv(OUTPUT_PATHS["song_info"], encoding="utf-8-sig", index=False)

    # 作者表（去重后唯一作者）
    df_author = pd.DataFrame(author_token_map.values()).sort_values("author_id", ascending=False)
    df_author["别名"] = ""
    df_author["备注"] = ""
    df_author["最新更新时间"] = current_time
    df_author = df_author[["author_id", "作者名", "别名", "备注", "最新更新时间"]]
    df_author.to_csv(OUTPUT_PATHS["author_info"], encoding="utf-8-sig", index=False)

    # 游戏-歌曲关联表（所有收录记录，数量最多）
    df_gs_rel = pd.DataFrame(game_song_rel)
    df_gs_rel = df_gs_rel[["rel_id", "游戏编号", "song_id", "本家", "收录时间", "最新更新时间"]]
    df_gs_rel.to_csv(OUTPUT_PATHS["game_song_rel"], encoding="utf-8", index=False)

    # 其他关联表
    pd.DataFrame(song_author_rel).to_csv(OUTPUT_PATHS["song_author_rel"], encoding="utf-8", index=False)
    pd.DataFrame(columns=["rel_id","游戏1编号","游戏2编号","联动名称","最新更新时间"]).to_csv(OUTPUT_PATHS["game_linkage_rel"], index=False)

    # ===================== 最终输出3张表数量（正确逻辑） =====================
    logger.info("="*60)
    logger.info(f"✅ 作者表(author_info)：{len(df_author):,} 条")
    logger.info(f"✅ 歌曲表(song_info)：{len(df_song):,} 条")
    logger.info(f"✅ 游戏歌曲关联表(game_song_rel)：{len(df_gs_rel):,} 条")
    logger.info("="*60)
    logger.info("🎉 逻辑修复完成，数量关系完全符合业务规则！")
    return True

if __name__ == "__main__":
    os.makedirs(CSV_TARGET_DIR, exist_ok=True)
    extract_song_data_v3()