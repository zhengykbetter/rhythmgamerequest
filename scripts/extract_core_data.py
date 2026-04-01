#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
核心数据提取脚本 V3.0（集成 unitoken.csv 统一逻辑）
✅ 功能：
1. 读取 unitoken.csv 统一作者Token
2. 按【歌名Token相同 + 统一作者Token有交集】去重生成唯一歌曲表
3. 按【统一作者Token】去重生成唯一作者表
4. 导出中间映射文件（原始song_id → 内部song_id）
"""
import os
import sys
import ast
import pandas as pd
import logging
from datetime import datetime
from collections import defaultdict

# ===================== 日志系统 =====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ===================== 路径配置（新增 unitoken.csv） =====================
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
UNITOKEN_CSV_PATH = os.path.join(CSV_TARGET_DIR, "unitoken.csv")  # 新增：统一Token表

# 核心输出路径
OUTPUT_PATHS = {
    "song_info": os.path.join(CSV_TARGET_DIR, OUTPUT_CSV_FILENAMES["song_info"]),
    "author_info": os.path.join(CSV_TARGET_DIR, OUTPUT_CSV_FILENAMES["author_info"]),
    # 中间映射文件（供关联表脚本使用）
    "temp_song_key_map": os.path.join(CSV_TARGET_DIR, "temp_song_key_map.csv")
}

# ===================== 工具函数 =====================
def clean_string(val):
    if pd.isna(val) or val == "":
        return ""
    return str(val).strip()

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
    existing = clean_string(existing)
    new = clean_string(new)
    if existing == "":
        return new
    if new == "":
        return existing
    set_existing = set([x.strip() for x in existing.split("/") if x.strip()])
    set_new = set([x.strip() for x in new.split("/") if x.strip()])
    merged = sorted(list(set_existing.union(set_new)))
    return " / ".join(merged)

def get_current_datetime():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ===================== 新增：unitoken.csv 读取与映射构建 =====================
def build_unitoken_map(unitoken_path):
    """
    读取 unitoken.csv，构建【原始作者Token → 统一作者Token】的映射
    如果原始Token不在映射中，则返回自身
    """
    if not os.path.exists(unitoken_path):
        logger.warning(f"unitoken.csv 不存在，将使用原始作者Token")
        return {}
    
    try:
        df_unitoken = pd.read_csv(unitoken_path, encoding="utf-8-sig", dtype=str)
        unitoken_map = dict(zip(df_unitoken["原始Token"], df_unitoken["统一Token"]))
        logger.info(f"✅ 成功读取 unitoken.csv：共 {len(unitoken_map)} 条统一映射")
        return unitoken_map
    except Exception as e:
        logger.error(f"读取 unitoken.csv 失败：{str(e)}，将使用原始作者Token")
        return {}

def get_unified_token(original_token, unitoken_map):
    """获取统一后的作者Token，无映射则返回原始Token"""
    return unitoken_map.get(original_token, original_token)

# ===================== 核心业务逻辑（集成 unitoken） =====================
def extract_core_data():
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

    # 2. 读取 unitoken 映射
    logger.info("读取 unitoken.csv 统一作者Token...")
    unitoken_map = build_unitoken_map(UNITOKEN_CSV_PATH)

    # 清洗
    for col in ["song_id", "歌名", "作者", "真实作者", "歌名token", "作者token"]:
        df_raw[col] = df_raw[col].apply(clean_string)
    df_raw = df_raw[df_raw["song_id"] != ""].reset_index(drop=True)

    # 3. 初始化存储
    # 歌曲存储结构：Key=歌名token → Value=[(统一作者token集合, 歌曲数据)]
    song_map = defaultdict(list)
    # 中间映射：原始song_id → 内部song_id
    raw_sid_to_internal_sid = {}
    # 作者存储：Key=统一作者token → Value=作者数据
    author_token_map = {}
    song_id_counter = 1
    author_id_counter = 999999
    current_time = get_current_datetime()

    # ===================== 构建去重后的歌曲&作者（使用统一Token） =====================
    logger.info("构建唯一歌曲/作者库（歌名Token相同+统一作者Token有交集 → 同一首歌）...")
    for _, row in df_raw.iterrows():
        raw_sid = row["song_id"]
        song_name = row["歌名"]
        nominal_author = row["作者"]
        song_token = row["歌名token"]
        
        # 解析原始作者Token，并替换为统一Token
        original_auth_tokens = parse_author_tokens(row["作者token"])
        unified_auth_tokens = [get_unified_token(tok, unitoken_map) for tok in original_auth_tokens]
        real_authors = parse_author_tokens(row["真实作者"]) or [nominal_author]

        # 基础过滤
        if not song_token or not unified_auth_tokens or not song_name:
            continue
        
        unified_auth_set = frozenset(unified_auth_tokens)
        internal_sid = None
        found_existing = False

        # ============== 歌曲查重：同歌名Token + 统一作者Token有交集 ==============
        if song_token in song_map:
            # 遍历同歌名Token下的所有已存歌曲，检查统一作者Token交集
            for idx, (existing_unified_auth_set, existing_song) in enumerate(song_map[song_token]):
                if existing_unified_auth_set & unified_auth_set:  # 统一作者集合有交集
                    # 合并到现有歌曲
                    internal_sid = existing_song["song_id"]
                    
                    # 合并别名
                    existing_song["别名"] = merge_aliases(existing_song["别名"], row["别名"])
                    
                    # 合并本家（冲突检测+更新）
                    existing_home = clean_string(existing_song["本家"])
                    new_home = clean_string(row["本家"])
                    if existing_home and new_home and existing_home != new_home:
                        logger.error(f"本家冲突 | 歌曲:{song_name} | {existing_home} vs {new_home}")
                    elif not existing_home and new_home:
                        existing_song["本家"] = new_home
                    
                    # 更新已存歌曲的统一作者集合（合并，扩大后续匹配范围）
                    merged_unified_auth_set = existing_unified_auth_set | unified_auth_set
                    song_map[song_token][idx] = (merged_unified_auth_set, existing_song)
                    
                    found_existing = True
                    break

        # 没找到现有歌曲，创建新歌
        if not found_existing:
            internal_sid = f"{song_id_counter:06d}"
            song_id_counter += 1
            new_song = {
                "song_id": internal_sid,
                "歌名": song_name,
                "别名": row["别名"],
                "作者": nominal_author,
                "本家": row["本家"],
                "最新更新时间": current_time
            }
            song_map[song_token].append((unified_auth_set, new_song))

        # 记录原始song_id到内部song_id的映射
        raw_sid_to_internal_sid[raw_sid] = internal_sid

        # ============== 作者去重：基于统一作者Token ==============
        for a_name, original_a_tok in zip(real_authors, original_auth_tokens):
            unified_a_tok = get_unified_token(original_a_tok, unitoken_map)
            if not unified_a_tok:
                continue
            if unified_a_tok not in author_token_map:
                aid = f"{author_id_counter:06d}"
                author_id_counter -= 1
                author_token_map[unified_a_tok] = {
                    "author_id": aid,
                    "作者名": a_name
                }

    # ===================== 导出文件 =====================
    # 歌曲表（从song_map中提取所有歌曲数据）
    all_songs = []
    for song_list in song_map.values():
        for _, song_data in song_list:
            all_songs.append(song_data)
    df_song = pd.DataFrame(all_songs).sort_values("song_id")
    df_song = df_song[["song_id", "歌名", "别名", "作者", "本家", "最新更新时间"]]
    df_song.to_csv(OUTPUT_PATHS["song_info"], encoding="utf-8-sig", index=False)

    # 作者表
    df_author = pd.DataFrame(author_token_map.values()).sort_values("author_id", ascending=False)
    df_author["别名"] = ""
    df_author["备注"] = ""
    df_author["最新更新时间"] = current_time
    df_author = df_author[["author_id", "作者名", "别名", "备注", "最新更新时间"]]
    df_author.to_csv(OUTPUT_PATHS["author_info"], encoding="utf-8-sig", index=False)

    # 中间映射文件：原始song_id → 内部song_id
    df_key_map = pd.DataFrame(list(raw_sid_to_internal_sid.items()), columns=["raw_song_id", "internal_song_id"])
    df_key_map.to_csv(OUTPUT_PATHS["temp_song_key_map"], encoding="utf-8", index=False)

    # ===================== 最终输出 =====================
    logger.info("="*60)
    logger.info(f"✅ 作者表(author_info)：{len(df_author):,} 条（基于统一Token去重）")
    logger.info(f"✅ 歌曲表(song_info)：{len(df_song):,} 条（基于统一作者Token查重）")
    logger.info(f"✅ 中间映射文件：{len(df_key_map):,} 条")
    logger.info("="*60)
    logger.info("🎉 核心数据提取完成！")
    return True

if __name__ == "__main__":
    os.makedirs(CSV_TARGET_DIR, exist_ok=True)
    extract_core_data()