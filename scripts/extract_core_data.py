#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
核心数据提取脚本 V4.0（基于全量映射解耦逻辑）
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
UNITOKEN_CSV_PATH = os.path.join(CSV_TARGET_DIR, "unitoken.csv")

OUTPUT_PATHS = {
    "song_info": os.path.join(CSV_TARGET_DIR, OUTPUT_CSV_FILENAMES["song_info"]),
    "author_info": os.path.join(CSV_TARGET_DIR, OUTPUT_CSV_FILENAMES["author_info"]),
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

# ===================== 核心业务逻辑 =====================
def extract_core_data():
    current_time = get_current_datetime()

    # 1. 读取所有输入数据
    logger.info("读取输入数据...")
    df_raw = pd.read_csv(RAW_SONG_CSV_PATH, encoding="utf-8", dtype=str, na_filter=True)
    df_token = pd.read_csv(SONG_TOKEN_CSV_PATH, encoding="utf-8-sig", dtype=str)
    df_unitoken = pd.read_csv(UNITOKEN_CSV_PATH, encoding="utf-8-sig", dtype=str)

    # 清洗基础数据
    for col in ["song_id", "歌名", "作者", "真实作者", "别名", "本家"]:
        df_raw[col] = df_raw[col].apply(clean_string)
    df_raw = df_raw[df_raw["song_id"] != ""].reset_index(drop=True)

    # 2. 构建 unitoken 全局映射
    logger.info("构建 unitoken 全局映射...")
    # 原始Token -> (统一Token, 统一作者名, 原始作者名)
    alias_to_main_map = dict(zip(
        df_unitoken["原始Token"],
        zip(df_unitoken["统一Token"], df_unitoken["统一作者名"], df_unitoken["原始作者名"])
    ))
    # 统一Token -> (标准主名, 别名集合)
    main_token_info = defaultdict(lambda: {"main_name": "", "aliases": set()})
    for _, row in df_unitoken.iterrows():
        main_tok = row["统一Token"]
        main_name = row["统一作者名"]
        alias_name = row["原始作者名"]
        main_token_info[main_tok]["main_name"] = main_name
        if alias_name != main_name:
            main_token_info[main_tok]["aliases"].add(alias_name)

    # 3. 全量作者 token 归集与身份锁定
    logger.info("全量作者 token 归集...")
    # 展开所有作者 token
    all_raw_tokens = set()
    song_token_map = {}  # song_id -> (歌名token, 原始作者token列表)
    for _, row in df_token.iterrows():
        sid = row["song_id"]
        song_tok = row["歌名token"]
        auth_toks = parse_author_tokens(row["作者token"])
        song_token_map[sid] = (song_tok, auth_toks)
        all_raw_tokens.update(auth_toks)

    # 生成 token -> 统一Token 的全局映射，同时归集无映射 token 的名称
    token_to_unified = {}
    # 先处理有 unitoken 映射的
    for raw_tok in all_raw_tokens:
        if raw_tok in alias_to_main_map:
            main_tok, _, _ = alias_to_main_map[raw_tok]
            token_to_unified[raw_tok] = main_tok
        else:
            token_to_unified[raw_tok] = raw_tok  # 无映射则自身为统一Token

    # 补充无映射 token 的作者名（从 raw_song 匹配）
    # 构建 raw_token -> 作者名列表 的辅助映射
    token_name_candidates = defaultdict(list)
    for _, row in df_raw.iterrows():
        sid = row["song_id"]
        if sid not in song_token_map:
            continue
        _, auth_toks = song_token_map[sid]
        real_authors = parse_author_tokens(row["真实作者"]) or [row["作者"]]
        # 简单对齐，token 优先取对应位置的名字，没有则取第一个
        for i, tok in enumerate(auth_toks):
            if i < len(real_authors):
                token_name_candidates[tok].append(real_authors[i])
            else:
                token_name_candidates[tok].append(real_authors[0] if real_authors else "")

    # 完善 main_token_info 中无映射 token 的信息
    for raw_tok in all_raw_tokens:
        main_tok = token_to_unified[raw_tok]
        if main_tok not in main_token_info:
            # 无映射的独立作者
            candidates = token_name_candidates.get(raw_tok, [])
            main_name = next((x for x in candidates if x), raw_tok)
            aliases = set(x for x in candidates if x and x != main_name)
            main_token_info[main_tok] = {"main_name": main_name, "aliases": aliases}
        else:
            # 有映射的，补充从 raw_song 里发现的新别名
            candidates = token_name_candidates.get(raw_tok, [])
            main_name = main_token_info[main_tok]["main_name"]
            for name in candidates:
                if name and name != main_name:
                    main_token_info[main_tok]["aliases"].add(name)

    # 4. 生成 author_info 表
    logger.info("生成 author_info 表...")
    author_id_counter = 999999
    author_list = []
    unified_to_aid = {}  # 统一Token -> author_id
    for main_tok in sorted(main_token_info.keys()):
        info = main_token_info[main_tok]
        aid = f"{author_id_counter:06d}"
        author_id_counter -= 1
        unified_to_aid[main_tok] = aid
        author_list.append({
            "author_id": aid,
            "作者名": info["main_name"],
            "别名": " / ".join(sorted(info["aliases"])),
            "备注": "",
            "最新更新时间": current_time
        })
    df_author = pd.DataFrame(author_list).sort_values("author_id", ascending=False)

    # 5. 歌曲查重与合并
    logger.info("歌曲查重与合并...")
    # 合并 raw_song 和 token 信息
    song_data_list = []
    for _, row in df_raw.iterrows():
        sid = row["song_id"]
        if sid not in song_token_map:
            continue
        song_tok, raw_auth_toks = song_token_map[sid]
        # 转换为统一Token集合
        unified_auth_set = frozenset(token_to_unified[t] for t in raw_auth_toks if t in token_to_unified)
        song_data_list.append({
            "raw_sid": sid,
            "song_tok": song_tok,
            "unified_auth_set": unified_auth_set,
            "歌名": row["歌名"],
            "别名": row["别名"],
            "作者": row["作者"],
            "本家": row["本家"]
        })

    # 按歌名token预分组
    song_groups = defaultdict(list)
    for data in song_data_list:
        song_groups[data["song_tok"]].append(data)

    # 组内查重合并
    song_id_counter = 1
    final_songs = []
    raw_sid_map = {}

    for song_tok, group in song_groups.items():
        # 合并簇：列表的每个元素是 (合并后的统一Token集合, 歌曲数据列表)
        clusters = []
        for song in group:
            matched = False
            for i, (cluster_set, cluster_songs) in enumerate(clusters):
                if song["unified_auth_set"] & cluster_set:
                    # 合并到簇
                    new_set = cluster_set | song["unified_auth_set"]
                    cluster_songs.append(song)
                    clusters[i] = (new_set, cluster_songs)
                    matched = True
                    break
            if not matched:
                clusters.append((song["unified_auth_set"], [song]))

        # 处理每个合并簇
        for cluster_set, cluster_songs in clusters:
            internal_sid = f"{song_id_counter:06d}"
            song_id_counter += 1

            # 合并信息
            base_song = cluster_songs[0]
            merged_alias = base_song["别名"]
            merged_home = base_song["本家"]

            for song in cluster_songs[1:]:
                merged_alias = merge_aliases(merged_alias, song["别名"])
                # 本家冲突检测
                if merged_home and song["本家"] and merged_home != song["本家"]:
                    logger.error(f"本家冲突 | 歌曲:{base_song['歌名']} | {merged_home} vs {song['本家']}")
                elif not merged_home and song["本家"]:
                    merged_home = song["本家"]

                # 记录原始id映射
                raw_sid_map[song["raw_sid"]] = internal_sid

            # 记录第一首的原始id
            raw_sid_map[base_song["raw_sid"]] = internal_sid

            final_songs.append({
                "song_id": internal_sid,
                "歌名": base_song["歌名"],
                "别名": merged_alias,
                "作者": base_song["作者"],
                "本家": merged_home,
                "最新更新时间": current_time
            })

    # 6. 导出文件
    logger.info("导出文件...")
    # 歌曲表
    df_song = pd.DataFrame(final_songs).sort_values("song_id")
    df_song = df_song[["song_id", "歌名", "别名", "作者", "本家", "最新更新时间"]]
    df_song.to_csv(OUTPUT_PATHS["song_info"], encoding="utf-8-sig", index=False)

    # 作者表
    df_author.to_csv(OUTPUT_PATHS["author_info"], encoding="utf-8-sig", index=False)

    # 映射表
    df_key_map = pd.DataFrame(list(raw_sid_map.items()), columns=["raw_song_id", "internal_song_id"])
    df_key_map.to_csv(OUTPUT_PATHS["temp_song_key_map"], encoding="utf-8", index=False)

    logger.info("="*60)
    logger.info(f"✅ 作者表(author_info)：{len(df_author):,} 条")
    logger.info(f"✅ 歌曲表(song_info)：{len(df_song):,} 条")
    logger.info(f"✅ 中间映射文件：{len(df_key_map):,} 条")
    logger.info("="*60)
    logger.info("🎉 核心数据提取完成！")
    return True

if __name__ == "__main__":
    os.makedirs(CSV_TARGET_DIR, exist_ok=True)
    extract_core_data()