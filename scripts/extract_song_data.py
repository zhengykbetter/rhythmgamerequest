#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
歌曲数据提取脚本 V2.0
核心逻辑变更：
1. Song去重：基于(曲名+名义作者)聚合，ID从000001自增
2. Author生成：解析真实作者列表，ID从999999自减
3. 冲突处理：别名合并，本家冲突记Error日志
4. Rel表生成：基于聚合后的ID进行关联
"""
import os
import sys
import ast
import pandas as pd
import logging
from datetime import datetime

# ===================== 日志系统配置 =====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ===================== 路径+文件名配置（复用原有架构） =====================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MAIN_PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, MAIN_PROJECT_ROOT)

from config.settings import (
    CSV_TARGET_DIR,
    RAW_SONG_CSV_FILENAME,
    OUTPUT_CSV_FILENAMES
)

RAW_SONG_CSV_PATH = os.path.join(CSV_TARGET_DIR, RAW_SONG_CSV_FILENAME)
OUTPUT_PATHS = {
    "song_info": os.path.join(CSV_TARGET_DIR, OUTPUT_CSV_FILENAMES["song_info"]),
    "author_info": os.path.join(CSV_TARGET_DIR, OUTPUT_CSV_FILENAMES["author_info"]),
    "game_song_rel": os.path.join(CSV_TARGET_DIR, OUTPUT_CSV_FILENAMES["game_song_rel"]),
    "song_author_rel": os.path.join(CSV_TARGET_DIR, OUTPUT_CSV_FILENAMES["song_author_rel"]),
    "game_linkage_rel": os.path.join(CSV_TARGET_DIR, OUTPUT_CSV_FILENAMES["game_linkage_rel"])
}

# ===================== 核心工具函数 =====================
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
    """
    解析真实作者字段，格式如 "['uma', 'モリモリあつし']"
    返回列表，解析失败返回空列表
    """
    s = clean_string(real_author_str)
    if not s:
        return []
    try:
        # 尝试将字符串转为列表
        parsed = ast.literal_eval(s)
        if isinstance(parsed, list):
            return [clean_string(x) for x in parsed if clean_string(x) != ""]
        return []
    except:
        # 如果不是列表格式，当做单个作者处理
        return [s]

def merge_aliases(existing, new):
    """合并别名：去重后用斜杠连接"""
    if existing == "":
        return new
    if new == "":
        return existing
    
    # 简单分割去重
    set_existing = set([x.strip() for x in existing.split("/") if x.strip()])
    set_new = set([x.strip() for x in new.split("/") if x.strip()])
    
    merged = sorted(list(set_existing.union(set_new)))
    return " / ".join(merged)

# ===================== 数据提取主逻辑 V2.0 =====================
def extract_song_data_v2():
    # 1. 基础校验与读取
    if not os.path.exists(RAW_SONG_CSV_PATH):
        logger.error(f"原始song_info文件不存在 → {RAW_SONG_CSV_PATH}")
        return False

    logger.info(f"读取原始数据 → {RAW_SONG_CSV_PATH}")
    df_raw = pd.read_csv(RAW_SONG_CSV_PATH, encoding="utf-8", dtype=str, na_filter=True)
    
    required_cols = ["song_id", "歌名", "别名", "作者", "来源", "本家", "更新时间", "真实作者"]
    missing_cols = [col for col in required_cols if col not in df_raw.columns]
    if missing_cols:
        logger.error(f"原始CSV缺少必要列 → {missing_cols}")
        return False

    # 清洗基础数据
    for col in required_cols:
        df_raw[col] = df_raw[col].apply(clean_string)
    
    df_raw = df_raw[df_raw["song_id"] != ""].reset_index(drop=True)
    if len(df_raw) == 0:
        logger.error("原始数据中无有效song_id行")
        return False

    # ===================== 2. 第一轮遍历：聚合 Song 和 Author =====================
    logger.info("开始聚合歌曲与作者信息...")
    
    # 数据结构初始化
    song_map = {}       # Key: (歌名, 名义作者), Value: 聚合后的歌曲信息Dict
    author_map = {}     # Key: 作者本名, Value: author_id
    
    # ID 计数器初始化
    song_id_counter = 1         # 从 000001 开始
    author_id_counter = 999999  # 从 999999 开始
    
    current_time = get_current_datetime()

    for idx, row in df_raw.iterrows():
        song_name = row["歌名"]
        nominal_author = row["作者"] # 名义作者
        source_csv_id = row["song_id"] # 原始CSV的ID (用于日志)
        
        # --- Song 聚合逻辑 ---
        # 每首歌必须有名义作者
        if not song_name or not nominal_author:
            logger.warning(f"跳过无效行(CSV_ID:{source_csv_id})：歌名或名义作者为空")
            continue
            
        song_key = (song_name, nominal_author)
        
        if song_key not in song_map:
            # 新歌：注册信息
            internal_song_id = f"{str(song_id_counter).zfill(6)}"
            song_id_counter += 1
            
            song_map[song_key] = {
                "song_id": internal_song_id,
                "歌名": song_name,
                "别名": row["别名"],
                "作者": nominal_author, # 名义作者
                "本家": row["本家"],
                "最新更新时间": current_time,
                # 辅助字段，用于检查冲突
                "_source_csv_id": source_csv_id 
            }
        else:
            # 已存在：检查冲突并合并
            existing_song = song_map[song_key]
            
            # 1. 别名冲突：合并 (策略B)
            new_alias = merge_aliases(existing_song["别名"], row["别名"])
            existing_song["别名"] = new_alias
            
            # 2. 本家冲突：记录 Error 日志
            existing_home = existing_song["本家"]
            new_home = row["本家"]
            if existing_home != new_home:
                if existing_home == "":
                    # 原来为空，直接更新
                    existing_song["本家"] = new_home
                elif new_home != "":
                    # 双方都有值且不同 -> 严重冲突
                    logger.error(
                        f"[本家冲突] 歌曲 '{song_name}' (作者: {nominal_author}) 本家信息不一致。"
                        f"原始行({existing_song['_source_csv_id']}): '{existing_home}', "
                        f"当前行({source_csv_id}): '{new_home}'。需人工处理。"
                    )

        # --- Author 注册逻辑 ---
        # 获取真实作者列表
        real_authors_list = parse_real_authors(row["真实作者"])
        if not real_authors_list:
            # 没有真实作者，使用名义作者
            real_authors_list = [nominal_author]
        
        for author_name in real_authors_list:
            if author_name not in author_map:
                auth_id = f"{str(author_id_counter).zfill(6)}"
                author_id_counter -= 1 # 自减
                author_map[author_name] = auth_id

    # ===================== 3. 第二轮遍历：生成 Relationship 数据 =====================
    logger.info("开始生成关联表数据...")
    
    game_song_rel_data = []
    song_author_rel_data = []
    # 注意：game_linkage_rel 需求未明确大幅变更，暂时保留简单逻辑或根据需要调整
    linkage_data = [] 
    
    song_author_rel_id_counter = 1000001 # 从 1000001 开始
    
    # 用于防止 song_author_rel 重复 (同一首歌可能在多行出现，不需要重复生成关联)
    generated_song_author_pairs = set()

    for idx, row in df_raw.iterrows():
        csv_rel_id = row["song_id"]
        source_game = row["来源"]
        update_time = row["更新时间"]
        song_name = row["歌名"]
        nominal_author = row["作者"]
        
        song_key = (song_name, nominal_author)
        if song_key not in song_map:
            continue # 理论上不会发生
            
        internal_song_id = song_map[song_key]["song_id"]
        current_home = song_map[song_key]["本家"] # 使用聚合后的本家

        # --- GameSongRel ---
        # 直接使用 CSV 的 song_id 作为 rel_id
        # 包含：rel_id, 来源游戏编号, song_id, 本家, 收录时间
        if source_game:
            game_song_rel_data.append({
                "rel_id": csv_rel_id,
                "游戏编号": source_game,
                "song_id": internal_song_id,
                "本家": current_home, # 记录名称
                "收录时间": get_standard_date(update_time),
                "最新更新时间": current_time
            })

        # --- SongAuthorRel ---
        real_authors_list = parse_real_authors(row["真实作者"])
        if not real_authors_list:
            real_authors_list = [nominal_author]
            
        for author_name in real_authors_list:
            if author_name not in author_map:
                continue
                
            pair_key = (internal_song_id, author_map[author_name])
            if pair_key in generated_song_author_pairs:
                continue # 避免重复关联
                
            generated_song_author_pairs.add(pair_key)
            
            rel_id = f"{str(song_author_rel_id_counter).zfill(7)}"
            song_author_rel_id_counter += 1
            
            song_author_rel_data.append({
                "rel_id": rel_id,
                "song_id": internal_song_id,
                "author_id": author_map[author_name],
                "曲名": song_name, # 冗余字段，方便核对
                "作者名": author_name, # 冗余字段
                "最新更新时间": current_time
            })

    # ===================== 4. 导出 CSV =====================
    
    # -- 导出 Song Info --
    # 移除辅助字段 _source_csv_id
    song_list = []
    for s in song_map.values():
        s_copy = s.copy()
        s_copy.pop("_source_csv_id", None)
        song_list.append(s_copy)
    
    df_song = pd.DataFrame(song_list)
    # 按生成的ID排序输出
    df_song = df_song.sort_values("song_id").reset_index(drop=True)
    # 确保列顺序
    song_cols = ["song_id", "歌名", "别名", "作者", "本家", "最新更新时间"]
    df_song = df_song[song_cols]
    df_song.to_csv(OUTPUT_PATHS["song_info"], encoding="utf-8-sig", index=False)
    logger.info(f"✅ 生成 song_info → {len(df_song)} 条")

    # -- 导出 Author Info --
    author_list = [{"author_id": aid, "作者名": aname, "别名": "", "备注": "", "最新更新时间": current_time} 
                   for aname, aid in author_map.items()]
    df_author = pd.DataFrame(author_list)
    df_author = df_author.sort_values("author_id", ascending=False).reset_index(drop=True) # ID大的排前面
    author_cols = ["author_id", "作者名", "别名", "备注", "最新更新时间"]
    df_author = df_author[author_cols]
    df_author.to_csv(OUTPUT_PATHS["author_info"], encoding="utf-8-sig", index=False)
    logger.info(f"✅ 生成 author_info → {len(df_author)} 条")

    # -- 导出 GameSongRel --
    df_game_song = pd.DataFrame(game_song_rel_data)
    gs_cols = ["rel_id", "游戏编号", "song_id", "本家", "收录时间", "最新更新时间"]
    # 防止列缺失报错
    for c in gs_cols:
        if c not in df_game_song.columns: df_game_song[c] = ""
    df_game_song = df_game_song[gs_cols]
    df_game_song.to_csv(OUTPUT_PATHS["game_song_rel"], encoding="utf-8", index=False)
    logger.info(f"✅ 生成 game_song_rel → {len(df_game_song)} 条")

    # -- 导出 SongAuthorRel --
    df_song_author = pd.DataFrame(song_author_rel_data)
    sa_cols = ["rel_id", "song_id", "author_id", "曲名", "作者名", "最新更新时间"]
    for c in sa_cols:
        if c not in df_song_author.columns: df_song_author[c] = ""
    df_song_author = df_song_author[sa_cols]
    df_song_author.to_csv(OUTPUT_PATHS["song_author_rel"], encoding="utf-8", index=False)
    logger.info(f"✅ 生成 song_author_rel → {len(df_song_author)} 条")

    # -- 导出 GameLinkageRel (占位逻辑，保持脚本完整性) --
    # 由于需求重点在Song/Author/Rel，此处仅保留空壳或简单逻辑
    pd.DataFrame(columns=["rel_id", "游戏1编号", "游戏2编号", "游戏1名称", "游戏2名称", "联动名称", "联动时间", "联动版本", "说明", "最新更新时间"])\
      .to_csv(OUTPUT_PATHS["game_linkage_rel"], encoding="utf-8", index=False)

    logger.info("🎉 所有处理完成！")
    return True

if __name__ == "__main__":
    os.makedirs(CSV_TARGET_DIR, exist_ok=True)
    extract_song_data_v2()