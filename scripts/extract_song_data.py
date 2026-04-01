#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
歌曲数据提取脚本 V5.0（架构优化版）
✅ 核心原则：
1. 歌曲/作者去重 100% 依赖 extract_core_data.py
2. 本脚本仅负责：读取 core 输出 + 生成关联表
3. 彻底杜绝数据不一致
4. 集成 unitoken.csv 用于作者关联
5. 统一使用 utf-8-sig 编码
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

# 输入文件
RAW_SONG_CSV_PATH = os.path.join(CSV_TARGET_DIR, RAW_SONG_CSV_FILENAME)
UNITOKEN_CSV_PATH = os.path.join(CSV_TARGET_DIR, "unitoken.csv")

# Core 脚本输出文件（权威数据源）
CORE_INPUT_PATHS = {
    "song_info": os.path.join(CSV_TARGET_DIR, OUTPUT_CSV_FILENAMES["song_info"]),
    "author_info": os.path.join(CSV_TARGET_DIR, OUTPUT_CSV_FILENAMES["author_info"]),
    "temp_song_key_map": os.path.join(CSV_TARGET_DIR, "temp_song_key_map.csv")
}

# 输出文件
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

# ===================== unitoken 支持 =====================
def build_unitoken_map(unitoken_path):
    """读取 unitoken.csv，构建【原始作者Token → 统一作者Token】映射"""
    if not os.path.exists(unitoken_path):
        logger.warning(f"unitoken.csv 不存在，将尝试直接通过作者名关联")
        return {}
    try:
        df_unitoken = pd.read_csv(unitoken_path, encoding="utf-8-sig", dtype=str)
        unitoken_map = dict(zip(df_unitoken["原始Token"], df_unitoken["统一Token"]))
        logger.info(f"✅ 成功读取 unitoken.csv：共 {len(unitoken_map)} 条统一映射")
        return unitoken_map
    except Exception as e:
        logger.error(f"读取 unitoken.csv 失败：{str(e)}")
        return {}

def get_unified_token(original_token, unitoken_map):
    """获取统一后的作者Token"""
    return unitoken_map.get(original_token, original_token)

# ===================== 核心业务逻辑（仅关联表生成） =====================
def extract_song_data_v5():
    # 1. 检查并读取 Core 输出（权威数据源）
    logger.info("="*60)
    logger.info("步骤1：读取 Core 脚本输出（权威数据源）")
    logger.info("="*60)
    
    for path in CORE_INPUT_PATHS.values():
        if not os.path.exists(path):
            logger.error(f"❌ Core 输出文件不存在：{path}")
            logger.error(f"❌ 请先运行 extract_core_data.py！")
            return False

    # 读取 Core 输出（100% 信赖）
    df_song = pd.read_csv(CORE_INPUT_PATHS["song_info"], encoding="utf-8-sig", dtype=str)
    df_author = pd.read_csv(CORE_INPUT_PATHS["author_info"], encoding="utf-8-sig", dtype=str)
    df_key_map = pd.read_csv(CORE_INPUT_PATHS["temp_song_key_map"], encoding="utf-8-sig", dtype=str)
    
    logger.info(f"✅ 读取歌曲表：{len(df_song):,} 条")
    logger.info(f"✅ 读取作者表：{len(df_author):,} 条")
    logger.info(f"✅ 读取中间映射：{len(df_key_map):,} 条")

    # 2. 读取原始数据和 unitoken
    logger.info("\n" + "="*60)
    logger.info("步骤2：读取原始数据与 unitoken")
    logger.info("="*60)
    
    if not os.path.exists(RAW_SONG_CSV_PATH):
        logger.error(f"❌ 原始文件不存在：{RAW_SONG_CSV_PATH}")
        return False

    df_raw = pd.read_csv(RAW_SONG_CSV_PATH, encoding="utf-8-sig", dtype=str, na_filter=True)
    unitoken_map = build_unitoken_map(UNITOKEN_CSV_PATH)

    # 清洗原始数据
    for col in ["song_id", "歌名", "作者", "真实作者"]:
        df_raw[col] = df_raw[col].apply(clean_string)
    df_raw = df_raw[df_raw["song_id"] != ""].reset_index(drop=True)

    # 3. 构建映射字典
    logger.info("\n" + "="*60)
    logger.info("步骤3：构建关联映射字典")
    logger.info("="*60)
    
    # 原始song_id → 内部song_id（来自 Core）
    raw_sid_to_internal_sid = dict(zip(df_key_map["raw_song_id"], df_key_map["internal_song_id"]))
    # 内部song_id → 本家（来自 Core）
    internal_sid_to_home = dict(zip(df_song["song_id"], df_song["本家"]))
    # 作者名 → author_id（来自 Core）
    author_name_to_id = dict(zip(df_author["作者名"], df_author["author_id"]))

    logger.info(f"✅ 映射字典构建完成")

    # 4. 生成关联表
    logger.info("\n" + "="*60)
    logger.info("步骤4：生成关联表")
    logger.info("="*60)
    
    current_time = get_current_datetime()
    game_song_rel = []
    song_author_rel = []
    rel_id_counter = 1000001
    sa_pairs = set()

    for _, row in df_raw.iterrows():
        raw_sid = row["song_id"]
        game = row["来源"]
        date = row["更新时间"]
        song_name = row["歌名"]
        real_authors = parse_real_authors(row["真实作者"]) or [row["作者"]]

        # 获取内部 song_id
        internal_sid = raw_sid_to_internal_sid.get(raw_sid)
        if not internal_sid:
            continue

        # ============== 游戏-歌曲关联（100% 保留原始收录记录） ==============
        if game:
            game_song_rel.append({
                "rel_id": raw_sid,
                "游戏编号": game,
                "song_id": internal_sid,
                "本家": internal_sid_to_home.get(internal_sid, ""),
                "收录时间": get_standard_date(date),
                "最新更新时间": current_time
            })

        # ============== 歌曲-作者关联（通过作者名匹配 Core 的 author_id） ==============
        for a_name in real_authors:
            if a_name not in author_name_to_id:
                continue
            aid = author_name_to_id[a_name]
            
            if (internal_sid, aid) not in sa_pairs:
                sa_pairs.add((internal_sid, aid))
                song_author_rel.append({
                    "rel_id": f"{rel_id_counter:07d}",
                    "song_id": internal_sid,
                    "author_id": aid,
                    "曲名": song_name,
                    "作者名": a_name,
                    "最新更新时间": current_time
                })
                rel_id_counter += 1

    # 5. 导出文件
    logger.info("\n" + "="*60)
    logger.info("步骤5：导出关联表")
    logger.info("="*60)
    
    # 游戏-歌曲关联表
    df_gs_rel = pd.DataFrame(game_song_rel)
    df_gs_rel = df_gs_rel[["rel_id", "游戏编号", "song_id", "本家", "收录时间", "最新更新时间"]]
    df_gs_rel.to_csv(OUTPUT_PATHS["game_song_rel"], encoding="utf-8-sig", index=False)

    # 歌曲-作者关联表
    pd.DataFrame(song_author_rel).to_csv(OUTPUT_PATHS["song_author_rel"], encoding="utf-8-sig", index=False)

    # 游戏联动关联表（空表）
    pd.DataFrame(columns=["rel_id","游戏1编号","游戏2编号","联动名称","最新更新时间"]).to_csv(OUTPUT_PATHS["game_linkage_rel"], encoding="utf-8-sig", index=False)

    # 6. 最终统计
    logger.info("\n" + "="*60)
    logger.info(f"✅ 游戏歌曲关联表：{len(df_gs_rel):,} 条")
    logger.info(f"✅ 歌曲作者关联表：{len(song_author_rel):,} 条")
    logger.info("="*60)
    logger.info("🎉 关联表生成完成！（数据100% 来自 Core）")
    return True

if __name__ == "__main__":
    os.makedirs(CSV_TARGET_DIR, exist_ok=True)
    extract_song_data_v5()