#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
歌曲数据提取脚本 V6.0（修复Token关联逻辑版）
✅ 核心修复：完全对齐Core脚本的Token链路，解决作者关联丢失问题
✅ 保留：原游戏名无效调试逻辑全量不变
"""
import os
import sys
import ast
import pandas as pd
import logging
from datetime import datetime
from collections import defaultdict, Counter

# ===================== 日志系统 =====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ===================== 路径配置（完全保留原配置不变） =====================
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
GAME_INFO_CSV_PATH = os.path.join(CSV_TARGET_DIR, "game_info.csv")
SONG_TOKEN_CSV_PATH = os.path.join(CSV_TARGET_DIR, "songtoken.csv")  # 新增：核心Token数据源

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

# ===================== 工具函数（完全保留原函数，新增必要函数） =====================
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

# ===================== 完全对齐Core脚本的Token映射构建（核心修复） =====================
def build_full_token_maps(unitoken_path, songtoken_path):
    """
    1:1复刻Core脚本的Token映射逻辑，保证和author_info的author_id完全对齐
    返回：
    - alias_to_main_map: 原始Token→统一Token
    - token_to_unified: 全量原始Token→统一Token（含无映射的自映射）
    - main_token_order: 统一Token的排序（和Core脚本生成author_id的顺序完全一致）
    """
    # 1. 读取unitoken构建基础映射
    if not os.path.exists(unitoken_path):
        logger.warning(f"unitoken.csv 不存在，将使用原始Token直接映射")
        alias_to_main_map = {}
        main_token_info = defaultdict(lambda: {"main_name": "", "aliases": set()})
    else:
        df_unitoken = pd.read_csv(unitoken_path, encoding="utf-8-sig", dtype=str)
        alias_to_main_map = dict(zip(df_unitoken["原始Token"], df_unitoken["统一Token"]))
        # 构建主Token信息，和Core脚本完全一致
        main_token_info = defaultdict(lambda: {"main_name": "", "aliases": set()})
        for _, row in df_unitoken.iterrows():
            main_tok = row["统一Token"]
            main_name = row["统一作者名"]
            alias_name = row["原始作者名"]
            main_token_info[main_tok]["main_name"] = main_name
            if alias_name != main_name:
                main_token_info[main_tok]["aliases"].add(alias_name)

    # 2. 全量展开songtoken的原始Token
    df_token = pd.read_csv(songtoken_path, encoding="utf-8-sig", dtype=str)
    all_raw_tokens = set()
    song_raw_token_map = {}  # song_id→原始作者Token列表（核心关联用）
    for _, row in df_token.iterrows():
        sid = row["song_id"]
        auth_toks = parse_author_tokens(row["作者token"])
        song_raw_token_map[sid] = auth_toks
        all_raw_tokens.update(auth_toks)

    # 3. 构建全量原始Token→统一Token映射，和Core脚本完全一致
    token_to_unified = {}
    for raw_tok in all_raw_tokens:
        if raw_tok in alias_to_main_map:
            token_to_unified[raw_tok] = alias_to_main_map[raw_tok]
        else:
            token_to_unified[raw_tok] = raw_tok

    # 4. 补充无映射Token的主信息，保证排序和Core脚本完全一致
    for raw_tok in all_raw_tokens:
        main_tok = token_to_unified[raw_tok]
        if main_tok not in main_token_info:
            main_token_info[main_tok] = {"main_name": raw_tok, "aliases": set()}

    # 5. 生成和Core脚本完全一致的统一Token排序（author_id生成顺序）
    main_token_order = sorted(main_token_info.keys())

    return alias_to_main_map, token_to_unified, main_token_order, song_raw_token_map

# ===================== 核心业务逻辑 =====================
def extract_song_data_v6():
    current_time = get_current_datetime()

    # 1. 检查并读取 Core 输出（权威数据源，完全保留）
    logger.info("="*80)
    logger.info("步骤1：读取 Core 脚本输出（权威数据源）")
    logger.info("="*80)
    
    for path in CORE_INPUT_PATHS.values():
        if not os.path.exists(path):
            logger.error(f"❌ Core 输出文件不存在：{path}")
            logger.error(f"❌ 请先运行 extract_core_data.py！")
            return False

    df_song = pd.read_csv(CORE_INPUT_PATHS["song_info"], encoding="utf-8-sig", dtype=str)
    df_author = pd.read_csv(CORE_INPUT_PATHS["author_info"], encoding="utf-8-sig", dtype=str)
    df_key_map = pd.read_csv(CORE_INPUT_PATHS["temp_song_key_map"], encoding="utf-8-sig", dtype=str)
    
    logger.info(f"✅ 读取歌曲表：{len(df_song):,} 条")
    logger.info(f"✅ 读取作者表：{len(df_author):,} 条")
    logger.info(f"✅ 读取中间映射：{len(df_key_map):,} 条")

    # 2. 读取所有数据源 + 构建全量Token映射（核心新增）
    logger.info("\n" + "="*80)
    logger.info("步骤2：读取原始数据、构建全量Token映射（对齐Core脚本）")
    logger.info("="*80)
    
    # 检查必要文件
    for path in [RAW_SONG_CSV_PATH, GAME_INFO_CSV_PATH, SONG_TOKEN_CSV_PATH]:
        if not os.path.exists(path):
            logger.error(f"❌ 必要文件不存在：{path}")
            return False

    # 读取原始数据、游戏信息表（完全保留原逻辑）
    df_raw = pd.read_csv(RAW_SONG_CSV_PATH, encoding="utf-8-sig", dtype=str, na_filter=True)
    df_game = pd.read_csv(GAME_INFO_CSV_PATH, encoding="utf-8-sig")
    game_name_to_id = dict(zip(df_game["游戏"], df_game["游戏编号"].astype(int)))
    
    # 构建全量Token映射（1:1对齐Core脚本，核心修复）
    alias_to_main_map, token_to_unified, main_token_order, song_raw_token_map = build_full_token_maps(
        UNITOKEN_CSV_PATH, SONG_TOKEN_CSV_PATH
    )
    
    # 构建 统一Token → author_id 映射（和Core脚本生成的author_id 100%匹配）
    if len(main_token_order) != len(df_author):
        logger.error(f"❌ 致命错误：Token映射和author_info行数不匹配！Core脚本和当前脚本输入文件不一致")
        logger.error(f"   统一Token数量：{len(main_token_order)} | author_info行数：{len(df_author)}")
        return False
    unified_token_to_aid = dict(zip(main_token_order, df_author["author_id"]))
    
    logger.info(f"✅ 成功读取 game_info.csv：共 {len(game_name_to_id)} 个游戏映射")
    logger.info(f"✅ 成功构建全量Token映射：共 {len(token_to_unified)} 个原始Token映射")
    logger.info(f"✅ 成功构建统一Token→author_id映射：共 {len(unified_token_to_aid)} 个作者实体")

    # 清洗原始数据（完全保留原逻辑）
    for col in ["song_id", "歌名", "作者", "真实作者"]:
        df_raw[col] = df_raw[col].apply(clean_string)
    df_raw = df_raw[df_raw["song_id"] != ""].reset_index(drop=True)
    logger.info(f"✅ 原始数据清洗完成：共 {len(df_raw):,} 条有效记录")

    # 3. 构建基础映射字典
    logger.info("\n" + "="*80)
    logger.info("步骤3：构建关联映射字典")
    logger.info("="*80)
    
    # 原始song_id → 内部song_id（来自Core，完全保留）
    raw_sid_to_internal_sid = dict(zip(df_key_map["raw_song_id"], df_key_map["internal_song_id"]))
    # 内部song_id → 本家（来自Core，完全保留）
    internal_sid_to_home = dict(zip(df_song["song_id"], df_song["本家"]))
    # 作者名辅助映射（仅用于日志展示，不用于匹配）
    aid_to_author_name = dict(zip(df_author["author_id"], df_author["作者名"]))

    logger.info(f"✅ 映射字典构建完成")

    # 4. 生成关联表（保留原游戏调试逻辑，重写作者关联逻辑）
    logger.info("\n" + "="*80)
    logger.info("步骤4：生成关联表（保留游戏名调试，修复作者关联）")
    logger.info("="*80)
    
    game_song_rel = []
    song_author_rel = []
    rel_id_counter = 1000001
    sa_pairs = set()
    gs_pairs = set()
    
    # 原游戏关联调试统计（完全保留）
    debug_gs_stats = {
        "total_candidates": 0,
        "skipped_no_internal_sid": 0,
        "skipped_invalid_game": 0,
        "duplicates_removed": 0,
        "final_kept": 0
    }
    invalid_game_counter = Counter()
    invalid_game_samples = defaultdict(list)

    # 新增：作者关联调试统计
    debug_sa_stats = {
        "total_raw_songs": 0,
        "skipped_no_internal_sid": 0,
        "skipped_no_token": 0,
        "total_token_processed": 0,
        "token_mapping_failed": 0,
        "aid_lookup_failed": 0,
        "duplicates_removed": 0,
        "final_kept": 0
    }
    failed_token_counter = Counter()

    for idx, row in df_raw.iterrows():
        raw_sid = row["song_id"]
        game_name = row["来源"]
        date = row["更新时间"]
        song_name = row["歌名"]
        real_authors = parse_real_authors(row["真实作者"]) or [row["作者"]]

        # 获取内部 song_id（共用逻辑）
        internal_sid = raw_sid_to_internal_sid.get(raw_sid)
        if not internal_sid:
            debug_gs_stats["skipped_no_internal_sid"] += 1
            debug_sa_stats["skipped_no_internal_sid"] += 1
            continue

        # ============== 游戏-歌曲关联（完全保留原逻辑+调试，无任何修改） ==============
        if game_name:
            debug_gs_stats["total_candidates"] += 1
            
            if game_name in game_name_to_id:
                game_id = game_name_to_id[game_name]
                dedup_key = (internal_sid, game_id)
                
                if dedup_key in gs_pairs:
                    debug_gs_stats["duplicates_removed"] += 1
                    continue
                
                gs_pairs.add(dedup_key)
                debug_gs_stats["final_kept"] += 1
                
                game_song_rel.append({
                    "rel_id": raw_sid,
                    "游戏编号": game_id,
                    "游戏名": game_name,
                    "song_id": internal_sid,
                    "本家": internal_sid_to_home.get(internal_sid, ""),
                    "收录时间": get_standard_date(date),
                    "最新更新时间": current_time
                })
            else:
                debug_gs_stats["skipped_invalid_game"] += 1
                invalid_game_counter[game_name] += 1
                if len(invalid_game_samples[game_name]) < 3:
                    invalid_game_samples[game_name].append({
                        "raw_sid": raw_sid,
                        "song_name": song_name
                    })

        # ============== 歌曲-作者关联（完全重写，Token链路1:1对齐Core脚本） ==============
        debug_sa_stats["total_raw_songs"] += 1
        # 1. 从songtoken获取该歌曲的原始作者Token列表（唯一可靠依据）
        raw_auth_tokens = song_raw_token_map.get(raw_sid, [])
        if not raw_auth_tokens:
            debug_sa_stats["skipped_no_token"] += 1
            continue

        # 2. 遍历每个Token，完成映射匹配
        for tok in raw_auth_tokens:
            debug_sa_stats["total_token_processed"] += 1
            # 3. 映射为统一Token（和Core脚本完全一致）
            unified_tok = token_to_unified.get(tok)
            if not unified_tok:
                debug_sa_stats["token_mapping_failed"] += 1
                failed_token_counter[tok] += 1
                continue
            # 4. 匹配author_id（和Core脚本生成的ID完全一致）
            aid = unified_token_to_aid.get(unified_tok)
            if not aid:
                debug_sa_stats["aid_lookup_failed"] += 1
                failed_token_counter[tok] += 1
                continue

            # 5. 去重
            dedup_key = (internal_sid, aid)
            if dedup_key in sa_pairs:
                debug_sa_stats["duplicates_removed"] += 1
                continue
            sa_pairs.add(dedup_key)
            debug_sa_stats["final_kept"] += 1

            # 6. 生成关联记录（保留原字段，补充正确信息）
            song_author_rel.append({
                "rel_id": f"{rel_id_counter:07d}",
                "song_id": internal_sid,
                "author_id": aid,
                "曲名": song_name,
                "作者名": aid_to_author_name.get(aid, ""),  # 从author_info取标准名，避免别名混乱
                "最新更新时间": current_time
            })
            rel_id_counter += 1

    # 🔥 原游戏关联调试输出（完全保留）
    logger.info("\n" + "="*80)
    logger.info("📊 【游戏-歌曲关联】调试统计")
    logger.info("="*80)
    logger.info(f"   原始候选记录数：{debug_gs_stats['total_candidates']:,}")
    logger.info(f"   跳过（无 internal_sid）：{debug_gs_stats['skipped_no_internal_sid']:,}")
    logger.info(f"   🔥 跳过（无效游戏名）：{debug_gs_stats['skipped_invalid_game']:,}")
    logger.info(f"   去重移除：{debug_gs_stats['duplicates_removed']:,} 条")
    logger.info(f"   最终保留：{debug_gs_stats['final_kept']:,} 条")
    
    if invalid_game_counter:
        logger.info(f"\n   ❌ 无效游戏名统计（按出现次数排序）：")
        for game_name, count in invalid_game_counter.most_common(20):
            logger.info(f"      - {game_name}: {count:,} 条")
            if game_name in invalid_game_samples:
                samples = invalid_game_samples[game_name]
                logger.info(f"         样本：{[s['song_name'] for s in samples]}")
        logger.info(f"\n   💡 提示：请检查 game_info.csv 是否包含以上游戏名！")
        logger.info(f"   当前 game_info.csv 中的游戏：{list(game_name_to_id.keys())}")

    # 🔥 新增：作者关联调试输出
    logger.info("\n" + "="*80)
    logger.info("📊 【歌曲-作者关联】调试统计")
    logger.info("="*80)
    logger.info(f"   原始歌曲记录数：{debug_sa_stats['total_raw_songs']:,}")
    logger.info(f"   跳过（无 internal_sid）：{debug_sa_stats['skipped_no_internal_sid']:,}")
    logger.info(f"   跳过（无作者Token）：{debug_sa_stats['skipped_no_token']:,}")
    logger.info(f"   总处理Token数：{debug_sa_stats['total_token_processed']:,}")
    logger.info(f"   Token映射失败：{debug_sa_stats['token_mapping_failed']:,}")
    logger.info(f"   author_id匹配失败：{debug_sa_stats['aid_lookup_failed']:,}")
    logger.info(f"   去重移除：{debug_sa_stats['duplicates_removed']:,} 条")
    logger.info(f"   最终保留：{debug_sa_stats['final_kept']:,} 条")

    if failed_token_counter:
        logger.info(f"\n   ❌ 失败Token统计（按出现次数排序）：")
        for tok, count in failed_token_counter.most_common(20):
            logger.info(f"      - {tok}: {count:,} 条")

    # 5. 导出文件（完全保留原输出结构）
    logger.info("\n" + "="*80)
    logger.info("步骤5：导出关联表")
    logger.info("="*80)
    
    # 游戏-歌曲关联表（完全保留原结构）
    df_gs_rel = pd.DataFrame(game_song_rel)
    df_gs_rel = df_gs_rel[["rel_id", "游戏编号", "游戏名", "song_id", "本家", "收录时间", "最新更新时间"]]
    df_gs_rel.to_csv(OUTPUT_PATHS["game_song_rel"], encoding="utf-8-sig", index=False)

    # 歌曲-作者关联表（修复后）
    df_sa_rel = pd.DataFrame(song_author_rel)
    df_sa_rel.to_csv(OUTPUT_PATHS["song_author_rel"], encoding="utf-8-sig", index=False)

    # 游戏联动关联表（完全保留原结构）
    pd.DataFrame(columns=["rel_id","游戏1编号","游戏2编号","联动名称","最新更新时间"]).to_csv(OUTPUT_PATHS["game_linkage_rel"], encoding="utf-8-sig", index=False)

    # 6. 最终统计
    logger.info("\n" + "="*80)
    logger.info(f"✅ 游戏歌曲关联表：{len(df_gs_rel):,} 条")
    logger.info(f"✅ 歌曲作者关联表：{len(df_sa_rel):,} 条")
    logger.info("="*80)
    logger.info("🎉 关联表生成完成！作者关联100%对齐Core脚本Token逻辑")
    return True

if __name__ == "__main__":
    os.makedirs(CSV_TARGET_DIR, exist_ok=True)
    extract_song_data_v6()