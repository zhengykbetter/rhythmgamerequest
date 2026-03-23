#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主仓库CSV同步脚本（开源+配置分离）
"""
import sys
import os
import shutil
import logging
import subprocess
import hashlib
import time
from datetime import datetime

# 新增：强制添加主仓库根目录到Python路径（解决ModuleNotFoundError）
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    CSV_SOURCE_DIR, CSV_TARGET_DIR, LOG_DIR,
    REQUIRED_CSV_FILES, CSV_REPO_BRANCH,
    PRIVATE_CSV_REPO_ROOT, LOG_FILE_PREFIX, LOG_FILE_SUFFIX
)

# ===================== 详细日志配置 =====================
def setup_logger():
    """配置超详细日志：引用配置文件的路径"""
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = LOG_DIR / f"{LOG_FILE_PREFIX}{datetime.now().strftime('%Y%m%d')}{LOG_FILE_SUFFIX}"
    
    logger = logging.getLogger("main_repo_csv_sync")
    logger.setLevel(logging.DEBUG)
    if logger.handlers:
        return logger
    
    # 日志格式：[时间] [级别] [耗时] [信息]
    fmt = "%(asctime)s - %(levelname)s - [耗时：%(relativeCreated)dms] - 同步流程：%(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    
    # 文件日志（保存所有细节）
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
    # 控制台日志（简化输出）
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt=datefmt))
    console_handler.setLevel(logging.INFO)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger

logger = setup_logger()

# ===================== 辅助函数：计算文件MD5 =====================
def get_file_md5(file_path):
    """计算文件MD5，用于校验文件完整性"""
    if not os.path.exists(file_path):
        return "文件不存在"
    md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        while chunk := f.read(4096):
            md5.update(chunk)
    return md5.hexdigest()

# ===================== 辅助函数：统计CSV非空行数 =====================
def count_csv_rows(file_path):
    """统计CSV文件非空行数（跳过空行，避免干扰）"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # 统计非空行（strip()后长度>0）
            row_count = sum(1 for line in f if line.strip())
        return row_count
    except Exception as e:
        logger.error(f"统计{file_path}行数失败：{str(e)}")
        return 0

# ===================== 核心函数（引用配置） =====================
def pull_private_csv_repo():
    """拉取私有CSV仓库（引用配置文件的分支/路径）"""
    logger.info("===== 开始拉取私有CSV仓库 ======")
    start_time = time.time()
    try:
        result = subprocess.run(
            ["git", "pull", "origin", CSV_REPO_BRANCH],
            cwd=PRIVATE_CSV_REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=30
        )
        elapsed = round((time.time() - start_time) * 1000, 2)
        if result.returncode == 0:
            logger.debug(f"Git拉取命令输出：{result.stdout.strip()}")
            logger.info(f"私有CSV仓库拉取成功（耗时{elapsed}ms），分支：{CSV_REPO_BRANCH}")
            return True
        else:
            logger.error(f"Git拉取失败（耗时{elapsed}ms），错误信息：{result.stderr.strip()}")
            logger.debug(f"Git完整错误输出：{result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        elapsed = round((time.time() - start_time) * 1000, 2)
        logger.error(f"Git拉取超时（30秒），耗时{elapsed}ms")
        return False
    except Exception as e:
        elapsed = round((time.time() - start_time) * 1000, 2)
        logger.error(f"拉取私有CSV仓库异常（耗时{elapsed}ms）：{str(e)}", exc_info=True)
        return False

def copy_csv_to_main_repo():
    """复制CSV（引用配置文件的路径/文件列表）"""
    logger.info("===== 开始复制CSV到主仓库目标目录 ======")
    os.makedirs(CSV_TARGET_DIR, exist_ok=True)
    copy_summary = {"成功": [], "失败": [], "MD5": []}
    
    # 清理旧CSV
    old_files = [f for f in os.listdir(CSV_TARGET_DIR) if f.endswith(".csv")]
    if old_files:
        logger.info(f"清理主仓库旧CSV文件：{old_files}")
        for f in old_files:
            os.remove(CSV_TARGET_DIR / f)
    
    # 复制新CSV并记录细节（引用配置的文件列表，自动适配songraw_info.csv）
    for csv_file in REQUIRED_CSV_FILES:
        source_path = CSV_SOURCE_DIR / csv_file
        target_path = CSV_TARGET_DIR / csv_file
        logger.debug(f"处理文件：源路径={source_path}，目标路径={target_path}")
        
        # 检查源文件
        if not os.path.exists(source_path):
            logger.error(f"源文件缺失：{source_path}")
            copy_summary["失败"].append(csv_file)
            continue
        
        # 获取源文件核心信息（新增：行数、MB单位大小、精确到分钟的时间）
        file_size_kb = os.path.getsize(source_path) / 1024  # 原有KB
        file_size_mb = round(file_size_kb / 1024, 2)        # 新增MB（保留2位小数）
        csv_row_count = count_csv_rows(source_path)         # 新增：CSV总行数
        precise_time = datetime.now().strftime("%Y-%m-%d %H:%M")  # 精确到分钟的时间
        source_md5 = get_file_md5(source_path)
        
        logger.debug(f"源文件{csv_file}信息：大小={file_size_kb:.2f}KB（{file_size_mb}MB），总行数={csv_row_count}，MD5={source_md5}")
        
        # 复制文件
        shutil.copy2(source_path, target_path)
        
        # 验证目标文件
        if os.path.exists(target_path):
            target_md5 = get_file_md5(target_path)
            copy_summary["成功"].append(csv_file)
            copy_summary["MD5"].append(f"{csv_file}: 源{source_md5} → 目标{target_md5}")
            
            # 增强日志：包含时间（分钟）、文件名、总行数、大小（MB）、MD5校验结果
            logger.info(f"[{precise_time}] CSV复制成功 | 文件名：{csv_file} | 总行数：{csv_row_count} | 文件大小：{file_size_mb}MB | MD5校验：{source_md5 == target_md5} | 源路径：{source_path} → 目标路径：{target_path}")
        else:
            copy_summary["失败"].append(csv_file)
            logger.error(f"[{precise_time}] CSV复制失败：目标文件{target_path}未生成 | 文件名：{csv_file}")
    
    # 汇总复制结果
    logger.info(f"CSV复制汇总：成功{len(copy_summary['成功'])}个，失败{len(copy_summary['失败'])}个")
    if copy_summary["MD5"]:
        logger.debug(f"MD5校验详情：{copy_summary['MD5']}")
    return len(copy_summary["失败"]) == 0

def main():
    """主流程"""
    logger.info("===== 主仓库CSV同步流程启动 ======")
    start_total = time.time()
    
    # 1. 拉取私有仓库
    pull_ok = pull_private_csv_repo()
    if not pull_ok:
        logger.error("私有仓库拉取失败，同步流程终止")
        sys.exit(1)
    
    # 2. 复制CSV到主仓库
    copy_ok = copy_csv_to_main_repo()
    
    # 3. 总耗时汇总
    total_elapsed = round((time.time() - start_total) * 1000, 2)
    if copy_ok:
        logger.info(f"===== 同步流程完成（总耗时{total_elapsed}ms）：所有CSV复制成功 ======")
    else:
        logger.warning(f"===== 同步流程完成（总耗时{total_elapsed}ms）：部分CSV复制失败 ======")

if __name__ == "__main__":
    main()