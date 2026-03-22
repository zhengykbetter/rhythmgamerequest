#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主仓库CSV同步脚本（开源+详细日志）
功能：
1. 拉取私有CSV仓库的最新CSV
2. 校验CSV完整性（存在性+大小+MD5）
3. 复制CSV到主仓库的data_csv目录（git忽略）
4. 记录超详细同步日志（文件大小/耗时/MD5/复制结果）
"""
import os
import sys
import shutil
import logging
import subprocess
import hashlib
import time
from datetime import datetime
from pathlib import Path

# ===================== 核心配置 =====================
PRIVATE_CSV_REPO = Path("/opt/csv_repo")
CSV_SOURCE_DIR = PRIVATE_CSV_REPO / "data"
CSV_TARGET_DIR = Path(__file__).parent.parent / "data_csv"
LOG_DIR = Path(__file__).parent.parent / "logs"
REQUIRED_CSV = ["game_info.csv", "song_info.csv"]
CSV_REPO_BRANCH = "main"

# ===================== 详细日志配置 =====================
def setup_logger():
    """配置超详细日志：包含时间戳/级别/模块/耗时/详细信息"""
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = LOG_DIR / f"sync_csv_{datetime.now().strftime('%Y%m%d')}.log"
    
    logger = logging.getLogger("main_repo_csv_sync")
    logger.setLevel(logging.DEBUG)  # 调试级别，记录所有细节
    if logger.handlers:
        return logger
    
    # 日志格式：[时间] [级别] [耗时] [信息]
    fmt = "%(asctime)s - %(levelname)s - [耗时：%(relativeCreated)dms] - 同步流程：%(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    
    # 文件日志（保存所有细节，开源可展示）
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
    # 控制台日志（简化输出，便于手动执行查看）
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt=datefmt))
    console_handler.setLevel(logging.INFO)  # 控制台只显示INFO及以上
    
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

# ===================== 核心函数（强化日志） =====================
def pull_private_csv_repo():
    """拉取私有CSV仓库（详细日志）"""
    logger.info("===== 开始拉取私有CSV仓库 ======")
    start_time = time.time()
    try:
        result = subprocess.run(
            ["git", "pull", "origin", CSV_REPO_BRANCH],
            cwd=PRIVATE_CSV_REPO,
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
    """复制CSV（详细日志：文件大小/MD5/复制结果）"""
    logger.info("===== 开始复制CSV到主仓库data_csv目录 ======")
    os.makedirs(CSV_TARGET_DIR, exist_ok=True)
    copy_summary = {"成功": [], "失败": [], "MD5": []}
    
    # 清理旧CSV
    old_files = [f for f in os.listdir(CSV_TARGET_DIR) if f.endswith(".csv")]
    if old_files:
        logger.info(f"清理主仓库旧CSV文件：{old_files}")
        for f in old_files:
            os.remove(CSV_TARGET_DIR / f)
    
    # 复制新CSV并记录细节
    for csv_file in REQUIRED_CSV:
        source_path = CSV_SOURCE_DIR / csv_file
        target_path = CSV_TARGET_DIR / csv_file
        logger.debug(f"处理文件：源路径={source_path}，目标路径={target_path}")
        
        # 检查源文件
        if not os.path.exists(source_path):
            logger.error(f"源文件缺失：{source_path}")
            copy_summary["失败"].append(csv_file)
            continue
        
        # 获取源文件信息
        file_size = os.path.getsize(source_path) / 1024  # 转为KB
        source_md5 = get_file_md5(source_path)
        logger.debug(f"源文件{csv_file}信息：大小={file_size:.2f}KB，MD5={source_md5}")
        
        # 复制文件
        shutil.copy2(source_path, target_path)
        
        # 验证目标文件
        if os.path.exists(target_path):
            target_md5 = get_file_md5(target_path)
            copy_summary["成功"].append(csv_file)
            copy_summary["MD5"].append(f"{csv_file}: 源{source_md5} → 目标{target_md5}")
            logger.info(f"CSV复制成功：{csv_file}（大小={file_size:.2f}KB，MD5校验：{source_md5 == target_md5}）")
        else:
            copy_summary["失败"].append(csv_file)
            logger.error(f"CSV复制失败：目标文件{target_path}未生成")
    
    # 汇总复制结果
    logger.info(f"CSV复制汇总：成功{len(copy_summary['成功'])}个，失败{len(copy_summary['失败'])}个")
    if copy_summary["MD5"]:
        logger.debug(f"MD5校验详情：{copy_summary['MD5']}")
    return len(copy_summary["失败"]) == 0

def main():
    """主流程（详细日志）"""
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