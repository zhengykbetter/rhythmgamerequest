#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主项目核心配置文件（开源）
所有路径、参数集中管理，避免硬编码
"""
import os
from pathlib import Path

# ===================== 基础路径配置 =====================
# 主项目根目录（动态获取，适配不同部署环境）
MAIN_REPO_ROOT = Path(__file__).parent.parent  # config目录的上一级（主仓库根）
# 私有CSV仓库根目录（服务器实际路径）
PRIVATE_CSV_REPO_ROOT = Path("/opt/csv_repo")

# ===================== CSV同步配置 =====================
# CSV源目录（私有仓库下的子目录：result）
CSV_SOURCE_SUBDIR = "result"
CSV_SOURCE_DIR = PRIVATE_CSV_REPO_ROOT / CSV_SOURCE_SUBDIR
# CSV目标目录（主仓库下的子目录：data_csv，git忽略）
CSV_TARGET_SUBDIR = "data_csv"
CSV_TARGET_DIR = MAIN_REPO_ROOT / CSV_TARGET_SUBDIR
# 需要同步的CSV文件列表（核心改：song_info → songraw_info）
REQUIRED_CSV_FILES = ["game_info.csv", "songraw_info.csv"]
# 私有CSV仓库分支
CSV_REPO_BRANCH = "main"

# ===================== 日志配置 =====================
# 日志子目录（主仓库下）
LOG_SUBDIR = "logs"
LOG_DIR = MAIN_REPO_ROOT / LOG_SUBDIR
# 日志文件名格式
LOG_FILE_PREFIX = "sync_csv_"
LOG_FILE_SUFFIX = ".log"

# ===================== 定时任务（cron）配置 =====================
# Python执行路径（服务器实际路径）
PYTHON_EXEC_PATH = "/usr/bin/python3"
# Cron备份目录（存放crontab备份文件）
CRON_BACKUP_SUBDIR = "logs"
CRON_BACKUP_DIR = MAIN_REPO_ROOT / CRON_BACKUP_SUBDIR
# Cron任务配置（核心：所有定时任务集中定义，语法简化）
CRON_TASKS = [
    f"0 2 * * * {PYTHON_EXEC_PATH} {MAIN_REPO_ROOT}/scripts/sync_csv_from_remote.py >> {LOG_DIR}/crontab_sync.log 2>&1",
    f"5 2 * * * {PYTHON_EXEC_PATH} {MAIN_REPO_ROOT}/scripts/csv_to_db.py >> {LOG_DIR}/crontab_db.log 2>&1"
]
# Cron任务特征标记（用于精准匹配/删除本项目任务）
CRON_TASK_MARK = str(MAIN_REPO_ROOT) + "/"

# ===================== 数据库配置（CSV→DB） =====================
# 敏感信息通过环境变量读取，不硬编码（.env文件git忽略）
DB_CONFIG = {
    "user": os.getenv("DB_USER", "default_user"),
    "password": os.getenv("DB_PASSWORD", ""),
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "db": os.getenv("DB_NAME", "rhythmgame"),
    "charset": "utf8mb4"
}