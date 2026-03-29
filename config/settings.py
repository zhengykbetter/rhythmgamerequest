#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主项目核心配置文件（开源）
所有路径、参数、颜色集中管理
"""
import os
from pathlib import Path

# ===================== 基础路径配置 =====================
MAIN_REPO_ROOT = Path(__file__).parent.parent  # config目录的上一级（主仓库根）
PRIVATE_CSV_REPO_ROOT = Path("/opt/csv_repo")

# ===================== CSV同步配置 =====================
CSV_SOURCE_SUBDIR = "result"
CSV_SOURCE_DIR = PRIVATE_CSV_REPO_ROOT / CSV_SOURCE_SUBDIR
CSV_TARGET_SUBDIR = "data_csv"
CSV_TARGET_DIR = MAIN_REPO_ROOT / CSV_TARGET_SUBDIR
REQUIRED_CSV_FILES = ["game_info.csv", "songraw_info.csv"]
CSV_REPO_BRANCH = "main"

# ===================== CSV文件名配置（新增：集中管理所有CSV文件名） =====================
# 原始歌曲数据文件名（你的实际文件名：songraw_info.csv）
RAW_SONG_CSV_FILENAME = "songraw_info.csv"
# 输出表文件名（5个标准化表，统一配置）
OUTPUT_CSV_FILENAMES = {
    "song_info": "song_info.csv",
    "author_info": "author_info.csv",
    "game_song_rel": "game_song_rel.csv",
    "song_author_rel": "song_author_rel.csv",
    "game_linkage_rel": "game_linkage_rel.csv"
}

# ===================== 日志配置 =====================
LOG_SUBDIR = "logs"
LOG_DIR = MAIN_REPO_ROOT / LOG_SUBDIR
LOG_FILE_PREFIX = "sync_csv_"
LOG_FILE_SUFFIX = ".log"

# ===================== 定时任务（cron）配置 =====================
PYTHON_EXEC_PATH = "/usr/bin/python3"
CRON_BACKUP_SUBDIR = "logs"
CRON_BACKUP_DIR = MAIN_REPO_ROOT / CRON_BACKUP_SUBDIR
CRON_TASKS = [
    f"0 2 * * * {PYTHON_EXEC_PATH} {MAIN_REPO_ROOT}/scripts/sync_csv_from_remote.py >> {LOG_DIR}/crontab_sync.log 2>&1",
    f"5 2 * * * {PYTHON_EXEC_PATH} {MAIN_REPO_ROOT}/scripts/csv_to_db.py >> {LOG_DIR}/crontab_db.log 2>&1"
]
CRON_TASK_MARK = str(MAIN_REPO_ROOT) + "/"

# ===================== 数据库配置 =====================
DB_CONFIG = {
    "user": os.getenv("DB_USER", "default_user"),
    "password": os.getenv("DB_PASSWORD", ""),
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "db": os.getenv("DB_NAME", "rhythmgame"),
    "charset": "utf8mb4"
}

# ===================== 全局颜色配置 =====================
COLORS = {
    "RED": "\033[0;31m",
    "GREEN": "\033[0;32m",
    "YELLOW": "\033[1;33m",
    "BOLD_RED": "\033[1;31m",
    "NC": "\033[0m"  # 重置颜色
}

# ===================== Web 部署配置（新增：网站运行核心配置） =====================
# Web 应用入口目录（你要运行的 frontend/demo，后续改这里即可）
WEB_APP_DIR = MAIN_REPO_ROOT / "frontend"
# Gunicorn 绑定地址（本地监听，安全）
GUNICORN_BIND_HOST = "127.0.0.1"
GUNICORN_BIND_PORT = 8000
# Flask 应用入口（固定写法，无需修改）
FLASK_APP_ENTRY = "run:app"
# Gunicorn 工作进程数
GUNICORN_WORKERS = 2
# 项目日志目录
WEB_LOG_DIR = LOG_DIR