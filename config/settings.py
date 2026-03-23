import os
from pathlib import Path

# ===================== 基础路径（核心，保留） =====================
BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# ===================== CSV业务配置（保留） =====================
CSV_ROOT_DIR = os.path.join(BASE_DIR, "data", "csv")
os.makedirs(CSV_ROOT_DIR, exist_ok=True)

RAW_FILES = {
    "game_info": os.path.join(CSV_ROOT_DIR, "game_info_raw.csv"),
    "song_info": os.path.join(CSV_ROOT_DIR, "song_info_raw.csv")
}

TARGET_FILES = {
    "game_info": os.path.join(CSV_ROOT_DIR, "game_info.csv"),
    "song_info": os.path.join(CSV_ROOT_DIR, "song_info.csv"),
    "author_info": os.path.join(CSV_ROOT_DIR, "author_info.csv"),
    "game_song_rel": os.path.join(CSV_ROOT_DIR, "game_song_rel.csv"),
    "song_author_rel": os.path.join(CSV_ROOT_DIR, "song_author_rel.csv"),
    "game_linkage_rel": os.path.join(CSV_ROOT_DIR, "game_linkage_rel.csv")
}

ARCHIVE_DIR = os.path.join(CSV_ROOT_DIR, "archive")
os.makedirs(ARCHIVE_DIR, exist_ok=True)

STATE_FILE_PATH = os.path.join(CSV_ROOT_DIR, "csv_processed_state.json")
CLEAN_CONFIG = {
    "expire_days": 7,
    "exclude_suffix": ["_raw.csv"],
    "archive_old_files": True
}

# ===================== 数据库配置（保留） =====================
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "123456"),
    "database": os.getenv("DB_NAME", "rhythmgame"),
    "charset": "utf8mb4"
}

# ===================== Cron配置（保留，每天2点） =====================
PYTHON_EXEC_PATH = "python3"
CRON_TASK_MARK = "# 节奏游戏项目定时任务"
CRON_TASKS = [
    f"0 2 * * * {PYTHON_EXEC_PATH} {os.path.join(BASE_DIR, 'manage.py')} auto >> {os.path.join(LOG_DIR, 'auto_cron.log')} 2>&1"
]
CRON_BACKUP_DIR = LOG_DIR