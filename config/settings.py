import os
from pathlib import Path

# 基础路径
BASE_DIR = Path(__file__).resolve().parent.parent
MAIN_REPO_ROOT = str(BASE_DIR)

# Python执行路径
PYTHON_EXEC_PATH = "python3"

# 日志目录
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
CRON_BACKUP_DIR = LOG_DIR

# CSV核心配置
CSV_ROOT_DIR = os.path.join(BASE_DIR, "data", "csv")
os.makedirs(CSV_ROOT_DIR, exist_ok=True)

# 👇 新增：解决CSV_TARGET_DIR导入错误（映射到CSV_ROOT_DIR，兼容sync_csv_from_remote.py）
CSV_TARGET_DIR = CSV_ROOT_DIR  

# 补充缺失的CSV_SOURCE_DIR
CSV_SOURCE_DIR = os.path.join(CSV_ROOT_DIR, "source")
os.makedirs(CSV_SOURCE_DIR, exist_ok=True)

# 源文件路径
RAW_FILES = {
    "game_info": os.path.join(CSV_ROOT_DIR, "game_info_raw.csv"),
    "song_info": os.path.join(CSV_ROOT_DIR, "song_info_raw.csv")
}

# 目标文件路径
TARGET_FILES = {
    "game_info": os.path.join(CSV_ROOT_DIR, "game_info.csv"),
    "song_info": os.path.join(CSV_ROOT_DIR, "song_info.csv"),
    "author_info": os.path.join(CSV_ROOT_DIR, "author_info.csv"),
    "game_song_rel": os.path.join(CSV_ROOT_DIR, "game_song_rel.csv"),
    "song_author_rel": os.path.join(CSV_ROOT_DIR, "song_author_rel.csv"),
    "game_linkage_rel": os.path.join(CSV_ROOT_DIR, "game_linkage_rel.csv")
}

# 归档目录
ARCHIVE_DIR = os.path.join(CSV_ROOT_DIR, "archive")
os.makedirs(ARCHIVE_DIR, exist_ok=True)

# 状态文件
STATE_FILE_PATH = os.path.join(CSV_ROOT_DIR, "csv_processed_state.json")

# 清理配置
CLEAN_CONFIG = {
    "expire_days": 7,
    "exclude_suffix": ["_raw.csv"],
    "archive_old_files": True
}

# 数据库配置（环境变量读取）
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 3306)) if os.getenv("DB_PORT", "").isdigit() else 3306,
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "rhythmgame"),
    "charset": "utf8mb4"
}

# Cron配置（每天2点执行auto）
CRON_TASK_MARK = "# 节奏游戏项目定时任务"
CRON_TASKS = [
    f"0 2 * * * {PYTHON_EXEC_PATH} {os.path.join(BASE_DIR, 'manage.py')} auto > {os.path.join(LOG_DIR, 'auto_cron.log')} 2>&1"
]

# 脚本路径（动态生成）
SYNC_SCRIPT = os.path.join(BASE_DIR, "scripts", "sync_csv_from_remote.py")
EXTRACT_SONG_SCRIPT = os.path.join(BASE_DIR, "scripts", "extract_song_data.py")
CRON_MANAGE_SCRIPT = os.path.join(BASE_DIR, "managers", "cron_manage.py")
CSV_MANAGE_SCRIPT = os.path.join(BASE_DIR, "managers", "csv_manage.py")