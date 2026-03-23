import os
from pathlib import Path

# ===================== 基础路径配置（核心） =====================
BASE_DIR = Path(__file__).resolve().parent.parent  # 项目根目录
MAIN_REPO_ROOT = str(BASE_DIR)
PYTHON_EXEC_PATH = "python3"  # Python执行路径

# ===================== 日志配置 =====================
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
CRON_BACKUP_DIR = LOG_DIR  # Cron备份目录

# ===================== CSV全量配置（包含所有子脚本需要的变量） =====================
# 核心目录
CSV_ROOT_DIR = os.path.join(BASE_DIR, "data", "csv")          # CSV根目录
CSV_TARGET_DIR = CSV_ROOT_DIR                                # 兼容sync_csv_from_remote.py的CSV_TARGET_DIR
CSV_SOURCE_DIR = os.path.join(CSV_ROOT_DIR, "source")        # 远程CSV下载源目录
ARCHIVE_DIR = os.path.join(CSV_ROOT_DIR, "archive")          # 归档目录
os.makedirs(CSV_ROOT_DIR, exist_ok=True)
os.makedirs(CSV_SOURCE_DIR, exist_ok=True)
os.makedirs(ARCHIVE_DIR, exist_ok=True)

# 同步远程CSV需要的文件列表（解决REQUIRED_CSV_FILES缺失）
REQUIRED_CSV_FILES = [
    "game_info_raw.csv",
    "song_info_raw.csv",
    "author_info_raw.csv",
    "game_song_rel_raw.csv",
    "song_author_rel_raw.csv",
    "game_linkage_rel_raw.csv"
]

# 源文件路径（原始CSV）
RAW_FILES = {
    "game_info": os.path.join(CSV_ROOT_DIR, "game_info_raw.csv"),
    "song_info": os.path.join(CSV_ROOT_DIR, "song_info_raw.csv"),
    "author_info": os.path.join(CSV_ROOT_DIR, "author_info_raw.csv"),
    "game_song_rel": os.path.join(CSV_ROOT_DIR, "game_song_rel_raw.csv"),
    "song_author_rel": os.path.join(CSV_ROOT_DIR, "song_author_rel_raw.csv"),
    "game_linkage_rel": os.path.join(CSV_ROOT_DIR, "game_linkage_rel_raw.csv")
}

# 目标文件路径（处理后的CSV）
TARGET_FILES = {
    "game_info": os.path.join(CSV_ROOT_DIR, "game_info.csv"),
    "song_info": os.path.join(CSV_ROOT_DIR, "song_info.csv"),
    "author_info": os.path.join(CSV_ROOT_DIR, "author_info.csv"),
    "game_song_rel": os.path.join(CSV_ROOT_DIR, "game_song_rel.csv"),
    "song_author_rel": os.path.join(CSV_ROOT_DIR, "song_author_rel.csv"),
    "game_linkage_rel": os.path.join(CSV_ROOT_DIR, "game_linkage_rel.csv")
}

# 清理配置
CLEAN_CONFIG = {
    "expire_days": 7,
    "exclude_suffix": ["_raw.csv"],
    "archive_old_files": True
}

# 状态文件（记录CSV处理状态）
STATE_FILE_PATH = os.path.join(CSV_ROOT_DIR, "csv_processed_state.json")

# ===================== 数据库配置 =====================
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 3306)) if os.getenv("DB_PORT", "").isdigit() else 3306,
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "rhythmgame"),
    "charset": "utf8mb4",
    "connect_timeout": 10  # 数据库连接超时
}

# ===================== 远程CSV同步配置（可选，供sync_csv_from_remote.py使用） =====================
REMOTE_CSV_CONFIG = {
    "remote_url_prefix": "https://example.com/csv/",  # 远程CSV前缀（根据实际修改）
    "download_timeout": 30,                           # 下载超时时间
    "verify_ssl": False                                # 是否验证SSL
}

# ===================== Cron定时任务配置 =====================
CRON_TASK_MARK = "# 节奏游戏项目定时任务"
CRON_TASKS = [
    f"0 2 * * * {PYTHON_EXEC_PATH} {os.path.join(BASE_DIR, 'manage.py')} auto > {os.path.join(LOG_DIR, 'auto_cron.log')} 2>&1"
]

# ===================== 脚本路径配置（所有子脚本路径） =====================
SYNC_SCRIPT = os.path.join(BASE_DIR, "scripts", "sync_csv_from_remote.py")
EXTRACT_SONG_SCRIPT = os.path.join(BASE_DIR, "scripts", "extract_song_data.py")
CRON_MANAGE_SCRIPT = os.path.join(BASE_DIR, "managers", "cron_manage.py")
CSV_MANAGE_SCRIPT = os.path.join(BASE_DIR, "managers", "csv_manage.py")