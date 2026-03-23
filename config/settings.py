import os
from pathlib import Path

# ===================== 基础路径配置（核心：动态生成，无硬编码） =====================
# 项目根目录（自动识别，无需手动修改）
BASE_DIR = Path(__file__).resolve().parent.parent
MAIN_REPO_ROOT = str(BASE_DIR)  # 兼容原有MAIN_REPO_ROOT配置

# Python执行路径
PYTHON_EXEC_PATH = "python3"

# 日志目录（基于项目根目录）
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Cron备份目录（复用日志目录）
CRON_BACKUP_DIR = LOG_DIR

# ===================== CSV相关配置 =====================
# CSV文件根目录（所有CSV都放在这里）
CSV_ROOT_DIR = os.path.join(BASE_DIR, "data", "csv")
os.makedirs(CSV_ROOT_DIR, exist_ok=True)

# 源文件（带_raw后缀）路径配置
RAW_FILES = {
    "game_info": os.path.join(CSV_ROOT_DIR, "game_info_raw.csv"),  # game_info原始文件
    "song_info": os.path.join(CSV_ROOT_DIR, "song_info_raw.csv")   # song_info原始文件
}

# 处理后目标文件路径配置（供同步程序使用）
TARGET_FILES = {
    "game_info": os.path.join(CSV_ROOT_DIR, "game_info.csv"),
    "song_info": os.path.join(CSV_ROOT_DIR, "song_info.csv"),
    "author_info": os.path.join(CSV_ROOT_DIR, "author_info.csv"),
    "game_song_rel": os.path.join(CSV_ROOT_DIR, "game_song_rel.csv"),
    "song_author_rel": os.path.join(CSV_ROOT_DIR, "song_author_rel.csv"),
    "game_linkage_rel": os.path.join(CSV_ROOT_DIR, "game_linkage_rel.csv")
}

# 归档目录（旧文件/处理后的备份）
ARCHIVE_DIR = os.path.join(CSV_ROOT_DIR, "archive")
os.makedirs(ARCHIVE_DIR, exist_ok=True)

# 状态文件路径（持久化同步状态）
STATE_FILE_PATH = os.path.join(CSV_ROOT_DIR, "csv_processed_state.json")

# 清理旧文件配置
CLEAN_CONFIG = {
    "expire_days": 7,  # 删除7天前的旧文件
    "exclude_suffix": ["_raw.csv"],  # 保留_raw源文件
    "archive_old_files": True  # 清理前先归档
}

# ===================== 数据库配置 =====================
# MySQL数据库配置（从环境变量读取，避免硬编码）
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "123456"),
    "database": os.getenv("DB_NAME", "rhythmgame"),
    "charset": "utf8mb4"
}

# ===================== 脚本路径配置（基于项目根目录动态生成） =====================
# 数据转换脚本路径
EXTRACT_SONG_SCRIPT = os.path.join(BASE_DIR, "scripts", "extract_song_data.py")

# 管理脚本路径（managers目录）
CRON_MANAGE_SCRIPT = os.path.join(BASE_DIR, "managers", "cron_manage.py")
CSV_MANAGE_SCRIPT = os.path.join(BASE_DIR, "managers", "csv_manage.py")

# ===================== Cron定时任务配置 =====================
# Cron任务特征标记（用于过滤/识别本项目任务）
CRON_TASK_MARK = "# 节奏游戏项目定时任务"

# Cron任务列表（路径自动适配项目根目录，无硬编码）
CRON_TASKS = [
    f"*/30 * * * * {PYTHON_EXEC_PATH} {os.path.join(BASE_DIR, 'manage.py')} auto >> {os.path.join(LOG_DIR, 'auto_cron.log')} 2>&1"
]