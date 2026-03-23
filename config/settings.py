# /opt/main_project/config/settings.py 中补充以下配置（加到合适位置）
import os
from pathlib import Path

# 基础路径（已有则无需重复）
BASE_DIR = Path(__file__).resolve().parent.parent
MAIN_REPO_ROOT = str(BASE_DIR)

# ========== 补充manage.py需要的所有缺失变量 ==========
# Python执行路径（已有则无需重复）
PYTHON_EXEC_PATH = "python3"

# 日志目录（已有则无需重复）
LOG_DIR = os.path.join(BASE_DIR, "logs")
CRON_BACKUP_DIR = LOG_DIR
os.makedirs(LOG_DIR, exist_ok=True)

# 脚本路径（核心：补充SYNC_SCRIPT等缺失变量）
SYNC_SCRIPT = os.path.join(BASE_DIR, "scripts", "sync_csv_from_remote.py")          # 缺失的SYNC_SCRIPT
EXTRACT_SONG_SCRIPT = os.path.join(BASE_DIR, "scripts", "extract_song_data.py")    # 若导入EXTRACT_SONG_SCRIPT也报错则补充
CRON_MANAGE_SCRIPT = os.path.join(BASE_DIR, "managers", "cron_manage.py")          # 若导入CRON_MANAGE_SCRIPT也报错则补充
CSV_MANAGE_SCRIPT = os.path.join(BASE_DIR, "managers", "csv_manage.py")            # 若导入CSV_MANAGE_SCRIPT也报错则补充

# Cron配置（已有则无需重复）
CRON_TASK_MARK = "# 节奏游戏项目定时任务"
CRON_TASKS = [
    f"0 2 * * * {PYTHON_EXEC_PATH} {os.path.join(BASE_DIR, 'manage.py')} auto > {os.path.join(LOG_DIR, 'auto_cron.log')} 2>&1"
]

# CSV/DB配置（已有则无需重复）
CSV_ROOT_DIR = os.path.join(BASE_DIR, "data", "csv")
ARCHIVE_DIR = os.path.join(CSV_ROOT_DIR, "archive")
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "123456"),
    "database": os.getenv("DB_NAME", "rhythmgame"),
    "charset": "utf8mb4"
}

# 其余原有配置（RAW_FILES/TARGET_FILES等）保持不变