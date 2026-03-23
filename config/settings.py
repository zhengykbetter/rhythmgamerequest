import os
from pathlib import Path

# ===================== 基础核心配置 =====================
BASE_DIR = Path(__file__).resolve().parent.parent  # 项目根目录
MAIN_REPO_ROOT = str(BASE_DIR)
PYTHON_EXEC_PATH = "python3"  # Python执行路径

# ===================== 日志配置 =====================
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
CRON_BACKUP_DIR = LOG_DIR  # Cron备份目录

# ===================== CSV 全量配置（适配你的场景，无冗余） =====================
# 1. 核心本地目录（CSV存储/归档）
CSV_ROOT_DIR = os.path.join(BASE_DIR, "data", "csv")          # 本地CSV根目录
CSV_TARGET_DIR = CSV_ROOT_DIR                                # 兼容子脚本的目标目录变量
CSV_SOURCE_DIR = os.path.join(CSV_ROOT_DIR, "source")        # 临时下载目录（备用）
ARCHIVE_DIR = os.path.join(CSV_ROOT_DIR, "archive")          # 旧文件归档目录
# 自动创建所有本地目录（避免运行时报错）
os.makedirs(CSV_ROOT_DIR, exist_ok=True)
os.makedirs(CSV_SOURCE_DIR, exist_ok=True)
os.makedirs(ARCHIVE_DIR, exist_ok=True)

# 2. 远程Git仓库配置（你的仓库+指定data_csv路径）
CSV_REPO_URL = "https://github.com/zhengykbetter/rhythmgamebase.git"  # 你的仓库地址
CSV_REPO_BRANCH = "main"                                             # 仓库分支
CSV_REPO_LOCAL_PATH = os.path.join(BASE_DIR, "data", "csv-repo")     # 本地克隆仓库的路径
PRIVATE_CSV_REPO_ROOT = "data_csv"                                   # 仓库内CSV文件的存放路径（你要求的/data_csv）
# 自动创建仓库本地存储目录
os.makedirs(CSV_REPO_LOCAL_PATH, exist_ok=True)

# 3. 需要同步的CSV文件列表（和你仓库data_csv下的文件对应）
REQUIRED_CSV_FILES = [
    "game_info_raw.csv",
    "song_info_raw.csv",
    "author_info_raw.csv",
    "game_song_rel_raw.csv",
    "song_author_rel_raw.csv",
    "game_linkage_rel_raw.csv"
]

# 4. 源文件路径（原始CSV，本地存储路径）
RAW_FILES = {
    "game_info": os.path.join(CSV_ROOT_DIR, "game_info_raw.csv"),
    "song_info": os.path.join(CSV_ROOT_DIR, "song_info_raw.csv"),
    "author_info": os.path.join(CSV_ROOT_DIR, "author_info_raw.csv"),
    "game_song_rel": os.path.join(CSV_ROOT_DIR, "game_song_rel_raw.csv"),
    "song_author_rel": os.path.join(CSV_ROOT_DIR, "song_author_rel_raw.csv"),
    "game_linkage_rel": os.path.join(CSV_ROOT_DIR, "game_linkage_rel_raw.csv")
}

# 5. 目标文件路径（处理后的CSV，本地存储路径）
TARGET_FILES = {
    "game_info": os.path.join(CSV_ROOT_DIR, "game_info.csv"),
    "song_info": os.path.join(CSV_ROOT_DIR, "song_info.csv"),
    "author_info": os.path.join(CSV_ROOT_DIR, "author_info.csv"),
    "game_song_rel": os.path.join(CSV_ROOT_DIR, "game_song_rel.csv"),
    "song_author_rel": os.path.join(CSV_ROOT_DIR, "song_author_rel.csv"),
    "game_linkage_rel": os.path.join(CSV_ROOT_DIR, "game_linkage_rel.csv")
}

# 6. 旧文件清理配置
CLEAN_CONFIG = {
    "expire_days": 7,
    "exclude_suffix": ["_raw.csv"],  # 保留原始CSV文件不删除
    "archive_old_files": True        # 清理前先归档
}

# 7. 状态文件（记录CSV处理进度/状态）
STATE_FILE_PATH = os.path.join(CSV_ROOT_DIR, "csv_processed_state.json")

# ===================== 数据库配置 =====================
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),          # 数据库地址（优先读环境变量）
    "port": int(os.getenv("DB_PORT", 3306)) if os.getenv("DB_PORT", "").isdigit() else 3306,  # 端口
    "user": os.getenv("DB_USER", "root"),               # 用户名
    "password": os.getenv("DB_PASSWORD", ""),           # 密码（优先读环境变量，避免硬编码）
    "database": os.getenv("DB_NAME", "rhythmgame"),     # 数据库名
    "charset": "utf8mb4",                               # 字符集
    "connect_timeout": 10                               # 连接超时时间（10秒）
}

# ===================== Cron定时任务配置 =====================
CRON_TASK_MARK = "# 节奏游戏项目定时任务"  # Cron任务标识（便于区分其他任务）
CRON_TASKS = [
    # 每天凌晨2点执行全自动同步流程
    f"0 2 * * * {PYTHON_EXEC_PATH} {os.path.join(BASE_DIR, 'manage.py')} auto > {os.path.join(LOG_DIR, 'auto_cron.log')} 2>&1"
]

# ===================== 脚本路径配置（所有子脚本的绝对路径） =====================
SYNC_SCRIPT = os.path.join(BASE_DIR, "scripts", "sync_csv_from_remote.py")       # 远程CSV同步脚本
EXTRACT_SONG_SCRIPT = os.path.join(BASE_DIR, "scripts", "extract_song_data.py")  # CSV解析脚本
CRON_MANAGE_SCRIPT = os.path.join(BASE_DIR, "managers", "cron_manage.py")        # Cron管理脚本
CSV_MANAGE_SCRIPT = os.path.join(BASE_DIR, "managers", "csv_manage.py")          # CSV管理脚本