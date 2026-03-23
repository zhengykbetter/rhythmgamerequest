import os
from pathlib import Path

# ===================== 基础核心配置 =====================
BASE_DIR = Path(__file__).resolve().parent.parent  # 项目根目录（Path对象）
MAIN_REPO_ROOT = str(BASE_DIR)
PYTHON_EXEC_PATH = "python3"  # Python执行路径

# ===================== 日志配置 =====================
LOG_DIR = Path(BASE_DIR) / "logs"  # Path对象（兼容脚本/拼接）
LOG_FILE_PREFIX = "sync_csv_"      # 日志文件前缀
LOG_FILE_SUFFIX = ".log"           # 日志文件后缀
LOG_DATE_FORMAT = "%Y%m%d"         # 日志文件名中的日期格式
LOG_ENCODING = "utf-8"             # 日志文件编码
os.makedirs(LOG_DIR, exist_ok=True)# 自动创建日志目录
CRON_BACKUP_DIR = str(LOG_DIR)     # 兼容字符串路径的逻辑

# ===================== CSV 全量配置（2026-03-23 增量更新：匹配实际目录+GitHub逻辑） =====================
# 1. 核心目录（区分「原始同步文件」和「提取生成文件」）
CSV_ROOT_DIR = Path(BASE_DIR) / "data" / "csv"          # 项目的data_csv目录（目标目录）
CSV_ROOT_DIR_STR = str(CSV_ROOT_DIR)                    # 兼容字符串版本
CSV_SOURCE_DIR = CSV_ROOT_DIR / "source"                # 同步后的原始文件存放目录
CSV_SOURCE_DIR_STR = str(CSV_SOURCE_DIR)                # 兼容字符串版本
CSV_TARGET_DIR = CSV_ROOT_DIR                            # 提取生成文件的目标目录
CSV_TARGET_DIR_STR = str(CSV_TARGET_DIR)                # 兼容字符串版本
ARCHIVE_DIR = CSV_ROOT_DIR / "archive"                  # 旧文件归档目录
ARCHIVE_DIR_STR = str(ARCHIVE_DIR)                      # 兼容字符串版本
# 自动创建所有目录
os.makedirs(CSV_ROOT_DIR, exist_ok=True)
os.makedirs(CSV_SOURCE_DIR, exist_ok=True)
os.makedirs(ARCHIVE_DIR, exist_ok=True)

# 2. 【关键修正】GitHub仓库抓取配置（匹配你的实际目录）
CSV_REPO_URL = "https://github.com/zhengykbetter/rhythmgamebase.git"  # 你的仓库地址
CSV_REPO_BRANCH = "main"                                             # 仓库分支
CSV_REPO_LOCAL_PATH = Path("/opt/csv_repo")                          # 你指定的仓库本地抓取路径
CSV_REPO_LOCAL_PATH_STR = str(CSV_REPO_LOCAL_PATH)                   # 兼容字符串版本
PRIVATE_CSV_REPO_ROOT = "result"                                     # CSV在仓库里的子目录（/opt/csv_repo/result）
# 自动创建/opt/csv_repo目录（需确保当前用户有/opt目录的读写权限）
os.makedirs(CSV_REPO_LOCAL_PATH, exist_ok=True)

# 3. 【关键修正】匹配/opt/csv_repo/result下的实际文件
# 备注：song_info.csv是老版本，songraw_info.csv待仓库更新后替换
REQUIRED_CSV_FILES = [
    "game_info.csv",
    "song_info.csv"
]

# 4. 原始同步文件路径（指向/opt/csv_repo/result同步来的文件）
RAW_SOURCE_FILES = {
    "game_info": os.path.join(CSV_SOURCE_DIR_STR, "game_info.csv"),
    "song_info": os.path.join(CSV_SOURCE_DIR_STR, "song_info.csv")  # 老版本，待替换为songraw_info.csv
}

# 5. extract_song_data.py生成的目标文件列表（供数据库导入）
EXTRACT_GENERATED_FILES = {
    "author_info": os.path.join(CSV_ROOT_DIR_STR, "author_info.csv"),
    "game_song_rel": os.path.join(CSV_ROOT_DIR_STR, "game_song_rel.csv"),
    "song_author_rel": os.path.join(CSV_ROOT_DIR_STR, "song_author_rel.csv"),
    "game_linkage_rel": os.path.join(CSV_ROOT_DIR_STR, "game_linkage_rel.csv"),
    # 保留原始同步文件的映射（供extract读取）
    "game_info": os.path.join(CSV_ROOT_DIR_STR, "game_info.csv"),
    "song_info": os.path.join(CSV_ROOT_DIR_STR, "song_info.csv")  # 老版本，待替换
}

# 6. 数据库导入用的目标文件（指向extract生成的文件）
TARGET_FILES = EXTRACT_GENERATED_FILES  # 统一指向生成的文件

# 7. 旧文件清理配置
CLEAN_CONFIG = {
    "expire_days": 7,
    "exclude_suffix": [".csv"],  # 保留所有CSV文件（原始+生成）
    "archive_old_files": True
}

# 8. 状态文件（记录同步/提取进度）
STATE_FILE_PATH = os.path.join(CSV_ROOT_DIR_STR, "csv_processed_state.json")

# ===================== 数据库配置 =====================
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 3306)) if os.getenv("DB_PORT", "").isdigit() else 3306,
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "rhythmgame"),
    "charset": "utf8mb4",
    "connect_timeout": 10
}

# ===================== Cron定时任务配置 =====================
CRON_TASK_MARK = "# 节奏游戏项目定时任务"
CRON_TASKS = [
    # 全自动流程：同步→提取→导入数据库
    f"0 2 * * * {PYTHON_EXEC_PATH} {os.path.join(BASE_DIR, 'manage.py')} auto > {os.path.join(LOG_DIR, 'auto_cron.log')} 2>&1"
]

# ===================== 脚本路径配置 =====================
SYNC_SCRIPT = os.path.join(BASE_DIR, "scripts", "sync_csv_from_remote.py")       # 同步原始文件
EXTRACT_SONG_SCRIPT = os.path.join(BASE_DIR, "scripts", "extract_song_data.py")  # 生成目标文件
CRON_MANAGE_SCRIPT = os.path.join(BASE_DIR, "managers", "cron_manage.py")        # Cron管理
CSV_MANAGE_SCRIPT = os.path.join(BASE_DIR, "managers", "csv_manage.py")          # 数据库导入

# ===================== 兜底配置 =====================
SYNC_TIMEOUT = 60  # 同步超时
SYNC_RETRY_TIMES = 3  # 同步重试
EXTRACT_TIMEOUT = 120  # extract解析超时（2分钟）
# 新增：Git拉取配置（确保仓库权限）
GIT_PULL_RETRY = 2  # Git拉取失败重试次数
GIT_SSH_KEY_PATH = os.getenv("GIT_SSH_KEY_PATH", "")  # 私有仓库时配置SSH密钥路径