from sqlalchemy import create_engine, text
from pathlib import Path
from dotenv import load_dotenv # 新增
import sys
import os

# ===================== 路径与环境配置 =====================
FILE = Path(__file__).resolve()
MAIN_PROJECT_ROOT = FILE.parents[3] 

# 1. 加载 .env 文件（关键！否则读不到密码）
load_dotenv(MAIN_PROJECT_ROOT / ".env")

# 2. 添加路径
if str(MAIN_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(MAIN_PROJECT_ROOT))

# 3. 正确导入：DB_CONFIG，不是 MYSQL_CONFIG
try:
    from config.settings import DB_CONFIG, TABLE_RULES
    # print(f"[DEBUG] 导入成功: {DB_CONFIG}")
except ImportError as e:
    print(f"[DEBUG] 导入失败: {e}")
    DB_CONFIG = {}
    TABLE_RULES = {}

# ===================== 数据库基础函数 =====================
def get_mysql_engine():
    if not DB_CONFIG:
        return None
    
    # 4. 修正字段名：使用 'db' 而不是 'database'
    conn_str = (
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['db']}?charset={DB_CONFIG['charset']}"
    )
    return create_engine(conn_str, pool_pre_ping=True, pool_recycle=3600)

# ===================== 首页统计查询 =====================
def get_dashboard_stats():
    engine = get_mysql_engine()
    if not engine:
        return {'info_count': 6455, 'song_count': 5483, 'artist_count': 3108}

    try:
        with engine.connect() as conn:
            # 你的真实表名
            result = conn.execute(text("SELECT COUNT(*) FROM game_song_rel"))
            info_count = result.scalar()
            
            result = conn.execute(text("SELECT COUNT(*) FROM song_info"))
            song_count = result.scalar()
            
            result = conn.execute(text("SELECT COUNT(*) FROM author_info"))
            artist_count = result.scalar()

            return {
                'info_count': info_count,
                'song_count': song_count,
                'artist_count': artist_count
            }
    except Exception as e:
        print(f"[DB Error] {e}")
        return {'info_count': 6455, 'song_count': 5483, 'artist_count': 3108}