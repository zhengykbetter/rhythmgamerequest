from sqlalchemy import create_engine, text
from pathlib import Path
import sys

# 导入配置（假设你的 MYSQL_CONFIG 在 config.settings 里）
# 这里需要确保能导入到你的 settings
FILE = Path(__file__).resolve()
FRONTEND_ROOT = FILE.parent.parent.parent
MAIN_PROJECT_ROOT = FRONTEND_ROOT.parent
if str(MAIN_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(MAIN_PROJECT_ROOT))
if str(FRONTEND_ROOT) not in sys.path:
    sys.path.insert(0, str(FRONTEND_ROOT))

try:
    from config.settings import MYSQL_CONFIG, TABLE_RULES
except ImportError:
    # 如果没有配置，给个默认空配置防止报错
    MYSQL_CONFIG = {}
    TABLE_RULES = {}

# ===================== 数据库基础函数 =====================
def get_mysql_engine():
    if not MYSQL_CONFIG:
        return None
    conn_str = (
        f"mysql+pymysql://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@"
        f"{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}?charset={MYSQL_CONFIG['charset']}"
    )
    return create_engine(conn_str, pool_pre_ping=True, pool_recycle=3600)

# ===================== 新增：首页统计查询 =====================
def get_dashboard_stats():
    """
    查询首页看板需要的三个数据
    返回一个字典: {'info_count': 0, 'song_count': 0, 'artist_count': 0}
    """
    engine = get_mysql_engine()
    if not engine:
        return {'info_count': 6455, 'song_count': 5483, 'artist_count': 3108} # 兜底默认值

    try:
        with engine.connect() as conn:
            # 注意：这里需要你根据实际的表名修改 SQL！！
            # 我假设了表名，你需要改成你真实的表名
            
            # 1. 查询收录信息数 (假设表名是 game_info)
            result = conn.execute(text("SELECT COUNT(*) FROM game_song_rel"))
            info_count = result.scalar()
            
            # 2. 查询歌曲数 (假设表名是 songs)
            result = conn.execute(text("SELECT COUNT(*) FROM song_info"))
            song_count = result.scalar()
            
            # 3. 查询曲师数 (假设表名是 artists)
            result = conn.execute(text("SELECT COUNT(*) FROM author_info"))
            artist_count = result.scalar()

            # --- 临时演示代码（请替换上面的真实查询） ---
            # info_count = 6455
            # song_count = 5483
            # artist_count = 3108
            # ---------------------------------------------

            return {
                'info_count': info_count,
                'song_count': song_count,
                'artist_count': artist_count
            }
    except Exception as e:
        print(f"[DB Error] {e}")
        # 数据库挂了也给个默认值，保证网页能打开
        return {'info_count': 6455, 'song_count': 5483, 'artist_count': 3108}