from sqlalchemy import create_engine, text
from pathlib import Path
import sys

# ===================== 核心修复：路径调试 =====================
FILE = Path(__file__).resolve()
print(f"[DEBUG] 当前文件位置: {FILE}")

# 向上找 4 层，直接定位到 main_project (main) 根目录
# db_service.py 位置: frontend/server/services/db_service.py (4层)
MAIN_PROJECT_ROOT = FILE.parents[3] 
print(f"[DEBUG] 计算出的根目录: {MAIN_PROJECT_ROOT}")

# 确保根目录在 sys.path 最前面
if str(MAIN_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(MAIN_PROJECT_ROOT))

# 现在尝试导入
try:
    from config.settings import MYSQL_CONFIG, TABLE_RULES
    print(f"[DEBUG] 导入成功！MYSQL_CONFIG host: {MYSQL_CONFIG.get('host', '空')}")
except ImportError as e:
    print(f"[DEBUG] 导入失败: {e}")
    print(f"[DEBUG] 当前 sys.path: {sys.path}")
    MYSQL_CONFIG = {}
    TABLE_RULES = {}
# ===============================================================

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