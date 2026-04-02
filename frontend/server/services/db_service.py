from sqlalchemy import create_engine, text
from pathlib import Path
from dotenv import load_dotenv
import sys
import os

# ===================== 路径与环境配置 =====================
FILE = Path(__file__).resolve()
MAIN_PROJECT_ROOT = FILE.parents[3] 

load_dotenv(MAIN_PROJECT_ROOT / ".env")

if str(MAIN_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(MAIN_PROJECT_ROOT))

try:
    from config.settings import DB_CONFIG
except ImportError as e:
    print(f"[DEBUG] 导入失败: {e}")
    DB_CONFIG = {}

# ===================== 数据库基础函数 =====================
def get_mysql_engine():
    if not DB_CONFIG:
        return None
    
    conn_str = (
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['db']}?charset={DB_CONFIG['charset']}"
    )
    return create_engine(conn_str, pool_pre_ping=True, pool_recycle=3600)

# ===================== 首页统计查询（原有代码，完全不动） =====================
def get_dashboard_stats():
    engine = get_mysql_engine()
    if not engine:
        return {'info_count': 6455, 'song_count': 5483, 'artist_count': 3108}

    try:
        with engine.connect() as conn:
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
        return {'info_count': 11, 'song_count': 45, 'artist_count': 14}
# ===================== 往年今日 - 核心数据库查询（修复版·英文字段） =====================
def get_year_today_songs():
    engine = get_mysql_engine()
    if not engine:
        return []

    try:
        with engine.connect() as conn:
            # 🔥 唯一修改：全部用英文字段名，彻底解决undefined问题
            sql = text("""
                SELECT
                  YEAR(g.收录时间) AS year,
                  g.游戏编号 AS game,
                  CASE WHEN g.本家 = g.游戏编号 THEN 1 ELSE 0 END AS is_original,
                  s.作者 AS author,
                  s.歌名 AS song
                FROM game_song_rel g
                JOIN song_info s ON g.song_id = s.song_id
                WHERE MONTH(g.收录时间) = MONTH(CURDATE())
                  AND DAY(g.收录时间) = DAY(CURDATE())
                ORDER BY is_original DESC, g.收录时间 DESC
            """)
            result = conn.execute(sql)
            data_list = [dict(row) for row in result]
            return data_list

    except Exception as e:
        print(f"[往年今日 DB错误] {e}")
        return []