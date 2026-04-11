from sqlalchemy import create_engine, text
from pathlib import Path
from dotenv import load_dotenv
import sys
import os

# ===================== 路径与环境配置（原样保留） =====================
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

# ===================== 数据库连接【恢复原版！解决500报错】 =====================
# 完全用你之前正常运行的配置，不添加任何多余参数
def get_mysql_engine():
    if not DB_CONFIG:
        return None
    
    conn_str = (
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['db']}?charset={DB_CONFIG['charset']}"
    )
    return create_engine(conn_str, pool_pre_ping=True, pool_recycle=3600)

# ===================== 首页统计查询（完全不动，正常运行） =====================
def get_dashboard_stats():
    engine = get_mysql_engine()
    if not engine:
        return {'info_count': 191, 'song_count': 98, 'artist_count': 10}

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
        return {'info_count': 191, 'song_count': 98, 'artist_count': 10}

# ===================== 🔥 往年今日【终极极简版】单表查询 + Python拼接 =====================
# 核心：无JOIN、无复杂函数、无别名，适配新表结构 | 本家 ↔ 游戏名 匹配
def get_year_today_songs():
    engine = get_mysql_engine()
    if not engine:
        return []

    try:
        with engine.connect() as conn:
            # 1. 【最简查询】读取游戏名字段，排序规则改为 本家=游戏名
            sql = text("""
                SELECT song_id, 游戏编号, 游戏名, 收录时间, 本家
                FROM game_song_rel
                WHERE MONTH(收录时间) = MONTH(CURDATE())
                  AND DAY(收录时间) = DAY(CURDATE())
                ORDER BY 本家 = 游戏名 DESC
            """)
            result = conn.execute(sql)
            song_list = [dict(row) for row in result.mappings()]

            # 2. Python循环拼接：单查song_info表
            final_data = []
            for item in song_list:
                song_id = item["song_id"]
                song_sql = text("SELECT 歌名, 作者 FROM song_info WHERE song_id = :sid")
                song_res = conn.execute(song_sql, {"sid": song_id}).mappings().first()

                if song_res:
                    final_data.append({
                        "year": item["收录时间"].year,
                        "game": item["游戏名"],  # 直接返回友好的游戏名称
                        # 🔥 核心修正：原创判定改为 本家 == 游戏名
                        "is_original": 1 if item["本家"] == item["游戏名"] else 0,
                        "author": song_res["作者"],
                        "song": song_res["歌名"]
                    })

            print(f"[DB] 成功查询到 {len(final_data)} 条数据")
            return final_data

    except Exception as e:
        print(f"[DB错误] {e}")
        return []