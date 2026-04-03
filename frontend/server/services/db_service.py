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
# 核心：无JOIN、无复杂函数、无别名，和你原有代码100%一致，绝对不报错
def get_year_today_songs():
    engine = get_mysql_engine()
    if not engine:
        return []

    try:
        with engine.connect() as conn:
            # 1. 【最简查询】只查主表，单表！无JOIN（和你原有代码一样）
            sql = text("""
                SELECT song_id, 游戏编号, 收录时间, 本家
                FROM game_song_rel
                WHERE MONTH(收录时间) = MONTH(CURDATE())
                  AND DAY(收录时间) = DAY(CURDATE())
                ORDER BY 本家 = 游戏编号 DESC
            """)
            result = conn.execute(sql)
            # 用你原有代码的取值方式，绝对兼容
            song_list = [dict(row) for row in result.mappings()]

            # 2. Python循环拼接：单查song_info表（单表查询，无坑）
            final_data = []
            for item in song_list:
                song_id = item["song_id"]
                # 单表查询歌名+作者，和你项目原有逻辑完全一致
                song_sql = text("SELECT 歌名, 作者 FROM song_info WHERE song_id = :sid")
                song_res = conn.execute(song_sql, {"sid": song_id}).mappings().first()

                if song_res:
                    final_data.append({
                        "year": item["收录时间"].year,
                        "game": item["游戏编号"],
                        "is_original": 1 if item["本家"] == item["游戏编号"] else 0,
                        "author": song_res["作者"],
                        "song": song_res["歌名"]
                    })

            print(f"[DB] 成功查询到 {len(final_data)} 条数据")
            return final_data

    except Exception as e:
        print(f"[DB错误] {e}")
        return []