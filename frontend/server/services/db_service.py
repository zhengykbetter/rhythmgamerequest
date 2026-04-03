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

# ===================== 数据库连接 =====================
def get_mysql_engine():
    if not DB_CONFIG:
        return None
    
    conn_str = (
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['db']}?charset={DB_CONFIG['charset']}"
    )
    return create_engine(conn_str, pool_pre_ping=True, pool_recycle=3600)

# ===================== 首页统计查询 =====================
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

# ===================== 🔥 往年今日【DEBUG版】 =====================
def get_year_today_songs():
    engine = get_mysql_engine()
    if not engine:
        print("[DB调试] 数据库引擎获取失败")
        return []

    try:
        with engine.connect() as conn:
            # 1. 先查一下数据库现在认为是几月几号
            time_check_sql = text("SELECT CURDATE() as db_date, NOW() as db_now")
            time_res = conn.execute(time_check_sql).mappings().first()
            print(f"[DB调试] 数据库当前日期: {time_res}")

            # 2. 执行主查询
            sql = text("""
                SELECT song_id, 游戏编号, 收录时间, 本家
                FROM game_song_rel
                WHERE MONTH(收录时间) = MONTH(CURDATE())
                  AND DAY(收录时间) = DAY(CURDATE())
                ORDER BY 本家 = 游戏编号 DESC
            """)
            print(f"[DB调试] 执行SQL: {sql}")
            
            result = conn.execute(sql)
            song_list = [dict(row) for row in result.mappings()]
            
            print(f"[DB调试] 主查询结果数量: {len(song_list)}")
            print(f"[DB调试] 主查询原始数据: {song_list}")

            # 3. Python循环拼接
            final_data = []
            for item in song_list:
                song_id = item["song_id"]
                print(f"[DB调试] 正在查 song_id: {song_id}")
                
                song_sql = text("SELECT 歌名, 作者 FROM song_info WHERE song_id = :sid")
                song_res = conn.execute(song_sql, {"sid": song_id}).mappings().first()
                
                print(f"[DB调试] song_info查询结果: {song_res}")

                if song_res:
                    final_data.append({
                        "year": item["收录时间"].year,
                        "game": item["游戏编号"],
                        "is_original": 1 if item["本家"] == item["游戏编号"] else 0,
                        "author": song_res["作者"],
                        "song": song_res["歌名"]
                    })

            print(f"[DB调试] 最终返回数据: {final_data}")
            return final_data

    except Exception as e:
        print(f"[DB错误] 捕获异常: {e}")
        import traceback
        traceback.print_exc()
        return []