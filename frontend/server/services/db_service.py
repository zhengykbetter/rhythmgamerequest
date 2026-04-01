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
def get_dashboard_stats():
    engine = get_mysql_engine()
    
    # 【调试】先检查配置到底加载了没
    if not engine:
        # 如果这里返回，说明 MYSQL_CONFIG 是空的，没导入成功
        return {'info_count': 99999, 'song_count': 99999, 'artist_count': 99999}

    try:
        with engine.connect() as conn:
            # ... (你的 SQL 代码保持不变) ...
            # 为了测试，可以先执行一句最简单的 SQL
            result = conn.execute(text("SELECT 1"))
            print("数据库连接成功！")
            
            # ... 原本的 COUNT 查询 ...
            
    except Exception as e:
        # 【调试】把错误信息塞进返回值里，这样网页上就能看到
        error_msg = str(e)
        print(f"[DB Error] {error_msg}")
        return {
            'info_count': 0, 
            'song_count': 0, 
            # 把错误信息放在这里，网页上曲师数的位置会显示错误
            'artist_count': f"ERR: {error_msg[:20]}" 
        }