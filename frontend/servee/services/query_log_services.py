from datetime import datetime
import sys
from pathlib import Path

# 1. 导入 Config
CURRENT_FILE = Path(__file__).resolve()
if str(CURRENT_FILE.parents[1]) not in sys.path:
    sys.path.insert(0, str(CURRENT_FILE.parents[1]))

from servee.config import Config

# 2. 使用 Config 中的路径，不再自己计算
LOG_FILE = Config.USER_QUERIES_LOG_PATH

def log_query_simple(query_text):
    """
    简单记录：只把问题追加写入文件，一行一个
    """
    if not query_text or query_text == 'ERROR_PARSING':
        return
    
    line = f"{query_text}\n"
    
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line)