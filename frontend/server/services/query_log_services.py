from pathlib import Path
from datetime import datetime

# 定义保存文件的路径
LOG_FILE = Path(__file__).parent.parent.parent / "data_queries" / "user_queries.txt"

# 确保目录存在
LOG_FILE.parent.mkdir(exist_ok=True)

def log_query_simple(query_text):
    """
    简单记录：只把问题追加写入文件，一行一个
    """
    if not query_text or query_text == 'ERROR_PARSING':
        return
        
    # 可以在前面加个时间，也可以不加
    # line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {query_text}\n"
    
    # 纯文本，一行一个
    line = f"{query_text}\n"
    
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line)