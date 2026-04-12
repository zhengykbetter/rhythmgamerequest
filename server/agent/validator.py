import re
from datetime import datetime
from server.encoding_utils import safe_print
from agent.config import TABLE_RULES

def validate_sql_safety(sql: str) -> bool:
    """
    完整保留原始SQL安全校验逻辑
    校验：仅SELECT、无危险关键词、仅使用授权表
    """
    if not sql:
        return False

    sql_upper = sql.strip().upper()
    allowed_tables = [t.upper() for t in TABLE_RULES["create_order"]]
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 仅允许SELECT查询
    if not sql_upper.startswith("SELECT"):
        safe_print(f"\n[🚨 拦截日志 {now}]")
        safe_print(f"[拦截原因] 非查询语句，仅支持SELECT查询")
        safe_print(f"[违规SQL] {sql}")
        return False

    # 危险SQL关键词拦截
    dangerous_keywords = [
        r"\bDROP\b", r"\bDELETE\b", r"\bUPDATE\b", r"\bINSERT\b",
        r"\bALTER\b", r"\bTRUNCATE\b", r"\bCREATE\b", r"\bGRANT\b"
    ]
    for pattern in dangerous_keywords:
        if re.search(pattern, sql_upper):
            keyword = re.search(pattern, sql_upper).group()
            safe_print(f"\n[🚨 拦截日志 {now}]")
            safe_print(f"[拦截原因] 检测到危险SQL操作：{keyword}")
            safe_print(f"[违规SQL] {sql}")
            safe_print(f"[处理结果] 已拦截，未执行任何数据库操作")
            return False

    # 校验使用的表是否在白名单中
    for table in allowed_tables:
        if re.search(r"\bFROM\s+"+table+r"\b", sql_upper) or re.search(r"\bJOIN\s+"+table+r"\b", sql_upper):
            return True
    
    safe_print(f"\n[🚨 拦截日志 {now}]")
    safe_print(f"[拦截原因] 使用了未授权的数据表")
    safe_print(f"[违规SQL] {sql}")
    return False