import re
from datetime import datetime
from server.encoding_utils import safe_print
from agent.config import TABLE_RULES

def validate_sql_safety(sql: str) -> bool:
    safe_print(f"      📥 [validator] 待校验 SQL: {sql[:80]}...")
    
    if not sql:
        safe_print(f"      ⚠️  [validator] SQL 为空")
        return False

    sql_upper = sql.strip().upper()
    allowed_tables = [t.upper() for t in TABLE_RULES["create_order"]]
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 仅允许SELECT查询
    if not sql_upper.startswith("SELECT"):
        safe_print(f"      ❌ [validator] 拦截: 非SELECT语句")
        return False

    # 危险SQL关键词拦截
    dangerous_keywords = [
        r"\bDROP\b", r"\bDELETE\b", r"\bUPDATE\b", r"\bINSERT\b",
        r"\bALTER\b", r"\bTRUNCATE\b", r"\bCREATE\b", r"\bGRANT\b"
    ]
    for pattern in dangerous_keywords:
        if re.search(pattern, sql_upper):
            keyword = re.search(pattern, sql_upper).group()
            safe_print(f"      ❌ [validator] 拦截: 危险关键词 {keyword}")
            return False

    # 校验使用的表是否在白名单中
    table_found = False
    for table in allowed_tables:
        if re.search(r"\bFROM\s+"+table+r"\b", sql_upper) or re.search(r"\bJOIN\s+"+table+r"\b", sql_upper):
            table_found = True
            break
    
    if not table_found:
        safe_print(f"      ❌ [validator] 拦截: 未授权表")
        return False
    
    safe_print(f"      ✅ [validator] 校验通过")
    return True