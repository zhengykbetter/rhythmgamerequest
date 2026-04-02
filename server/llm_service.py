#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
✅ LLM自然语言转SQL（适配新版查询接口）
功能：仅生成合规SQL + 安全校验，不执行查询
供 api.py 调用
"""
import os
import sys
import re
from datetime import datetime

# 项目根目录
MAIN_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, MAIN_PROJECT_ROOT)

from dotenv import load_dotenv
from openai import OpenAI

# 工具导入
from server.encoding_utils import safe_input, safe_print
from config.table_schemas import TABLE_RULES

# 加载配置
load_dotenv(os.path.join(MAIN_PROJECT_ROOT, ".env"))

# LLM配置
LLM_CONFIG = {
    "api_key": os.getenv("LLM_API_KEY", ""),
    "base_url": os.getenv("LLM_BASE_URL", ""),
    "model": os.getenv("LLM_MODEL", "deepseek-chat"),
    "temperature": 0.1,
}

client = OpenAI(api_key=LLM_CONFIG["api_key"], base_url=LLM_CONFIG["base_url"])

# ===================== 表结构提示词 =====================
def get_table_schema_prompt():
    table_names = [t for t in TABLE_RULES["create_order"]]
    schema_prompt = "数据库表结构如下（仅允许查询这些表）：\n"
    for table in table_names:
        rule = TABLE_RULES[table]
        fields = list(rule["field_types"].keys())
        schema_prompt += f"表名：{table} | 字段：{', '.join(fields)}\n"
    schema_prompt += "\n规则：\n1. 仅生成SELECT查询语句，禁止任何增删改\n2. 仅使用上述表和字段\n3. 返回纯SQL，不要任何解释\n4. 字段名必须严格匹配"
    return schema_prompt

# ===================== 安全拦截（全词匹配+详细日志） =====================
def validate_sql_safety(sql: str) -> bool:
    if not sql:
        return False

    sql_upper = sql.strip().upper()
    allowed_tables = [t.upper() for t in TABLE_RULES["create_order"]]
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if not sql_upper.startswith("SELECT"):
        safe_print(f"\n[🚨 拦截日志 {now}]")
        safe_print(f"[拦截原因] 非查询语句，仅支持SELECT查询")
        safe_print(f"[违规SQL] {sql}")
        return False

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

    for table in allowed_tables:
        if re.search(r"\bFROM\s+"+table+r"\b", sql_upper) or re.search(r"\bJOIN\s+"+table+r"\b", sql_upper):
            return True
    safe_print(f"\n[🚨 拦截日志 {now}]")
    safe_print(f"[拦截原因] 使用了未授权的数据表")
    safe_print(f"[违规SQL] {sql}")
    return False

# ===================== 核心：自然语言转SQL（函数名适配 api.py） =====================
def generate_sql(natural_query: str) -> str:
    """
    供 api.py 调用的核心函数
    返回：合规SQL / 空字符串
    """
    if not LLM_CONFIG["api_key"]:
        safe_print("[❌ 错误] 请配置.env中的LLM_API_KEY")
        return ""
        
    try:
        # 调用LLM生成SQL
        resp = client.chat.completions.create(
            model=LLM_CONFIG["model"],
            temperature=LLM_CONFIG["temperature"],
            messages=[
                {"role": "system", "content": get_table_schema_prompt()},
                {"role": "user", "content": natural_query}
            ]
        )
        # 清洗SQL格式（去除markdown标签）
        sql = resp.choices[0].message.content.strip()
        sql = re.sub(r"```sql|```", "", sql).strip()
        
        # 安全校验
        if validate_sql_safety(sql):
            return sql
        return ""
        
    except Exception as e:
        safe_print(f"[❌ LLM调用失败：{str(e)}]")
        return ""

# ===================== 保留本地测试入口（无改动） =====================
if __name__ == "__main__":
    safe_print("=" * 60)
    safe_print("🎯 LLM自然语言查询数据库（输入 exit 退出）")
    safe_print("=" * 60)
    
    while True:
        user_q = safe_input("\n请输入你的查询：")
        if user_q.lower() in ["exit", "quit", "退出"]:
            safe_print("👋 退出成功！")
            break
        if not user_q:
            safe_print("⚠️ 请输入有效内容")
            continue
        # 本地测试仅生成并打印SQL
        test_sql = generate_sql(user_q)
        if test_sql:
            safe_print(f"[✅ 生成SQL] {test_sql}")
        else:
            safe_print("[ℹ️] 未生成有效SQL")