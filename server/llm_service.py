#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
✅ LLM自然语言转SQL（最终稳定版）
无IO修改 | 无崩溃 | 全编码兼容 | 交互式手动输入
"""
import os
import sys
import re

# 项目根目录
MAIN_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, MAIN_PROJECT_ROOT)

from dotenv import load_dotenv
from openai import OpenAI

# 导入安全工具
from server.encoding_utils import safe_input, safe_print
# 导入业务配置
from scripts.csv_incremental_update import TABLE_RULES
from server.db_query import query_database

# 加载配置
load_dotenv(os.path.join(MAIN_PROJECT_ROOT, ".env"))

# LLM配置
LLM_CONFIG = {
    "api_key": os.getenv("LLM_API_KEY", ""),
    "base_url": os.getenv("LLM_BASE_URL", ""),
    "model": os.getenv("LLM_MODEL", "deepseek-chat"),
    "temperature": float(os.getenv("LLM_TEMPERATURE", 0.1)),
}

client = OpenAI(api_key=LLM_CONFIG["api_key"], base_url=LLM_CONFIG["base_url"])

# 生成表结构提示词
def get_table_schema_prompt():
    table_names = [t for t in TABLE_RULES["create_order"]]
    schema_prompt = "数据库表结构如下（仅允许查询这些表）：\n"
    for table in table_names:
        rule = TABLE_RULES[table]
        fields = list(rule["field_types"].keys())
        schema_prompt += f"表名：{table} | 字段：{', '.join(fields)}\n"
    schema_prompt += "\n规则：\n1. 仅生成SELECT查询语句，禁止任何增删改\n2. 仅使用上述表和字段\n3. 返回纯SQL，不要任何解释\n4. 字段名必须严格匹配"
    return schema_prompt

# SQL安全校验
def validate_sql_safety(sql: str) -> bool:
    if not sql:
        return False
    sql_upper = sql.strip().upper()
    allowed_tables = [t.upper() for t in TABLE_RULES["create_order"]]
    
    if not sql_upper.startswith("SELECT"):
        safe_print("[❌ 拦截] 仅允许查询语句")
        return False
    
    dangerous = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "TRUNCATE", "CREATE"]
    for kw in dangerous:
        if kw in sql_upper:
            safe_print(f"[❌ 拦截] 禁止危险操作：{kw}")
            return False
    
    for table in allowed_tables:
        if f"FROM {table}" in sql_upper or f"JOIN {table}" in sql_upper:
            return True
    safe_print("[❌ 拦截] 禁止使用未授权数据表")
    return False

# 自然语言转SQL
def nl_to_sql(natural_query: str) -> str:
    if not LLM_CONFIG["api_key"]:
        safe_print("[❌ 错误] 请配置.env中的LLM_API_KEY")
        return ""
    try:
        resp = client.chat.completions.create(
            model=LLM_CONFIG["model"],
            temperature=LLM_CONFIG["temperature"],
            messages=[{"role": "system", "content": get_table_schema_prompt()},
                      {"role": "user", "content": natural_query}]
        )
        sql = resp.choices[0].message.content.strip()
        return re.sub(r"```sql|```", "", sql).strip()
    except Exception as e:
        safe_print(f"[❌ LLM调用失败：{str(e)}]")
        return ""

# 全流程查询
def llm_query(q: str):
    safe_print(f"\n[🔍 你的查询] {q}")
    sql = nl_to_sql(q)
    if not sql or not validate_sql_safety(sql):
        return []
    return query_database(sql)

# 主程序：交互式输入
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
        llm_query(user_q)