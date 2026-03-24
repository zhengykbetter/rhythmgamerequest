#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
LLM 自然语言转SQL服务（安全版）
1. 从.env加载API配置
2. 从csv_to_db读取TABLE_RULES表结构
3. 严格SQL安全校验
4. 仅支持只读查询
"""
import os
import sys
import re
# 自动添加项目根目录到Python路径
MAIN_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, MAIN_PROJECT_ROOT)

from dotenv import load_dotenv
from openai import OpenAI

# 🔥 修复核心：TABLE_RULES 在 scripts/csv_to_db 中！
from scripts.csv_to_db import TABLE_RULES

# 导入数据库查询
from server.db_query import query_database

# 加载环境变量
load_dotenv(os.path.join(MAIN_PROJECT_ROOT, ".env"))

# ===================== LLM 配置（从.env读取，绝不硬编码） =====================
LLM_CONFIG = {
    "api_key": os.getenv("LLM_API_KEY", ""),
    "base_url": os.getenv("LLM_BASE_URL", ""),
    "model": os.getenv("LLM_MODEL", "deepseek-chat"),
    "temperature": float(os.getenv("LLM_TEMPERATURE", 0.1)),
}

# 初始化LLM客户端
client = OpenAI(
    api_key=LLM_CONFIG["api_key"],
    base_url=LLM_CONFIG["base_url"]
)

# ===================== 自动生成表结构提示词（从TABLE_RULES读取） =====================
def get_table_schema_prompt():
    """自动读取业务表结构，生成LLM提示词"""
    table_names = [t for t in TABLE_RULES["create_order"]]
    schema_prompt = "数据库表结构如下（仅允许查询这些表）：\n"
    
    for table in table_names:
        rule = TABLE_RULES[table]
        fields = list(rule["field_types"].keys())
        schema_prompt += f"表名：{table} | 字段：{', '.join(fields)}\n"
    
    schema_prompt += "\n规则：\n1. 仅生成SELECT查询语句，禁止任何增删改\n2. 仅使用上述表和字段\n3. 返回纯SQL，不要任何解释\n4. 字段名必须严格匹配"
    return schema_prompt

# ===================== 严格SQL安全校验（核心防护） =====================
def validate_sql_safety(sql: str) -> bool:
    """
    安全校验：仅允许合法查询
    返回 True=安全 / False=非法
    """
    if not sql:
        return False
    
    sql_upper = sql.strip().upper()
    allowed_tables = [t.upper() for t in TABLE_RULES["create_order"]]
    
    # 1. 仅允许SELECT
    if not sql_upper.startswith("SELECT"):
        print("[❌ 拦截] 非查询语句")
        return False
    
    # 2. 禁止危险操作
    dangerous_keywords = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "TRUNCATE", "CREATE"]
    for keyword in dangerous_keywords:
        if keyword in sql_upper:
            print(f"[❌ 拦截] 危险操作：{keyword}")
            return False
    
    # 3. 仅允许使用指定表
    table_found = False
    for table in allowed_tables:
        if f"FROM {table}" in sql_upper or f"JOIN {table}" in sql_upper:
            table_found = True
            break
    if not table_found:
        print("[❌ 拦截] 使用了未授权的表")
        return False
    
    return True

# ===================== 核心：自然语言转SQL =====================
def nl_to_sql(natural_query: str) -> str:
    """自然语言转SQL"""
    if not LLM_CONFIG["api_key"]:
        print("[❌ 错误] 未配置LLM API密钥")
        return ""
    
    system_prompt = get_table_schema_prompt()
    
    try:
        response = client.chat.completions.create(
            model=LLM_CONFIG["model"],
            temperature=LLM_CONFIG["temperature"],
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": natural_query}
            ]
        )
        sql = response.choices[0].message.content.strip()
        # 清理LLM可能添加的多余符号
        sql = re.sub(r"```sql|```", "", sql).strip()
        return sql
    
    except Exception as e:
        print(f"[❌ LLM调用失败：{str(e)}]")
        return ""

# ===================== 全流程：自然语言→SQL→查询结果 =====================
def llm_query(natural_query: str):
    """
    全流程查询
    :param natural_query: 用户自然语言
    :return: 查询结果
    """
    print(f"[🔍 用户查询] {natural_query}")
    
    # 1. 转SQL
    sql = nl_to_sql(natural_query)
    if not sql:
        return []
    
    # 2. 安全校验
    if not validate_sql_safety(sql):
        return []
    
    # 3. 执行查询
    return query_database(sql)

# ===================== 测试 =====================
if __name__ == "__main__":
    # 测试自然语言查询
    test_query = "查询所有游戏名称"
    llm_query(test_query)