#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库查询核心模块
仅支持只读查询，返回格式化结果 + 终端输出
"""
import os
import sys
# 🔥 修复：自动添加项目根目录到Python路径
MAIN_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, MAIN_PROJECT_ROOT)

from sqlalchemy import text
# 🔥 修复：正确导入 db.py
from server.db import get_mysql_engine

def query_database(sql: str, params: dict = None):
    """
    执行SQL查询，返回结果并打印到终端
    :param sql: 查询SQL语句
    :param params: 参数化查询（防注入）
    :return: 字典列表 / 空列表
    """
    # ===================== 安全校验（仅允许查询） =====================
    sql_clean = sql.strip().upper()
    if not sql_clean.startswith("SELECT"):
        print("[❌ 安全拦截] 仅允许执行 SELECT 查询语句！")
        return []

    engine = get_mysql_engine()
    result_list = []

    try:
        with engine.connect() as conn:
            print("=" * 60)
            print(f"[📝 执行SQL] \n{sql}")
            if params:
                print(f"[🔧 查询参数] {params}")

            # 执行查询
            result = conn.execute(text(sql), params or {})
            # 转换为字典列表
            columns = list(result.keys())
            for row in result:
                row_dict = dict(zip(columns, row))
                result_list.append(row_dict)

            print(f"[✅ 查询成功] 共返回 {len(result_list)} 条数据")
            print("[📊 结果预览]")
            for idx, item in enumerate(result_list[:5]):
                print(f"  {idx+1}. {item}")
            if len(result_list) > 5:
                print(f"  ... 共 {len(result_list)} 条")
            print("=" * 60)

            return result_list

    except Exception as e:
        print(f"[❌ 查询失败] 错误信息：{str(e)}")
        return []

# 测试用例
if __name__ == "__main__":
    test_sql = "SELECT 游戏编号, 游戏 FROM game_info LIMIT 5;"
    query_database(test_sql)