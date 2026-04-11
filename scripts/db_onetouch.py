#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV一键同步MySQL（极简一步版）
✅ 一步执行：删除所有表 → 重建表结构 → 全量导入CSV数据
✅ 适配新 game_song_rel 表结构（游戏编号INT+新增游戏名字段）
✅ 无参数、无交互、无冗余逻辑
✅ 🔥 修复：彻底处理 NaT 日期空值问题
"""
import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ===================== 路径配置 =====================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MAIN_PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, MAIN_PROJECT_ROOT)

# ===================== 配置加载 =====================
load_dotenv(os.path.join(MAIN_PROJECT_ROOT, ".env"))
from config.settings import CSV_TARGET_DIR, DB_CONFIG
from config.table_schemas import TABLE_RULES

# ===================== 数据库/CSV配置 =====================
MYSQL_CONFIG = {
    "host": DB_CONFIG["host"],
    "port": DB_CONFIG["port"],
    "user": DB_CONFIG["user"],
    "password": DB_CONFIG["password"],
    "database": DB_CONFIG["db"],
    "charset": DB_CONFIG["charset"]
}

CSV_PATHS = {
    "game_info": os.path.join(CSV_TARGET_DIR, "game_info.csv"),
    "song_info": os.path.join(CSV_TARGET_DIR, "song_info.csv"),
    "author_info": os.path.join(CSV_TARGET_DIR, "author_info.csv"),
    "game_song_rel": os.path.join(CSV_TARGET_DIR, "game_song_rel.csv"),
    "song_author_rel": os.path.join(CSV_TARGET_DIR, "song_author_rel.csv"),
    "game_linkage_rel": os.path.join(CSV_TARGET_DIR, "game_linkage_rel.csv")
}

# ===================== 数据库核心函数 =====================
def get_mysql_engine():
    conn_str = (
        f"mysql+pymysql://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@"
        f"{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}?charset={MYSQL_CONFIG['charset']}"
    )
    return create_engine(conn_str, pool_pre_ping=True)

def generate_create_table_sql(table_name):
    """生成建表SQL"""
    table_rule = TABLE_RULES[table_name]
    fields = list(table_rule["field_types"].keys())
    field_sql = []
    
    for field in fields:
        field_type = table_rule["field_types"][field]
        if field == table_rule["primary_key"]:
            field_sql.append(f"`{field}` {field_type} NOT NULL PRIMARY KEY COMMENT '主键'")
        elif field == "update_timestamp":
            field_sql.append(f"`{field}` {field_type} DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '系统时间戳'")
        else:
            field_sql.append(f"`{field}` {field_type} COMMENT '{field}'")
    
    if table_rule["foreign_keys"]:
        field_sql.extend(table_rule["foreign_keys"])
    
    return f"""CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(field_sql)}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"""

def read_csv_safe(csv_path):
    """安全读取CSV"""
    try:
        return pd.read_csv(csv_path, encoding="utf-8-sig")
    except:
        return pd.read_csv(csv_path, encoding="utf-8")

# ===================== 【核心修复】数据清洗函数 =====================
def preprocess_df(df, table_name):
    """数据清洗（修复版：彻底处理 NaT 日期空值）"""
    rules = TABLE_RULES[table_name]
    pk = rules["primary_key"]
    date_cols = rules["date_cols"]

    # 主键清洗
    if pk in df.columns:
        df[pk] = df[pk].fillna("").astype(str).str.strip()
    
    # 日期列清洗（核心修复）
    for col in date_cols:
        if col in df.columns:
            # 1. 先统一转为 datetime，错误/空值转为 NaT
            df[col] = pd.to_datetime(df[col], errors="coerce")
            # 2. 显式将 NaT 替换为 None，有效日期转为 date 对象
            # 这一步确保 MySQL 收到的是 NULL 而不是 NaT
            df[col] = df[col].apply(lambda x: x.date() if pd.notna(x) else None)
    
    # 其余列空值处理
    return df.where(pd.notna(df), None)

# ===================== 一步式核心流程 =====================
def one_click_sync():
    engine = get_mysql_engine()
    create_order = TABLE_RULES["create_order"]
    drop_order = ["game_song_rel", "song_author_rel", "game_linkage_rel", "song_info", "author_info", "game_info"]

    print("=" * 60)
    print("🔥 一步式数据库同步开始：删表 → 建表 → 导入数据")
    print("=" * 60)

    # 1. 删除所有表（避免结构冲突）
    print("\n1/3 🗑️ 删除旧表...")
    with engine.connect() as conn:
        for table in drop_order:
            try:
                conn.execute(text(f"DROP TABLE IF EXISTS {table};"))
                print(f"✅ 删除表: {table}")
            except Exception as e:
                print(f"⚠️ 删除表 {table} 失败: {str(e)}")
        conn.commit()

    # 2. 重建所有表
    print("\n2/3 📊 重建表结构...")
    with engine.connect() as conn:
        for table in create_order:
            try:
                conn.execute(text(generate_create_table_sql(table)))
                print(f"✅ 重建表: {table}")
            except Exception as e:
                print(f"⚠️ 创建表 {table} 失败: {str(e)}")
        conn.commit()

    # 3. 全量导入CSV数据
    print("\n3/3 📥 全量导入数据...")
    for table_name in create_order:
        if table_name not in CSV_PATHS:
            continue
        
        csv_path = CSV_PATHS[table_name]
        if not os.path.exists(csv_path):
            print(f"⚠️ {table_name} CSV不存在，跳过")
            continue

        # 读取+清洗数据
        df = read_csv_safe(csv_path)
        df = preprocess_df(df, table_name)
        if df.empty:
            print(f"ℹ️ {table_name} 无数据，跳过")
            continue

        # 批量插入
        rules = TABLE_RULES[table_name]
        cols = [f"`{col}`" for col in rules["field_types"].keys() if col in df.columns]
        placeholders = [f":{col}" for col in rules["field_types"].keys() if col in df.columns]
        sql = text(f"INSERT INTO {table_name} ({','.join(cols)}) VALUES ({','.join(placeholders)})")

        with engine.begin() as conn:
            for _, row in df.iterrows():
                row_dict = row.to_dict()
                # 额外安全检查：确保没有 NaT 漏网
                for k, v in row_dict.items():
                    if pd.isna(v):
                        row_dict[k] = None
                conn.execute(sql, row_dict)
        
        print(f"✅ 导入 {table_name}: {len(df)} 条")

    print("\n" + "=" * 60)
    print("🎉 一步同步完成！所有表已更新为最新结构+数据")
    print("=" * 60)

# ===================== 主入口（直接运行） =====================
if __name__ == "__main__":
    one_click_sync()