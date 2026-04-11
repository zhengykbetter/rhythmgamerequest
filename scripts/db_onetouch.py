#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV一键同步MySQL（增强调试版）
✅ 一步执行：删除所有表 → 重建表结构 → 全量导入CSV数据
✅ 适配新 game_song_rel 表结构
✅ 🔥 增强：详细的空值统计、日期解析调试信息
✅ 🔥 帮助定位：为什么会有这么多空时间数据
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

# ===================== 【增强调试】数据清洗与统计函数 =====================
def preprocess_df_with_debug(df, table_name, csv_path):
    """
    数据清洗 + 详细调试统计
    输出：清洗后的DataFrame + 详细统计信息
    """
    rules = TABLE_RULES[table_name]
    pk = rules["primary_key"]
    date_cols = rules["date_cols"]
    
    print(f"\n📊 【{table_name}】数据质量统计：")
    print(f"   总行数：{len(df):,}")
    
    # 1. 原始数据空值统计
    print(f"\n   🔍 原始CSV空值统计：")
    for col in df.columns:
        null_count = df[col].isna().sum()
        if null_count > 0:
            print(f"      - {col}: {null_count:,} 条空值 ({null_count/len(df)*100:.1f}%)")
    
    # 2. 日期列详细解析统计
    if date_cols:
        print(f"\n   📅 日期列解析统计：")
        for col in date_cols:
            if col not in df.columns:
                continue
            
            # 统计原始空值
            original_null = df[col].isna().sum()
            
            # 尝试解析
            parsed_series = pd.to_datetime(df[col], errors="coerce")
            parse_fail = parsed_series.isna().sum() - original_null
            
            print(f"      - {col}:")
            print(f"         原始空值：{original_null:,} 条")
            if parse_fail > 0:
                print(f"         ⚠️  解析失败：{parse_fail:,} 条（格式错误）")
                
                # 打印前5条解析失败的样本
                fail_samples = df[parsed_series.isna() & df[col].notna()][col].head(5)
                if len(fail_samples) > 0:
                    print(f"         解析失败样本：{list(fail_samples.values)}")
    
    # 3. 正式清洗
    print(f"\n   🔧 开始数据清洗...")
    
    # 主键清洗
    if pk in df.columns:
        df[pk] = df[pk].fillna("").astype(str).str.strip()
    
    # 日期列清洗
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            df[col] = df[col].apply(lambda x: x.date() if pd.notna(x) else None)
    
    # 其余列空值处理
    df = df.where(pd.notna(df), None)
    
    # 4. 清洗后最终统计
    final_null_count = sum(df[col].isna().sum() for col in date_cols if col in df.columns)
    if final_null_count > 0:
        print(f"   ⚠️  清洗后仍有 {final_null_count:,} 条日期空值将存入 NULL")
    else:
        print(f"   ✅ 清洗完成，无日期空值")
    
    return df

# ===================== 一步式核心流程 =====================
def one_click_sync():
    engine = get_mysql_engine()
    create_order = TABLE_RULES["create_order"]
    drop_order = ["game_song_rel", "song_author_rel", "game_linkage_rel", "song_info", "author_info", "game_info"]

    print("=" * 80)
    print("🔥 一步式数据库同步开始（增强调试版）")
    print("=" * 80)

    # 1. 删除所有表
    print("\n" + "=" * 80)
    print("1/3 🗑️ 删除旧表...")
    print("=" * 80)
    with engine.connect() as conn:
        for table in drop_order:
            try:
                conn.execute(text(f"DROP TABLE IF EXISTS {table};"))
                print(f"✅ 删除表: {table}")
            except Exception as e:
                print(f"⚠️ 删除表 {table} 失败: {str(e)}")
        conn.commit()

    # 2. 重建所有表
    print("\n" + "=" * 80)
    print("2/3 📊 重建表结构...")
    print("=" * 80)
    with engine.connect() as conn:
        for table in create_order:
            try:
                conn.execute(text(generate_create_table_sql(table)))
                print(f"✅ 重建表: {table}")
            except Exception as e:
                print(f"⚠️ 创建表 {table} 失败: {str(e)}")
        conn.commit()

    # 3. 全量导入CSV数据（带调试）
    print("\n" + "=" * 80)
    print("3/3 📥 全量导入数据（增强调试）")
    print("=" * 80)
    for table_name in create_order:
        if table_name not in CSV_PATHS:
            continue
        
        csv_path = CSV_PATHS[table_name]
        if not os.path.exists(csv_path):
            print(f"\n⚠️ {table_name} CSV不存在，跳过")
            continue

        print(f"\n{'='*80}")
        print(f"📌 处理表：{table_name}")
        print(f"📂 CSV文件：{csv_path}")
        print(f"{'='*80}")

        # 读取+清洗数据（带调试）
        df = read_csv_safe(csv_path)
        df = preprocess_df_with_debug(df, table_name, csv_path)
        
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
                # 额外安全检查
                for k, v in row_dict.items():
                    if pd.isna(v):
                        row_dict[k] = None
                conn.execute(sql, row_dict)
        
        print(f"\n✅ 成功导入 {table_name}: {len(df):,} 条")

    print("\n" + "=" * 80)
    print("🎉 一步同步完成！")
    print("=" * 80)

# ===================== 主入口 =====================
if __name__ == "__main__":
    one_click_sync()