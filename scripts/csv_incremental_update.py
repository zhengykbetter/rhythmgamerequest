#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV增量更新MySQL数据库（适配新数据结构版 + 主键冲突修复版）
核心修改：
1. 更新 TABLE_RULES 以匹配新的 CSV 结构
2. 兼容 utf-8-sig 编码读取
3. 🔥 修复主键冲突：新增同步使用 ON DUPLICATE KEY UPDATE
"""
import os
import sys
import time
import json
import hashlib
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

# ===================== 核心业务规则配置 =====================
CSV_PATHS = {
    "game_info": str(CSV_TARGET_DIR / "game_info.csv"),
    "song_info": str(CSV_TARGET_DIR / "song_info.csv"),
    "author_info": str(CSV_TARGET_DIR / "author_info.csv"),
    "game_song_rel": str(CSV_TARGET_DIR / "game_song_rel.csv"),
    "song_author_rel": str(CSV_TARGET_DIR / "song_author_rel.csv"),
    "game_linkage_rel": str(CSV_TARGET_DIR / "game_linkage_rel.csv")
}

TABLE_RULES = {
    "create_order": ["game_info", "author_info", "song_info", "game_song_rel", "song_author_rel", "game_linkage_rel"],
    "game_info": {
        "primary_key": "游戏编号",
        "auto_cols": ["最新更新时间", "update_timestamp"],
        "date_cols": ["实装时间", "更新时间", "数据时间", "开服时间"],
        "field_types": {
            "游戏编号": "VARCHAR(100)",
            "游戏": "VARCHAR(200)",
            "别名": "VARCHAR(200)",
            "实装时间": "DATE",
            "更新时间": "DATE",
            "数据时间": "DATE",
            "开服时间": "DATE",
            "最新更新时间": "DATETIME",
            "update_timestamp": "DATETIME"
        },
        "foreign_keys": []
    },
    "author_info": {
        "primary_key": "author_id",
        "auto_cols": ["最新更新时间", "update_timestamp"],
        "date_cols": [],
        "field_types": {
            "author_id": "VARCHAR(50)",
            "作者名": "VARCHAR(1000)",
            "别名": "VARCHAR(500)",
            "备注": "VARCHAR(500)",
            "最新更新时间": "DATETIME",
            "update_timestamp": "DATETIME"
        },
        "foreign_keys": []
    },
    "song_info": {
        "primary_key": "song_id",
        "auto_cols": ["最新更新时间", "update_timestamp"],
        "date_cols": [],
        "field_types": {
            "song_id": "VARCHAR(50)",
            "歌名": "VARCHAR(1000)",
            "别名": "VARCHAR(1000)",
            "作者": "VARCHAR(1000)",
            "本家": "VARCHAR(200)",
            "最新更新时间": "DATETIME",
            "update_timestamp": "DATETIME"
        },
        "foreign_keys": []
    },
    "game_song_rel": {
        "primary_key": "rel_id",
        "auto_cols": ["最新更新时间", "update_timestamp"],
        "date_cols": ["收录时间"],
        "field_types": {
            "rel_id": "VARCHAR(200)",
            "游戏编号": "VARCHAR(100)",
            "song_id": "VARCHAR(50)",
            "本家": "VARCHAR(200)",
            "收录时间": "DATE",
            "最新更新时间": "DATETIME",
            "update_timestamp": "DATETIME"
        },
        "foreign_keys": []
    },
    "song_author_rel": {
        "primary_key": "rel_id",
        "auto_cols": ["最新更新时间", "update_timestamp"],
        "date_cols": [],
        "field_types": {
            "rel_id": "VARCHAR(100)",
            "song_id": "VARCHAR(50)",
            "author_id": "VARCHAR(50)",
            "曲名": "VARCHAR(1000)",
            "作者名": "VARCHAR(1000)",
            "最新更新时间": "DATETIME",
            "update_timestamp": "DATETIME"
        },
        "foreign_keys": []
    },
    "game_linkage_rel": {
        "primary_key": "rel_id",
        "auto_cols": ["最新更新时间", "update_timestamp"],
        "date_cols": ["联动时间"],
        "field_types": {
            "rel_id": "VARCHAR(200)",
            "游戏1编号": "VARCHAR(100)",
            "游戏2编号": "VARCHAR(50)",
            "游戏1名称": "VARCHAR(200)",
            "游戏2名称": "VARCHAR(200)",
            "联动名称": "VARCHAR(200)",
            "联动时间": "DATE",
            "联动版本": "VARCHAR(50)",
            "说明": "VARCHAR(500)",
            "最新更新时间": "DATETIME",
            "update_timestamp": "DATETIME"
        },
        "foreign_keys": []
    }
}

# ===================== 其他配置 =====================
TYPE_MAPPING = {
    "int64": "INT",
    "float64": "FLOAT",
    "object": "VARCHAR(255)",
    "datetime64[ns]": "DATE",
    "bool": "TINYINT(1)"
}

ARCHIVE_DIR = str(CSV_TARGET_DIR / "archive")
os.makedirs(ARCHIVE_DIR, exist_ok=True)
STATE_FILE_PATH = os.path.join(MAIN_PROJECT_ROOT, "data_csv", "csv_processed_state.json")

MYSQL_CONFIG = {
    "host": DB_CONFIG["host"],
    "port": DB_CONFIG["port"],
    "user": DB_CONFIG["user"],
    "password": DB_CONFIG["password"],
    "database": DB_CONFIG["db"],
    "charset": DB_CONFIG["charset"]
}

# ===================== 数据库基础函数 =====================
def get_mysql_engine():
    conn_str = (
        f"mysql+pymysql://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@"
        f"{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}?charset={MYSQL_CONFIG['charset']}"
    )
    return create_engine(conn_str, pool_pre_ping=True, pool_recycle=3600)

# ===================== 核心建表逻辑 =====================
def generate_create_table_sql(table_name):
    table_rule = TABLE_RULES[table_name]
    fields = list(table_rule["field_types"].keys())
    
    field_sql = []
    for field in fields:
        field_type = table_rule["field_types"][field]
        if field == table_rule["primary_key"]:
            field_sql.append(f"`{field}` {field_type} NOT NULL PRIMARY KEY COMMENT '主键'")
        elif field == "update_timestamp":
            field_sql.append(f"`{field}` {field_type} DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '系统更新时间戳'")
        else:
            field_sql.append(f"`{field}` {field_type} COMMENT '{field}'")
    
    if table_rule["foreign_keys"]:
        field_sql.extend(table_rule["foreign_keys"])
    
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(field_sql)}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='{table_name}';
    """
    return create_sql

def init_all_tables():
    engine = get_mysql_engine()
    with engine.connect() as conn:
        create_order = TABLE_RULES["create_order"]
        for table_name in create_order:
            try:
                create_sql = generate_create_table_sql(table_name)
                conn.execute(text(create_sql))
                print(f"✅ 成功初始化表 {table_name}")
            except Exception as e:
                print(f"⚠️  初始化表 {table_name} 失败：{str(e)}")
        conn.commit()
    print("\n✅ 所有表初始化完成！")

# ===================== CSV处理通用函数 =====================
def read_csv_with_encoding(csv_path):
    try:
        return pd.read_csv(csv_path, encoding="utf-8-sig")
    except:
        return pd.read_csv(csv_path, encoding="utf-8")

def archive_csv(table_name, csv_path):
    if not csv_path or not os.path.exists(csv_path):
        return None
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    archive_path = os.path.join(ARCHIVE_DIR, f"{table_name}_{timestamp}.csv")
    read_csv_with_encoding(csv_path).to_csv(archive_path, index=False, encoding="utf-8-sig")
    print(f"📁 CSV存档完成：{archive_path}")
    return archive_path

def preprocess_data(df, table_name):
    rules = TABLE_RULES[table_name]
    primary_key = rules["primary_key"]
    date_cols = rules["date_cols"]
    
    if primary_key in df.columns:
        df[primary_key] = df[primary_key].fillna("").astype(str).str.strip()
    
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    
    df = df.where(pd.notna(df), None)
    return df

def get_file_md5(file_path):
    if not os.path.exists(file_path):
        return ""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def load_processed_state():
    if os.path.exists(STATE_FILE_PATH):
        with open(STATE_FILE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_processed_state(state):
    os.makedirs(os.path.dirname(STATE_FILE_PATH), exist_ok=True)
    with open(STATE_FILE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

# ===================== 通用增量更新函数（🔥 核心修复：主键冲突处理） =====================
def incremental_update_single(table_name):
    if table_name not in CSV_PATHS or table_name not in TABLE_RULES:
        print(f"❌ 错误：不支持的表名 {table_name}")
        return False

    csv_path = CSV_PATHS[table_name]
    if not os.path.exists(csv_path):
        print(f"❌ 错误：{table_name}的CSV文件不存在 → {csv_path}")
        return False

    # MD5校验
    processed_state = load_processed_state()
    current_md5 = get_file_md5(csv_path)
    last_md5 = processed_state.get(table_name, {}).get("md5", "")
    if current_md5 == last_md5 and last_md5 != "":
        print(f"ℹ️ 提示：{table_name} 的CSV无变化，不跳过更新！")

    # CSV存档
    archive_csv(table_name, csv_path)

    # 读取并预处理CSV
    df = read_csv_with_encoding(csv_path)
    df = preprocess_data(df, table_name)
    if df.empty:
        print(f"ℹ️ 提示：{table_name} 的CSV无有效数据！")
        return True

    # 主键校验
    rules = TABLE_RULES[table_name]
    primary_key = rules["primary_key"]
    if primary_key not in df.columns:
        print(f"❌ 错误：{table_name}的CSV缺少主键「{primary_key}」")
        return False

    # 数据库同步
    engine = get_mysql_engine()
    add_count = update_count = delete_count = 0
    upsert_count = 0 # 新增：统计UPSERT数量

    with engine.connect() as conn:
        # 获取数据库主键列表
        db_pk_result = conn.execute(text(f"SELECT {primary_key} FROM {table_name}")).fetchall()
        db_pk_list = [k[0] for k in db_pk_result]

        # 1. 删除同步
        csv_pk_list = df[df[primary_key] != ""][primary_key].tolist()
        delete_pk_list = [pk for pk in db_pk_list if pk not in csv_pk_list]
        
        if delete_pk_list:
            placeholders = ", ".join([f"'{pk}'" for pk in delete_pk_list])
            conn.execute(text(f"DELETE FROM {table_name} WHERE {primary_key} IN ({placeholders})"))
            delete_count = len(delete_pk_list)
            print(f"🗑️ 删除 {delete_count} 条{table_name}数据")

        # 2. 🔥 核心修复：新增/更新同步 (UPSERT: ON DUPLICATE KEY UPDATE)
        # 不再区分 df_add 和 df_update，统一使用 UPSERT 处理所有数据
        # 这样既能解决主键冲突，又能自动更新旧数据
        
        # 获取字段列表（过滤掉不存在于CSV中的字段）
        available_cols = [col for col in rules["field_types"].keys() if col in df.columns]
        exclude_cols = rules["auto_cols"] # 自动更新的字段不强制覆盖
        
        # 构建 SQL 模板
        cols_sql = ", ".join([f"`{col}`" for col in available_cols])
        vals_sql = ", ".join([f":{col}" for col in available_cols])
        
        # 构建 ON DUPLICATE KEY UPDATE 部分
        update_sql_parts = []
        for col in available_cols:
            if col == primary_key or col in exclude_cols:
                continue
            update_sql_parts.append(f"`{col}` = VALUES(`{col}`)")
        
        update_sql = ", ".join(update_sql_parts)
        
        # 完整的 UPSERT SQL
        upsert_sql = text(f"""
            INSERT INTO {table_name} ({cols_sql})
            VALUES ({vals_sql})
            ON DUPLICATE KEY UPDATE
            {update_sql}
        """)

        # 批量执行 UPSERT
        print(f"🔄 执行 {table_name} 数据同步 (UPSERT模式)...")
        transaction = conn.begin()
        try:
            for idx, row in df.iterrows():
                # 类型处理
                row_dict = row.to_dict()
                for k, v in row_dict.items():
                    if isinstance(v, pd.Timestamp):
                        row_dict[k] = v.date()
                    if pd.isna(v):
                        row_dict[k] = None
                
                conn.execute(upsert_sql, row_dict)
                upsert_count += 1
            
            transaction.commit()
            
            # 估算统计（因为UPSERT同时处理新增和更新，这里简化统计）
            print(f"✅ {table_name} 同步完成！共处理 {upsert_count} 条数据（新增+更新合并）")

        except Exception as e:
            transaction.rollback()
            print(f"❌ {table_name} 同步失败：{str(e)}")
            return False

        # 3. (原有的逐行更新逻辑已移除，因为 UPSERT 已经包含了更新功能)
        # 为了保持状态文件兼容，我们这里简单赋值
        add_count = upsert_count
        update_count = 0 

    # 保存状态
    processed_state[table_name] = {
        "md5": current_md5,
        "process_time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "sync_stats": {"add": add_count, "update": update_count, "delete": delete_count, "upsert": upsert_count}
    }
    save_processed_state(processed_state)
    return True

# ===================== 批量同步+删除表函数 =====================
def incremental_update_all():
    print("===== 开始批量同步所有表 =====")
    for table_name in TABLE_RULES["create_order"]:
        if table_name in CSV_PATHS:
            print(f"\n📌 同步 {table_name}...")
            incremental_update_single(table_name)
    print("\n✅ 所有表同步完成！")

def drop_all_tables():
    print("⚠️  警告：将删除所有表，且无法恢复！")
    if input("输入 YES 确认：") != "YES" or input("再次输入 YES 确认：") != "YES":
        print("ℹ️  取消删除")
        return False

    engine = get_mysql_engine()
    with engine.connect() as conn:
        drop_order = ["game_song_rel", "song_author_rel", "game_linkage_rel", "song_info", "author_info", "game_info"]
        for table in drop_order:
            try:
                conn.execute(text(f"DROP TABLE IF EXISTS {table};"))
                print(f"🗑️ 成功删除表：{table}")
            except Exception as e:
                print(f"⚠️  删除 {table} 失败：{str(e)}")
        conn.commit()
    print("✅ 所有表已删除！")
    return True

# ===================== 主函数 =====================
def main():
    if len(sys.argv) == 1:
        print("📖 使用说明：")
        print("  1. 初始化表结构：python3 脚本名.py init")
        print("  2. 同步指定表：python3 脚本名.py [表名]（如 song_info）")
        print("  3. 批量同步所有表：python3 脚本名.py all")
        print("  4. 删除所有表：python3 脚本名.py clear")
        sys.exit(0)

    if sys.argv[1] == "init":
        init_all_tables()
    elif sys.argv[1] == "all":
        incremental_update_all()
    elif sys.argv[1] == "clear":
        drop_all_tables()
    else:
        incremental_update_single(sys.argv[1])

if __name__ == "__main__":
    main()