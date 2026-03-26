#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV增量更新MySQL数据库（适配新数据结构版）
核心修改：
1. 更新 TABLE_RULES 以匹配新的 CSV 结构（song_info/author_info/game_song_rel/song_author_rel）
2. 移除旧字段，添加新字段（如 song_info 的"本家"、author_info 的"作者名"）
3. 兼容 utf-8-sig 编码读取（防止Excel乱码带来的读取问题）
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

# ===================== 核心业务规则配置（已更新字段匹配新CSV） =====================
# 1. 6张表的CSV路径
CSV_PATHS = {
    "game_info": str(CSV_TARGET_DIR / "game_info.csv"),
    "song_info": str(CSV_TARGET_DIR / "song_info.csv"),
    "author_info": str(CSV_TARGET_DIR / "author_info.csv"),
    "game_song_rel": str(CSV_TARGET_DIR / "game_song_rel.csv"),
    "song_author_rel": str(CSV_TARGET_DIR / "song_author_rel.csv"),
    "game_linkage_rel": str(CSV_TARGET_DIR / "game_linkage_rel.csv")
}

TABLE_RULES = {
    # 建表顺序（基础表在前，关联表在后）
    "create_order": ["game_info", "author_info", "song_info", "game_song_rel", "song_author_rel", "game_linkage_rel"],
    
    # 基础表：游戏信息表（保持不变）
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

    # 基础表：作者信息表（已适配新结构）
    # 新结构：author_id, 作者名, 别名, 备注, 最新更新时间
    "author_info": {
        "primary_key": "author_id",
        "auto_cols": ["最新更新时间", "update_timestamp"],
        "date_cols": [],
        "field_types": {
            "author_id": "VARCHAR(50)",
            "作者名": "VARCHAR(1000)",       # 旧：作者本名
            "别名": "VARCHAR(500)",          # 旧：作者别称
            "备注": "VARCHAR(500)",
            "最新更新时间": "DATETIME",
            "update_timestamp": "DATETIME"
        },
        "foreign_keys": []
    },

    # 基础表：歌曲信息表（已适配新结构）
    # 新结构：song_id, 歌名, 别名, 作者, 本家, 最新更新时间
    "song_info": {
        "primary_key": "song_id",
        "auto_cols": ["最新更新时间", "update_timestamp"],
        "date_cols": [],                   # 移除了旧的"歌曲更新时间"
        "field_types": {
            "song_id": "VARCHAR(50)",
            "歌名": "VARCHAR(1000)",
            "别名": "VARCHAR(1000)",        # 长度放宽以适应合并后的别名
            "作者": "VARCHAR(1000)",         # 新增：名义作者
            "本家": "VARCHAR(200)",         # 新增：本家字段
            "最新更新时间": "DATETIME",
            "update_timestamp": "DATETIME"
        },
        "foreign_keys": []
    },

    # 关联表：游戏-歌曲关联表（已适配新结构）
    # 新结构：rel_id, 游戏编号, song_id, 本家, 收录时间, 最新更新时间
    "game_song_rel": {
        "primary_key": "rel_id",
        "auto_cols": ["最新更新时间", "update_timestamp"],
        "date_cols": ["收录时间"],
        "field_types": {
            "rel_id": "VARCHAR(200)",
            "游戏编号": "VARCHAR(100)",
            "song_id": "VARCHAR(50)",
            "本家": "VARCHAR(200)",         # 新增：记录本家名称
            "收录时间": "DATE",
            "最新更新时间": "DATETIME",
            "update_timestamp": "DATETIME"
        },
        "foreign_keys": []
    },

    # 关联表：歌曲-作者关联表（已适配新结构）
    # 新结构：rel_id, song_id, author_id, 曲名, 作者名, 最新更新时间
    "song_author_rel": {
        "primary_key": "rel_id",
        "auto_cols": ["最新更新时间", "update_timestamp"],
        "date_cols": [],
        "field_types": {
            "rel_id": "VARCHAR(100)",
            "song_id": "VARCHAR(50)",
            "author_id": "VARCHAR(50)",
            "曲名": "VARCHAR(1000)",          # 新增：冗余核对字段
            "作者名": "VARCHAR(1000)",         # 新增：冗余核对字段
            "最新更新时间": "DATETIME",
            "update_timestamp": "DATETIME"
        },
        "foreign_keys": []
    },

    # 关联表：游戏联动表（保持原样，作为占位）
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

# ===================== 其他配置（保持不变） =====================
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

# ===================== 核心建表逻辑（已优化） =====================
def generate_create_table_sql(table_name):
    """基于 TABLE_RULES 生成建表 SQL"""
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

# ===================== CSV处理通用函数（已适配utf-8-sig） =====================
def read_csv_with_encoding(csv_path):
    """兼容读取 utf-8 和 utf-8-sig (Excel乱码修复版)"""
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

# ===================== 通用增量更新函数（适配动态字段） =====================
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
        print(f"ℹ️ 提示：{table_name} 的CSV无变化，跳过更新！")
        return True

    # CSV存档
    archive_csv(table_name, csv_path)

    # 读取并预处理CSV（使用兼容读取）
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

    with engine.connect() as conn:
        # 获取数据库主键列表
        db_pk_result = conn.execute(text(f"SELECT {primary_key} FROM {table_name}")).fetchall()
        db_pk_list = [k[0] for k in db_pk_result]

        # 1. 删除同步
        csv_pk_list = df[df[primary_key] != ""][primary_key].tolist()
        delete_pk_list = [pk for pk in db_pk_list if pk not in csv_pk_list]
        
        if delete_pk_list:
            # 安全拼接IN查询
            placeholders = ", ".join([f"'{pk}'" for pk in delete_pk_list])
            conn.execute(text(f"DELETE FROM {table_name} WHERE {primary_key} IN ({placeholders})"))
            delete_count = len(delete_pk_list)
            print(f"🗑️ 删除 {delete_count} 条{table_name}数据")

        # 2. 新增同步
        df_add = df[~df[primary_key].isin(db_pk_list)]
        add_count = len(df_add)
        if add_count > 0:
            df_add.to_sql(table_name, engine, if_exists="append", index=False)
            print(f"✅ 新增 {add_count} 条{table_name}数据")

        # 3. 更新同步
        df_update = df[df[primary_key].isin(db_pk_list)]
        exclude_cols = rules["auto_cols"] + [primary_key]
        update_count = 0

        for idx, row in df_update.iterrows():
            pk_value = row[primary_key]
            # 使用 text() 和正确的参数绑定
            db_row = conn.execute(text(f"SELECT * FROM {table_name} WHERE {primary_key} = :pk"), {"pk": pk_value}).fetchone()
            if not db_row:
                continue

            db_dict = dict(zip(db_row.keys(), db_row))
            row_dict = row.to_dict()
            update_sql_parts = []
            update_params = {}

            for col in row_dict:
                if col in exclude_cols or col not in db_dict:
                    continue
                
                db_val = db_dict[col]
                csv_val = row_dict[col]
                
                # 类型兼容处理
                if isinstance(csv_val, pd.Timestamp):
                    csv_val = csv_val.date()
                
                # 只有值不同时才更新
                if db_val != csv_val:
                    update_sql_parts.append(f"`{col}` = :{col}")
                    update_params[col] = csv_val

            if update_sql_parts:
                update_sql = f"UPDATE {table_name} SET {', '.join(update_sql_parts)} WHERE {primary_key} = :pk"
                update_params["pk"] = pk_value
                conn.execute(text(update_sql), update_params)
                update_count += 1

        conn.commit()
        print(f"✏️ 更新 {update_count} 条{table_name}数据")
        print(f"✅ {table_name} 同步完成！新增：{add_count} | 更新：{update_count} | 删除：{delete_count}")

    # 保存状态
    processed_state[table_name] = {
        "md5": current_md5,
        "process_time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "sync_stats": {"add": add_count, "update": update_count, "delete": delete_count}
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
    # 注意：如果表结构已变更，建议先运行 python script.py clear 删表，再运行 init
    # init_all_tables() # 移除自动init，防止误操作，改为手动命令控制更好

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