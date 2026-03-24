#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV增量更新MySQL数据库（全自动版）
核心特性：
1. 完全从CSV自动识别字段名+字段类型，无需硬编码表结构
2. 支持6张表的增/改/删全量同步，自动字段匹配
3. 仅保留核心业务规则配置（主键、自动字段、外键），字段从CSV动态生成
4. MD5校验+状态持久化，减少重复更新
5. clear命令删除所有表（不可逆）
配置来源：
- 非敏感路径：/config/settings.py
- 敏感信息：.env 文件（git忽略）
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

# ===================== 核心业务规则配置（仅保留规则，无字段硬编码） =====================
# 1. 6张表的CSV路径（用户只需配置这个）
CSV_PATHS = {
    "game_info": str(CSV_TARGET_DIR / "game_info.csv"),
    "song_info": str(CSV_TARGET_DIR / "song_info.csv"),
    "author_info": str(CSV_TARGET_DIR / "author_info.csv"),
    "game_song_rel": str(CSV_TARGET_DIR / "game_song_rel.csv"),
    "song_author_rel": str(CSV_TARGET_DIR / "song_author_rel.csv"),
    "game_linkage_rel": str(CSV_TARGET_DIR / "game_linkage_rel.csv")
}
TABLE_RULES = {
    # 新增：指定建表顺序（基础表在前，关联表在后）
    "create_order": ["game_info", "author_info", "song_info", "game_song_rel", "song_author_rel", "game_linkage_rel"],
    
    # 基础表：游戏信息表（清空外键）
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
        "varchar_length": {"游戏": 200, "别名": 200},
        "foreign_keys": []  # 清空外键
    },

    # 基础表：作者信息表（清空外键）
    "author_info": {
        "primary_key": "author_id",
        "auto_cols": ["最新更新时间", "update_timestamp"],
        "date_cols": [],
        "field_types": {
            "author_id": "VARCHAR(50)",
            "作者本名": "VARCHAR(300)",
            "作者别称": "VARCHAR(500)",
            "擅长风格": "VARCHAR(200)",
            "备注": "VARCHAR(500)",
            "最新更新时间": "DATETIME",
            "update_timestamp": "DATETIME"
        },
        "varchar_length": {"author_id": 50, "作者本名": 300, "作者别称": 500, "擅长风格": 200, "备注": 500},
        "foreign_keys": []  # 清空外键
    },

    # 基础表：歌曲信息表（清空外键）
    "song_info": {
        "primary_key": "song_id",
        "auto_cols": ["最新更新时间", "update_timestamp"],
        "date_cols": ["歌曲更新时间"],
        "field_types": {
            "song_id": "VARCHAR(50)",
            "歌名": "VARCHAR(200)",
            "别名": "VARCHAR(200)",
            "歌曲更新时间": "DATE",
            "最新更新时间": "DATETIME",
            "update_timestamp": "DATETIME"
        },
        "varchar_length": {"song_id": 50, "歌名": 200, "别名": 200},
        "foreign_keys": []  # 清空外键
    },

    # 关联表：游戏-歌曲关联表（清空外键）
    "game_song_rel": {
        "primary_key": "rel_id",
        "auto_cols": ["最新更新时间", "update_timestamp"],
        "date_cols": ["收录时间"],
        "field_types": {
            "rel_id": "VARCHAR(200)",
            "游戏编号": "VARCHAR(100)",
            "song_id": "VARCHAR(50)",
            "收录版本": "VARCHAR(50)",
            "收录时间": "DATE",
            "最新更新时间": "DATETIME",
            "update_timestamp": "DATETIME"
        },
        "varchar_length": {"rel_id": 200, "游戏编号": 100, "song_id": 50, "收录版本": 50},
        "foreign_keys": []  # 清空外键（核心修改：删除原外键配置）
    },

    # 关联表：歌曲-作者关联表（清空外键）
    "song_author_rel": {
        "primary_key": "rel_id",
        "auto_cols": ["最新更新时间", "update_timestamp"],
        "date_cols": [],
        "field_types": {
            "rel_id": "VARCHAR(100)",
            "song_id": "VARCHAR(50)",
            "author_id": "VARCHAR(50)",
            "合作类型": "VARCHAR(50)",
            "备注": "VARCHAR(500)",
            "最新更新时间": "DATETIME",
            "update_timestamp": "DATETIME"
        },
        "varchar_length": {"rel_id": 100, "song_id": 50, "author_id": 50, "合作类型": 50, "备注": 500},
        "foreign_keys": []  # 清空外键（核心修改：删除原外键配置）
    },

    # 关联表：游戏联动表（清空外键）
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
        "varchar_length": {"rel_id": 200, "游戏1编号": 100, "游戏2编号": 50, "游戏1名称": 200, "游戏2名称": 200, "联动名称": 200, "联动版本": 50, "说明": 500},
        "foreign_keys": []  # 清空外键
    }
}
# 3. 类型映射（Python类型 → MySQL类型）
TYPE_MAPPING = {
    "int64": "INT",
    "float64": "FLOAT",
    "object": "VARCHAR({length})",  # 字符串默认255长度
    "datetime64[ns]": "DATE",
    "bool": "TINYINT(1)"
}

# 4. 其他配置
ARCHIVE_DIR = str(CSV_TARGET_DIR / "archive")
os.makedirs(ARCHIVE_DIR, exist_ok=True)
STATE_FILE_PATH = os.path.join(MAIN_PROJECT_ROOT, "data_csv", "csv_processed_state.json")

# 5. MySQL连接配置
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
    """创建MySQL连接引擎"""
    conn_str = (
        f"mysql+pymysql://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@"
        f"{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}?charset={MYSQL_CONFIG['charset']}"
    )
    return create_engine(
        conn_str,
        pool_pre_ping=True,
        pool_recycle=3600
    )

# ===================== 核心：从CSV自动推断字段+生成建表SQL =====================
def infer_csv_fields(table_name):
    """从CSV推断字段名和类型"""
    csv_path = CSV_PATHS[table_name]
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV文件不存在：{csv_path}")
    
    # 读取CSV（仅读前10行推断类型，提升效率）
    df = pd.read_csv(csv_path, encoding="utf-8", nrows=10)
    # 处理日期字段（提前转类型，确保推断准确）
    date_cols = TABLE_RULES[table_name]["date_cols"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    
    # 推断每个字段的类型
    field_info = {}
    for col in df.columns:
        # 获取Pandas类型
        dtype = str(df[col].dtype)
        # 特殊处理：主键字段强制为INT（game_info）或VARCHAR（其余）
        if col == TABLE_RULES[table_name]["primary_key"]:
            if table_name == "game_info":
                dtype = "int64"
            else:
                dtype = "object"
        field_info[col] = dtype
    return field_info

def generate_create_table_sql(table_name):
    """生成建表SQL（完全基于TABLE_RULES的field_types，无CSV依赖）"""
    # 1. 获取当前表的规则配置
    table_rule = TABLE_RULES[table_name]
    
    # 2. 从field_types提取字段（核心：不再读CSV，直接用显式定义的字段）
    fields = list(table_rule["field_types"].keys())
    
    # 3. 构建字段SQL（显式指定类型，解决自动推断错误）
    field_sql = []
    for field in fields:
        field_type = table_rule["field_types"][field]
        # 主键处理
        if field == table_rule["primary_key"]:
            field_sql.append(f"`{field}` {field_type} NOT NULL PRIMARY KEY COMMENT '主键'")
        # 自动时间戳字段特殊处理
        elif field == "update_timestamp":
            field_sql.append(f"`{field}` {field_type} DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '系统更新时间戳'")
        # 普通字段
        else:
            field_sql.append(f"`{field}` {field_type} COMMENT '{field}'")
    
    # 4. 拼接外键（修正写法：中文字段加反引号）
    if table_rule["foreign_keys"]:
        field_sql.extend(table_rule["foreign_keys"])
    
    # 5. 组装完整SQL
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(field_sql)}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='{table_name}（自动生成）';
    """
    return create_sql

def init_all_tables():
    """全自动初始化表结构（从TABLE_RULES推断字段，无需硬编码/CSV/OUTPUT_PATHS）"""
    engine = get_mysql_engine()  # 保留你原有API，不新增MYSQL_CONFIG
    with engine.connect() as conn:
        # 核心修改：按create_order遍历（基础表在前，关联表在后），解决外键依赖
        create_order = TABLE_RULES["create_order"]
        for table_name in create_order:
            try:
                # 动态生成建表SQL（重写该函数，基于field_types）
                create_sql = generate_create_table_sql(table_name)
                conn.execute(text(create_sql))
                print(f"✅ 成功初始化表 {table_name}（自动生成表结构）")
            except Exception as e:
                print(f"⚠️  初始化表 {table_name} 失败：{str(e)}")
        conn.commit()
    print("\n✅ 所有表初始化完成！")

# ===================== CSV处理通用函数（无修改） =====================
def archive_csv(table_name, csv_path):
    if not csv_path or not os.path.exists(csv_path):
        return None
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    archive_path = os.path.join(ARCHIVE_DIR, f"{table_name}_{timestamp}.csv")
    pd.read_csv(csv_path, encoding="utf-8").to_csv(archive_path, index=False, encoding="utf-8")
    print(f"📁 CSV存档完成：{archive_path}")
    return archive_path

def preprocess_data(df, table_name):
    """通用数据预处理（适配动态字段）"""
    rules = TABLE_RULES[table_name]
    primary_key = rules["primary_key"]
    date_cols = rules["date_cols"]
    
    # 1. 主键处理
    if primary_key in df.columns:
        if table_name == "game_info":
            df[primary_key] = pd.to_numeric(df[primary_key], errors="coerce").fillna(0).astype(int)
        else:
            df[primary_key] = df[primary_key].fillna("").astype(str).str.strip()
    
    # 2. 日期字段转换
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    
    # 3. 空值替换为None
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
    """通用单表增量更新（适配动态字段）"""
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

    # 读取并预处理CSV
    df = pd.read_csv(csv_path, encoding="utf-8")
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
        if primary_key == "游戏编号":
            csv_pk_list = df[df[primary_key] != 0][primary_key].tolist()
            delete_pk_list = [pk for pk in db_pk_list if pk not in csv_pk_list]
            placeholders = ", ".join([f"{pk}" for pk in delete_pk_list]) if delete_pk_list else ""
        else:
            csv_pk_list = df[df[primary_key] != ""][primary_key].tolist()
            delete_pk_list = [pk for pk in db_pk_list if pk not in csv_pk_list]
            placeholders = ", ".join([f"'{pk}'" for pk in delete_pk_list]) if delete_pk_list else ""
        
        delete_count = len(delete_pk_list)
        if delete_count > 0 and placeholders:
            conn.execute(text(f"DELETE FROM {table_name} WHERE {primary_key} IN ({placeholders})"))
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
            db_row = conn.execute(text(f"SELECT * FROM {table_name} WHERE {primary_key} = %s"), (pk_value,)).fetchone()
            if not db_row:
                continue

            # 动态对比字段（排除自动字段）
            db_dict = dict(zip(db_row.keys(), db_row))
            row_dict = row.to_dict()
            update_sql_parts = []
            update_params = []

            for col in row_dict:
                if col in exclude_cols or col not in db_dict:
                    continue
                # 类型兼容对比
                db_val = db_dict[col]
                csv_val = row_dict[col]
                if isinstance(db_val, str) and isinstance(csv_val, pd.Timestamp):
                    csv_val = csv_val.date()
                if db_val != csv_val:
                    update_sql_parts.append(f"`{col}` = %s")
                    update_params.append(csv_val)

            if update_sql_parts:
                update_sql = f"UPDATE {table_name} SET {', '.join(update_sql_parts)} WHERE {primary_key} = %s"
                update_params.append(pk_value)
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
    """批量同步所有表"""
    print("===== 开始批量同步所有表 =====")
    for table_name in CSV_PATHS.keys():
        print(f"\n📌 同步 {table_name}...")
        incremental_update_single(table_name)
    print("\n✅ 所有表同步完成！")

def drop_all_tables():
    """删除所有表"""
    print("⚠️  警告：将删除所有表，且无法恢复！")
    if input("输入 YES 确认：") != "YES" or input("再次输入 YES 确认：") != "YES":
        print("ℹ️  取消删除")
        return False

    engine = get_mysql_engine()
    with engine.connect() as conn:
        drop_order = ["game_song_rel", "song_author_rel", "game_linkage_rel", "game_info", "song_info", "author_info"]
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
    init_all_tables()

    if len(sys.argv) == 1:
        print("📖 使用说明：")
        print("  1. 同步指定表：python3 脚本名.py [表名]（如 game_info）")
        print("  2. 批量同步所有表：python3 脚本名.py all")
        print("  3. 删除所有表：python3 脚本名.py clear")
        sys.exit(0)

    if sys.argv[1] == "all":
        incremental_update_all()
    elif sys.argv[1] == "clear":
        drop_all_tables()
    else:
        incremental_update_single(sys.argv[1])

if __name__ == "__main__":
    main()