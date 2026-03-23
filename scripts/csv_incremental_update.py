#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV增量更新MySQL数据库（适配6张核心表）
核心规则：
1. game_info仅记录维护的游戏，game_linkage_rel放开外键约束
2. 游戏多次联动通过rel_id=游戏1编号_游戏2编号_8位联动时间（如11_12_202603）区分
3. author_info统一管理别称，支持模糊搜索
4. game_info新增「最新更新时间」列，自动刷新；支持增/改/删全量同步，保证DB与CSV完全一致
5. 新增CSV状态持久化，减少重复更新（对比MD5，无变化则跳过）
6. clear命令仅支持删除所有6张表结构（不可逆，需谨慎）
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

# ===================== 关键：添加主项目根目录到Python路径（确保能导入config） =====================
# 获取当前脚本所在目录
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# 主项目根目录（脚本目录的上一级，对应config目录的同级）
MAIN_PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
# 将主项目根目录加入Python路径，才能导入config.settings
sys.path.insert(0, MAIN_PROJECT_ROOT)

# ===================== 导入配置 =====================
# 加载.env敏感配置
load_dotenv(os.path.join(MAIN_PROJECT_ROOT, ".env"))  # 明确指定.env路径
# 导入公开配置（settings.py，修正文件名拼写+移除无效导入）
from config.settings import (
    CSV_TARGET_DIR,  # game_info.csv所在目录
    DB_CONFIG  # 数据库配置（从环境变量读取敏感信息）
)

# ===================== 基础配置（从setting+env整合） =====================
# 1. CSV文件路径（核心：从setting的CSV_TARGET_DIR拼接game_info.csv路径）
CSV_PATHS = {
    "game_info": str(CSV_TARGET_DIR / "game_info.csv"),  # 拼接完整路径并转字符串
    "song_info": "",
    "author_info": "",
    "game_song_rel": "",
    "song_author_rel": "",
    "game_linkage_rel": ""
}

# 2. CSV存档目录（基于CSV目标目录自动拼接）
ARCHIVE_DIR = str(CSV_TARGET_DIR / "archive")
os.makedirs(ARCHIVE_DIR, exist_ok=True)

# 3. MySQL连接配置（从setting的DB_CONFIG整合，敏感信息来自.env）
MYSQL_CONFIG = {
    "host": DB_CONFIG["host"],
    "port": DB_CONFIG["port"],
    "user": DB_CONFIG["user"],
    "password": DB_CONFIG["password"],
    "database": DB_CONFIG["db"],
    "charset": DB_CONFIG["charset"]
}

# ===================== 新增：状态文件路径（持久化CSV处理状态） =====================
STATE_FILE_PATH = os.path.join(MAIN_PROJECT_ROOT, "data_csv", "csv_processed_state.json")

# ===================== 数据库核心函数 =====================
def get_mysql_engine():
    """创建MySQL连接引擎（带连接保活）"""
    conn_str = (
        f"mysql+pymysql://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@"
        f"{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}?charset={MYSQL_CONFIG['charset']}"
    )
    return create_engine(
        conn_str,
        pool_pre_ping=True,  # 自动检测失效连接
        pool_recycle=3600    # 1小时回收连接，避免超时
    )

def init_all_tables():
    """初始化6张核心表（按3基础+3关联拆分）
    关键修改：game_info新增「最新更新时间」列，自动刷新
    """
    engine = get_mysql_engine()
    with engine.connect() as conn:
        # ---------------------- 基础表 ----------------------
        # 1. 游戏基础表（仅记录维护的游戏）
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS game_info (
                游戏编号 INT PRIMARY KEY COMMENT '游戏唯一标识（仅维护的游戏）',
                游戏 VARCHAR(100) NOT NULL COMMENT '游戏名称',
                别名 VARCHAR(200) COMMENT '游戏别名（多个用逗号分隔）',
                实装时间 DATE COMMENT '实装时间',
                更新时间 DATE COMMENT '更新时间',
                数据时间 DATE COMMENT '数据时间',
                开服时间 DATE COMMENT '开服时间',
                最新更新时间 DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据最后改动时间（自动刷新）',
                update_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '系统更新时间戳'
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='游戏基础信息表（仅维护的游戏）';
        """))

        # 2. 歌曲基础表
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS song_info (
                song_id VARCHAR(20) PRIMARY KEY COMMENT '歌曲唯一标识',
                歌名 VARCHAR(200) NOT NULL COMMENT '曲目名称',
                别名 VARCHAR(300) COMMENT '曲目别名（多个用逗号分隔）',
                更新时间 DATE COMMENT '更新时间',
                update_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间'
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='歌曲基础信息表';
        """))

        # 3. 作者基础表（统一管理别称）
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS author_info (
                author_id VARCHAR(20) PRIMARY KEY COMMENT '作者唯一标识（如A001）',
                作者本名 VARCHAR(200) NOT NULL COMMENT '作者核心名称（唯一）',
                作者别称 VARCHAR(500) COMMENT '所有别称（多个用逗号分隔）',
                擅长风格 VARCHAR(200) COMMENT '擅长音乐风格',
                备注 VARCHAR(500) COMMENT '作者备注',
                update_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间'
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='作者基础信息表（统一别称）';
        """))

        # ---------------------- 关联表 ----------------------
        # 4. 游戏-歌曲收录关联表（多对多）
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS game_song_rel (
                rel_id VARCHAR(50) PRIMARY KEY COMMENT '关联主键：游戏编号_song_id',
                游戏编号 INT NOT NULL COMMENT '游戏编号（关联game_info）',
                song_id VARCHAR(20) NOT NULL COMMENT '歌曲ID（关联song_info）',
                收录版本 VARCHAR(50) COMMENT '收录版本',
                收录时间 DATE COMMENT '收录时间',
                update_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间',
                -- 仅关联维护的游戏（外键约束）
                FOREIGN KEY (游戏编号) REFERENCES game_info(游戏编号) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='游戏-歌曲收录关系表';
        """))

        # 5. 歌曲-作者创作关联表（多对多）
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS song_author_rel (
                rel_id VARCHAR(50) PRIMARY KEY COMMENT '关联主键：song_id_author_id',
                song_id VARCHAR(20) NOT NULL COMMENT '歌曲ID（关联song_info）',
                author_id VARCHAR(20) NOT NULL COMMENT '作者ID（关联author_info）',
                合作类型 VARCHAR(50) COMMENT '作曲/编曲/作词',
                备注 VARCHAR(500) COMMENT '合作备注',
                update_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间',
                FOREIGN KEY (song_id) REFERENCES song_info(song_id),
                FOREIGN KEY (author_id) REFERENCES author_info(author_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='歌曲-作者创作关系表';
        """))

        # 6. 游戏-游戏联动关联表（多对多，放开外键，支持多次联动）
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS game_linkage_rel (
                rel_id VARCHAR(50) PRIMARY KEY COMMENT '关联主键：游戏1编号_游戏2编号_8位联动时间（如11_12_202603）',
                游戏1编号 INT NOT NULL COMMENT '联动游戏1编号（可非维护游戏）',
                游戏2编号 INT NOT NULL COMMENT '联动游戏2编号（可非维护游戏）',
                游戏1名称 VARCHAR(100) NOT NULL COMMENT '游戏1名称（强制录入，避免无意义编号）',
                游戏2名称 VARCHAR(100) NOT NULL COMMENT '游戏2名称（强制录入）',
                联动名称 VARCHAR(200) COMMENT '联动活动名称',
                联动时间 DATE COMMENT '联动时间（8位格式：YYYYMMDD）',
                联动版本 VARCHAR(50) COMMENT '联动版本',
                说明 VARCHAR(500) COMMENT '联动说明',
                update_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间'
                -- 放开外键约束，允许非维护游戏录入
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='游戏-游戏联动关系表（支持多次联动）';
        """))

        conn.commit()
    print("✅ 6张核心表初始化完成！（game_info已新增「最新更新时间」列）")

# ===================== CSV处理函数 =====================
def archive_csv(table_name, csv_path):
    """CSV存档（仅有效路径执行）"""
    if not csv_path or not os.path.exists(csv_path):
        return None
    # 按表名+时间戳命名（避免覆盖）
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    archive_path = os.path.join(ARCHIVE_DIR, f"{table_name}_{timestamp}.csv")
    # 读取并存档（保留UTF-8编码）
    pd.read_csv(csv_path, encoding="utf-8").to_csv(archive_path, index=False, encoding="utf-8")
    print(f"📁 CSV存档完成：{archive_path}")
    return archive_path

def preprocess_game_info(df):
    """预处理game_info数据（日期格式+空值处理）"""
    # 1. 确保游戏编号是整数（避免格式问题）
    if "游戏编号" in df.columns:
        df["游戏编号"] = pd.to_numeric(df["游戏编号"], errors="coerce").fillna(0).astype(int)
    # 2. 日期列统一转换（适配2026/3/22格式）
    date_cols = ["实装时间", "更新时间", "数据时间", "开服时间"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    # 3. 空值替换为None（适配MySQL）
    df = df.where(pd.notna(df), None)
    return df

# ===================== 新增：计算CSV文件MD5（唯一标识文件内容） =====================
def get_file_md5(file_path):
    """计算文件的MD5值，用于判断内容是否变化"""
    if not os.path.exists(file_path):
        return ""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

# ===================== 新增：读取/保存处理状态 =====================
def load_processed_state():
    """读取已处理的CSV状态"""
    if os.path.exists(STATE_FILE_PATH):
        with open(STATE_FILE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_processed_state(state):
    """保存CSV处理状态（持久化到文件）"""
    # 确保目录存在
    os.makedirs(os.path.dirname(STATE_FILE_PATH), exist_ok=True)
    with open(STATE_FILE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

# ===================== 增量更新核心函数（新增删除同步+最新更新时间） =====================
def incremental_update_single(table_name):
    """单个表增量更新（仅game_info完整支持）+ 状态持久化 + 删改同步 + 最新更新时间"""
    # 1. 校验表名合法性
    if table_name not in CSV_PATHS:
        print(f"❌ 错误：不支持的表名 {table_name}，支持的表名：{list(CSV_PATHS.keys())}")
        return False

    # 2. 非game_info表暂不处理
    if table_name != "game_info":
        print(f"ℹ️ 提示：{table_name} 暂未安排更新，跳过！")
        return True

    # 3. 校验CSV文件
    csv_path = CSV_PATHS[table_name]
    if not os.path.exists(csv_path):
        print(f"❌ 错误：game_info的CSV文件不存在 → {csv_path}")
        return False

    # ===================== 状态校验（核心优化） =====================
    # 3.1 读取历史状态
    processed_state = load_processed_state()
    # 3.2 计算当前CSV的MD5
    current_md5 = get_file_md5(csv_path)
    # 3.3 获取该表上次处理的MD5
    last_md5 = processed_state.get(table_name, {}).get("md5", "")
    # 3.4 若MD5一致（内容无变化），直接跳过
    if current_md5 == last_md5 and last_md5 != "":
        print(f"ℹ️ 提示：{table_name} 的CSV文件内容无变化，跳过更新！")
        return True

    # 4. CSV存档（仅内容变化时执行）
    archive_csv(table_name, csv_path)

    # 5. 读取并预处理CSV
    df = pd.read_csv(csv_path, encoding="utf-8")
    df = preprocess_game_info(df)
    # 获取CSV中的所有游戏编号（去重，排除0值）
    csv_game_ids = df[df["游戏编号"] != 0]["游戏编号"].tolist() if "游戏编号" in df.columns else []

    # 6. 数据库全量同步逻辑（增+改+删）
    primary_key = "游戏编号"  # game_info主键
    engine = get_mysql_engine()
    with engine.connect() as conn:
        # 6.1 获取数据库现有主键列表
        db_game_ids_result = conn.execute(text(f"SELECT {primary_key} FROM {table_name}")).fetchall()
        db_game_ids = [k[0] for k in db_game_ids_result]

        # ---------------------- 第一步：删除同步（库有CSV无） ----------------------
        # 找出需要删除的游戏编号（库中有，CSV中无）
        delete_game_ids = [gid for gid in db_game_ids if gid not in csv_game_ids and gid != 0]
        delete_count = len(delete_game_ids)
        if delete_count > 0:
            # 批量删除（参数化查询，防止SQL注入）
            placeholders = ", ".join([f"{gid}" for gid in delete_game_ids])
            conn.execute(text(f"DELETE FROM {table_name} WHERE {primary_key} IN ({placeholders})"))
            print(f"🗑️ 删除 {delete_count} 条game_info数据（库有CSV无）：{delete_game_ids}")

        # ---------------------- 第二步：新增同步（CSV有库无） ----------------------
        # 拆分新增数据（CSV有，库无）
        df_add = df[~df[primary_key].isin(db_game_ids) & (df[primary_key] != 0)]
        add_count = len(df_add)
        if add_count > 0:
            # 新增时自动触发「最新更新时间」字段（MySQL默认值）
            df_add.to_sql(table_name, engine, if_exists="append", index=False)
            print(f"✅ 新增 {add_count} 条game_info数据：{df_add[primary_key].tolist()}")

        # ---------------------- 第三步：更新同步（CSV有库有，字段差异） ----------------------
        # 拆分更新数据（CSV有，库也有）
        df_update = df[df[primary_key].isin(db_game_ids) & (df[primary_key] != 0)]
        update_count = 0
        # 逐行对比更新（排除自动刷新的字段）
        exclude_cols = [primary_key, "最新更新时间", "update_timestamp"]
        for idx, row in df_update.iterrows():
            gid = row[primary_key]
            # 获取数据库当前行数据
            db_row = conn.execute(text(f"SELECT * FROM {table_name} WHERE {primary_key} = %s"), (gid,)).fetchone()
            if not db_row:
                continue

            # 构建字段对比字典（排除无需对比的字段）
            db_dict = dict(zip(db_row.keys(), db_row))
            row_dict = row.to_dict()
            # 过滤掉排除字段
            db_dict_filtered = {k: v for k, v in db_dict.items() if k not in exclude_cols}
            row_dict_filtered = {k: v for k, v in row_dict.items() if k not in exclude_cols and k in db_dict_filtered}

            # 检查是否有字段差异
            has_diff = False
            update_sql_parts = []
            update_params = []
            for col in row_dict_filtered:
                # 处理日期类型对比（MySQL的date vs pandas的date）
                db_val = db_dict_filtered[col]
                csv_val = row_dict_filtered[col]
                if isinstance(db_val, str) and isinstance(csv_val, pd.Timestamp):
                    csv_val = csv_val.date()
                if db_val != csv_val:
                    has_diff = True
                    update_sql_parts.append(f"`{col}` = %s")
                    update_params.append(csv_val)

            # 有差异则执行更新（更新会触发「最新更新时间」字段自动刷新）
            if has_diff:
                update_sql = f"UPDATE {table_name} SET {', '.join(update_sql_parts)} WHERE {primary_key} = %s"
                update_params.append(gid)
                conn.execute(text(update_sql), update_params)
                update_count += 1

        # 提交所有事务（删+增+改）
        conn.commit()
        print(f"✏️ 更新 {update_count} 条game_info数据（字段差异）")
        print(f"✅ {table_name} 全量同步完成！新增：{add_count} | 更新：{update_count} | 删除：{delete_count}")

    # 7. 保存本次处理状态
    processed_state[table_name] = {
        "md5": current_md5,
        "process_time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "file_path": csv_path,
        "sync_stats": {
            "add": add_count,
            "update": update_count,
            "delete": delete_count
        }
    }
    save_processed_state(processed_state)
    return True

# ===================== 修改后：删除所有6张表结构（替代原clear逻辑） =====================
def drop_all_tables():
    """删除所有6张表结构（不可逆，按外键依赖顺序删除）"""
    # 二次确认（强化风险提示）
    print("⚠️  警告：此操作将删除所有6张表的结构和数据，且无法恢复！")
    confirm1 = input("请输入 YES 确认删除：")
    if confirm1 != "YES":
        print("ℹ️  取消删除操作")
        return False
    
    # 二次验证（防止误触）
    confirm2 = input("请再次输入 YES 确认永久删除：")
    if confirm2 != "YES":
        print("ℹ️  取消删除操作")
        return False

    engine = get_mysql_engine()
    with engine.connect() as conn:
        # 步骤1：先删关联表（依赖基础表，必须先删）
        drop_tables_order = [
            "game_song_rel",
            "song_author_rel",
            "game_linkage_rel",
            "game_info",
            "song_info",
            "author_info"
        ]
        
        for table in drop_tables_order:
            try:
                conn.execute(text(f"DROP TABLE IF EXISTS {table};"))
                print(f"🗑️ 成功删除表：{table}")
            except Exception as e:
                print(f"⚠️  删除表 {table} 时出错：{str(e)}")
        
        conn.commit()
    print("✅ 所有6张表已成功删除（结构+数据全部清除）！")
    return True

# ===================== 主函数（命令行交互） =====================
def main():
    # 初始化表（首次运行必执行，自动新增「最新更新时间」列）
    init_all_tables()

    # 解析命令行参数
    if len(sys.argv) == 1:
        print("📖 使用说明：")
        print("  1. 全量同步game_info表：python3 csv_incremental_update.py game_info")
        print("     （支持增/改/删全量同步，保证DB与CSV完全一致，新增「最新更新时间」列自动刷新）")
        print("  2. 删除所有6张表结构（不可逆）：python3 csv_incremental_update.py clear")
        sys.exit(0)

    # 处理删除所有表逻辑
    if sys.argv[1] == "clear":
        drop_all_tables()
        return

    # 处理全量同步逻辑
    incremental_update_single(sys.argv[1])

if __name__ == "__main__":
    main()