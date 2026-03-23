#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV增量更新MySQL数据库（适配6张核心表）
核心规则：
1. game_info仅记录维护的游戏，game_linkage_rel放开外键约束
2. 游戏多次联动通过rel_id=游戏1编号_游戏2编号_联动时间（8位日期）区分
3. author_info统一管理作者别称，支持模糊搜索
4. 仅game_info完整支持增量更新，其他表暂留空（后续拓展）
配置来源：
- 非敏感路径：/config/setting.py
- 敏感信息：.env 文件（git忽略）
"""
import os
import sys
import time
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ===================== 关键：添加主项目根目录到Python路径（确保能导入config） =====================
# 获取当前脚本所在目录
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# 主项目根目录（脚本目录的上一级，对应config目录的同级）
MAIN_PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
# 将主项目根目录加入Python路径，才能导入config.setting
sys.path.insert(0, MAIN_PROJECT_ROOT)

# ===================== 导入配置 =====================
# 加载.env敏感配置
load_dotenv(os.path.join(MAIN_PROJECT_ROOT, ".env"))  # 明确指定.env路径
# 导入公开配置（settings.py）
from config.settings import (
    CSV_TARGET_DIR,  # game_info.csv所在目录
    ARCHIVE_DIR as SETTING_ARCHIVE_DIR,  # 存档目录（若setting里有，否则用下面的）
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

# 2. CSV存档目录（优先用setting里的，没有则用默认）
ARCHIVE_DIR = SETTING_ARCHIVE_DIR if 'SETTING_ARCHIVE_DIR' in locals() else str(CSV_TARGET_DIR / "archive")
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
    """初始化6张核心表（按3基础+3关联拆分）"""
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
                update_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间'
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
                FOREIGN KEY (游戏编号) REFERENCES game_info(游戏编号)
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
    print("✅ 6张核心表初始化完成！")

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
    # 1. 日期列统一转换（适配2026/3/22格式）
    date_cols = ["实装时间", "更新时间", "数据时间", "开服时间"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    # 2. 空值替换为None（适配MySQL）
    df = df.where(pd.notna(df), None)
    return df

# ===================== 增量更新核心函数 =====================
def incremental_update_single(table_name):
    """单个表增量更新（仅game_info完整支持）"""
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

    # 4. CSV存档
    archive_csv(table_name, csv_path)

    # 5. 读取并预处理CSV
    df = pd.read_csv(csv_path, encoding="utf-8")
    df = preprocess_game_info(df)

    # 6. 数据库增量更新逻辑
    primary_key = "游戏编号"  # game_info主键
    engine = get_mysql_engine()
    with engine.connect() as conn:
        # 6.1 获取数据库现有主键列表
        existing_keys = conn.execute(text(f"SELECT {primary_key} FROM {table_name}")).fetchall()
        existing_keys = [k[0] for k in existing_keys]

        # 6.2 拆分新增/更新数据
        df_add = df[~df[primary_key].isin(existing_keys)]  # 新增数据
        df_update = df[df[primary_key].isin(existing_keys)]  # 更新数据

        # 6.3 执行新增
        add_count = len(df_add)
        if add_count > 0:
            df_add.to_sql(table_name, engine, if_exists="append", index=False)
            print(f"✅ 新增 {add_count} 条 game_info 数据")

        # 6.4 执行更新（仅更新差异数据）
        update_count = 0
        for idx, row in df_update.iterrows():
            key = row[primary_key]
            # 获取数据库现有数据
            db_row = conn.execute(text(f"SELECT * FROM {table_name} WHERE {primary_key} = %s"), (key,)).fetchone()
            if not db_row:
                continue

            # 对比数据（排除自动更新的时间戳字段）
            db_dict = dict(zip(db_row.keys(), db_row))
            db_dict.pop("update_timestamp", None)
            row_dict = row.to_dict()
            row_dict.pop("update_timestamp", None) if "update_timestamp" in row_dict else None

            # 内容不同则更新
            if db_dict != row_dict:
                update_cols = [f"`{col}` = %s" for col in row_dict.keys() if col != primary_key]
                update_sql = f"UPDATE {table_name} SET {','.join(update_cols)} WHERE {primary_key} = %s"
                params = [row_dict[col] for col in row_dict.keys() if col != primary_key] + [key]
                conn.execute(text(update_sql), params)
                update_count += 1

        conn.commit()
        print(f"✅ 更新 {update_count} 条 game_info 数据")
        print(f"✅ {table_name} 增量更新完成！")
    return True

# ===================== 清空表函数（测试用） =====================
def clear_table(table_name=None):
    """清空指定表/所有表（谨慎执行）"""
    # 1. 清空单个表
    if table_name:
        # 校验表名
        if table_name not in CSV_PATHS:
            print(f"❌ 错误：不支持的表名 {table_name}")
            return False
        # 二次确认
        confirm = input(f"⚠️  确认清空 {table_name} 表？输入 YES 确认：")
        if confirm != "YES":
            print("ℹ️  取消清空操作")
            return False
        # 执行清空
        engine = get_mysql_engine()
        with engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table_name};"))
            conn.commit()
        print(f"✅ {table_name} 表已清空！")
        return True

    # 2. 清空所有表（按外键依赖顺序）
    confirm = input(f"⚠️  确认清空数据库所有6张表？输入 YES 确认：")
    if confirm != "YES":
        print("ℹ️  取消清空操作")
        return False
    engine = get_mysql_engine()
    with engine.connect() as conn:
        # 先清空关联表（避免外键约束报错）
        conn.execute(text("TRUNCATE TABLE game_song_rel;"))
        conn.execute(text("TRUNCATE TABLE song_author_rel;"))
        conn.execute(text("TRUNCATE TABLE game_linkage_rel;"))
        # 再清空基础表
        conn.execute(text("TRUNCATE TABLE game_info;"))
        conn.execute(text("TRUNCATE TABLE song_info;"))
        conn.execute(text("TRUNCATE TABLE author_info;"))
        conn.commit()
    print("✅ 数据库所有6张表已清空！")
    return True

# ===================== 主函数（命令行交互） =====================
def main():
    # 初始化表（首次运行必执行）
    init_all_tables()

    # 解析命令行参数
    if len(sys.argv) == 1:
        print("📖 使用说明：")
        print("  1. 更新单个表：python3 csv_incremental_update.py [表名] （示例：python3 csv_incremental_update.py game_info）")
        print("  2. 清空单个表：python3 csv_incremental_update.py clear [表名] （示例：python3 csv_incremental_update.py clear game_info）")
        print("  3. 清空所有表：python3 csv_incremental_update.py clear all")
        sys.exit(0)

    # 处理清空逻辑
    if sys.argv[1] == "clear":
        if len(sys.argv) == 3:
            if sys.argv[2] == "all":
                clear_table()  # 清空所有表
            else:
                clear_table(sys.argv[2])  # 清空单个表
        else:
            print("❌ 错误：清空表需指定表名或 all，示例：clear game_info 或 clear all")
        return

    # 处理更新逻辑
    incremental_update_single(sys.argv[1])

if __name__ == "__main__":
    main()