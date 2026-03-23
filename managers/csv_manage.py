#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV管理子脚本：仅保留清理旧文件 + 同步DB（extract逻辑移到extract_song_data.py）
"""
import os
import sys
import shutil
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

# 颜色常量
RED = '\033[0;31m'
GREEN = '\033[0;32m'
YELLOW = '\033[1;33m'
NC = '\033[0m'

def clean_old_files(config):
    """清理旧文件（保留_raw源文件）"""
    print(f"{YELLOW}===== 开始清理旧文件 ====={NC}")
    expire_days = 7
    expire_time = datetime.now() - timedelta(days=expire_days)
    cleaned = 0
    archived = 0

    # 遍历CSV目录
    for file in os.listdir(config["CSV_ROOT_DIR"]):
        file_path = os.path.join(config["CSV_ROOT_DIR"], file)
        if os.path.isdir(file_path) or file.endswith("_raw.csv"):
            continue

        # 检查文件过期
        mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
        if mtime < expire_time:
            # 归档
            archive_path = os.path.join(config["ARCHIVE_DIR"], f"{os.path.splitext(file)[0]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{os.path.splitext(file)[1]}")
            shutil.move(file_path, archive_path)
            archived += 1
            # 删除
            os.remove(archive_path) if os.path.exists(archive_path) else None
            cleaned += 1

    print(f"{GREEN}✅ 清理完成：归档{archived}个，删除{cleaned}个{NC}")
    return True

def sync_db(config):
    """同步CSV到数据库"""
    print(f"{YELLOW}===== 开始同步CSV到DB ====={NC}")
    # 创建数据库引擎
    engine = create_engine(
        f"mysql+pymysql://{config['DB_CONFIG']['user']}:{config['DB_CONFIG']['password']}@"
        f"{config['DB_CONFIG']['host']}:{config['DB_CONFIG']['port']}/{config['DB_CONFIG']['database']}?charset=utf8mb4"
    )

    # 初始化表结构
    with engine.connect() as conn:
        # game_info表
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS game_info (
                游戏编号 INT PRIMARY KEY, 游戏 VARCHAR(100) NOT NULL, 别名 VARCHAR(200),
                实装时间 DATE, 更新时间 DATE, 数据时间 DATE, 开服时间 DATE,
                最新更新时间 DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                update_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """))
        # song_info表
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS song_info (
                song_id VARCHAR(20) PRIMARY KEY, 歌名 VARCHAR(200) NOT NULL,
                别名 VARCHAR(300), 更新时间 DATE,
                update_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """))
        # author_info表
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS author_info (
                author_id VARCHAR(20) PRIMARY KEY, 作者本名 VARCHAR(200) NOT NULL,
                作者别称 VARCHAR(500), 擅长风格 VARCHAR(200), 备注 VARCHAR(500),
                update_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """))
        # game_song_rel表
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS game_song_rel (
                rel_id VARCHAR(50) PRIMARY KEY, 游戏编号 INT NOT NULL,
                song_id VARCHAR(20) NOT NULL, 收录版本 VARCHAR(50), 收录时间 DATE,
                update_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (游戏编号) REFERENCES game_info(游戏编号) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """))
        # song_author_rel表
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS song_author_rel (
                rel_id VARCHAR(50) PRIMARY KEY, song_id VARCHAR(20) NOT NULL,
                author_id VARCHAR(20) NOT NULL, 合作类型 VARCHAR(50), 备注 VARCHAR(500),
                update_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (song_id) REFERENCES song_info(song_id),
                FOREIGN KEY (author_id) REFERENCES author_info(author_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """))
        # game_linkage_rel表
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS game_linkage_rel (
                rel_id VARCHAR(50) PRIMARY KEY, 游戏1编号 INT NOT NULL,
                游戏2编号 INT NOT NULL, 游戏1名称 VARCHAR(100), 游戏2名称 VARCHAR(100) NOT NULL,
                联动名称 VARCHAR(200), 联动时间 DATE, 联动版本 VARCHAR(50), 说明 VARCHAR(500),
                update_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """))
        conn.commit()

    # 同步game_info（增删改）
    game_csv = os.path.join(config["CSV_ROOT_DIR"], "game_info.csv")
    if os.path.exists(game_csv):
        df = pd.read_csv(game_csv, encoding="utf-8", dtype=str).fillna("")
        df["游戏编号"] = pd.to_numeric(df["游戏编号"], errors="coerce").fillna(0).astype(int)
        csv_ids = df[df["游戏编号"] != 0]["游戏编号"].tolist()

        with engine.connect() as conn:
            # 删除库有CSV无的行
            db_ids = [k[0] for k in conn.execute(text("SELECT 游戏编号 FROM game_info")).fetchall()]
            del_ids = [i for i in db_ids if i not in csv_ids and i != 0]
            if del_ids:
                conn.execute(text(f"DELETE FROM game_info WHERE 游戏编号 IN ({','.join(map(str, del_ids))})"))
                print(f"{GREEN}✅ 删除game_info {len(del_ids)}条{NC}")

            # 新增行
            df_add = df[~df["游戏编号"].isin(db_ids) & (df["游戏编号"] != 0)]
            if len(df_add) > 0:
                df_add.to_sql("game_info", engine, if_exists="append", index=False)
                print(f"{GREEN}✅ 新增game_info {len(df_add)}条{NC}")
            conn.commit()

    # 同步其他表（全量覆盖）
    tables = ["song_info", "author_info", "game_song_rel", "song_author_rel", "game_linkage_rel"]
    for table in tables:
        csv_path = os.path.join(config["CSV_ROOT_DIR"], f"{table}.csv")
        if not os.path.exists(csv_path):
            print(f"{YELLOW}ℹ️ {table}.csv不存在，跳过同步{NC}")
            continue
        df = pd.read_csv(csv_path, encoding="utf-8", dtype=str).fillna("")
        with engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table};"))
            df.to_sql(table, engine, if_exists="append", index=False)
            print(f"{GREEN}✅ 同步{table} {len(df)}条{NC}")
            conn.commit()

    print(f"{GREEN}✅ 所有数据同步完成{NC}")
    return True

if __name__ == "__main__":
    # 被manage.py调用，参数格式：python3 csv_manage.py [clean_old/sync_db] --config '{...}'
    if len(sys.argv) < 2:
        print("用法：python3 csv_manage.py [clean_old/sync_db] --config '{...}'")
        sys.exit(1)
    cmd = sys.argv[1]
    config = eval(sys.argv[2].replace('--config=', '')) if len(sys.argv) > 2 else {}
    if cmd == "clean_old":
        clean_old_files(config)
    elif cmd == "sync_db":
        sync_db(config)
    else:
        print(f"{RED}❌ 未知命令：{cmd}{NC}")
        sys.exit(1)