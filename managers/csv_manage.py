#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV管理子脚本：修复所有缺陷 + 兼容短横线指令 + 保留核心同步/清理逻辑
"""
import os
import sys
import json
import shutil
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from pathlib import Path

# 颜色常量
RED = '\033[0;31m'
GREEN = '\033[0;32m'
YELLOW = '\033[1;33m'
NC = '\033[0m'

# ===================== 前置检查（新增） =====================
def pre_check():
    """检查依赖库和基础配置"""
    # 检查pandas/sqlalchemy
    try:
        import pandas as pd
        from sqlalchemy import create_engine
    except ImportError as e:
        print(f"{RED}❌ 缺少依赖库：{e}，请执行 pip install pandas sqlalchemy pymysql{NC}", file=sys.stderr)
        sys.exit(1)
    
    # 添加项目根目录到Python路径
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    
    # 加载settings（替代外部传config，解决KeyError）
    try:
        from config.settings import (
            CSV_ROOT_DIR, ARCHIVE_DIR, CLEAN_CONFIG,
            DB_CONFIG, TARGET_FILES
        )
        return {
            "CSV_ROOT_DIR": CSV_ROOT_DIR,
            "ARCHIVE_DIR": ARCHIVE_DIR,
            "CLEAN_CONFIG": CLEAN_CONFIG,
            "DB_CONFIG": DB_CONFIG,
            "TARGET_FILES": TARGET_FILES
        }
    except ImportError as e:
        print(f"{RED}❌ 读取settings.py失败：{e}{NC}", file=sys.stderr)
        sys.exit(1)

# ===================== 清理旧文件（修复逻辑错误） =====================
def clean_old_files(config):
    """清理旧文件（保留_raw源文件，修复归档后删除的bug）"""
    print(f"{YELLOW}===== 开始清理旧文件 ====={NC}")
    try:
        expire_days = config["CLEAN_CONFIG"].get("expire_days", 7)
        exclude_suffix = config["CLEAN_CONFIG"].get("exclude_suffix", ["_raw.csv"])
        archive_old = config["CLEAN_CONFIG"].get("archive_old_files", True)
        cutoff_time = datetime.now() - timedelta(days=expire_days)
        cleaned = 0
        archived = 0

        # 检查目录是否存在
        if not os.path.exists(config["CSV_ROOT_DIR"]):
            print(f"{YELLOW}ℹ️ CSV目录不存在：{config['CSV_ROOT_DIR']}{NC}")
            return True

        for file in os.listdir(config["CSV_ROOT_DIR"]):
            file_path = os.path.join(config["CSV_ROOT_DIR"], file)
            # 跳过目录、排除后缀文件
            if os.path.isdir(file_path):
                continue
            if any(file.endswith(suffix) for suffix in exclude_suffix):
                continue

            # 检查文件修改时间
            file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
            if file_mtime < cutoff_time:
                # 修复：归档后不删除（原逻辑错误）
                if archive_old:
                    # 创建归档目录（防止不存在）
                    os.makedirs(config["ARCHIVE_DIR"], exist_ok=True)
                    # 避免文件名重复
                    archive_filename = f"{os.path.splitext(file)[0]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{os.path.splitext(file)[1]}"
                    archive_path = os.path.join(config["ARCHIVE_DIR"], archive_filename)
                    shutil.move(file_path, archive_path)
                    archived += 1
                    print(f"{GREEN}✅ 归档旧文件：{file_path} → {archive_path}{NC}")
                else:
                    os.remove(file_path)
                    cleaned += 1
                    print(f"{GREEN}✅ 删除旧文件：{file_path}{NC}")

        print(f"{GREEN}✅ 清理完成：归档{archived}个，直接删除{cleaned}个{NC}")
        return True
    except Exception as e:
        print(f"{RED}❌ 清理旧文件失败：{str(e)}{NC}", file=sys.stderr)
        return False

# ===================== 同步DB（增强鲁棒性） =====================
def sync_db(config):
    """同步CSV到数据库（新增异常处理、更新逻辑、编码兼容）"""
    print(f"{YELLOW}===== 开始同步CSV到DB ====={NC}")
    try:
        # 创建数据库引擎（新增超时配置）
        engine = create_engine(
            f"mysql+pymysql://{config['DB_CONFIG']['user']}:{config['DB_CONFIG']['password']}@"
            f"{config['DB_CONFIG']['host']}:{config['DB_CONFIG']['port']}/{config['DB_CONFIG']['database']}?charset=utf8mb4",
            connect_args={"connect_timeout": 10}
        )

        # 初始化表结构（保留原逻辑）
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

        # 同步game_info（新增更新逻辑）
        game_csv = os.path.join(config["CSV_ROOT_DIR"], "game_info.csv")
        if os.path.exists(game_csv):
            # 兼容多编码读取
            try:
                df = pd.read_csv(game_csv, encoding="utf-8", dtype=str).fillna("")
            except UnicodeDecodeError:
                df = pd.read_csv(game_csv, encoding="gbk", dtype=str).fillna("")
            
            df["游戏编号"] = pd.to_numeric(df["游戏编号"], errors="coerce").fillna(0).astype(int)
            csv_ids = df[df["游戏编号"] != 0]["游戏编号"].tolist()

            with engine.connect() as conn:
                # 删除库有CSV无的行
                db_ids = [k[0] for k in conn.execute(text("SELECT 游戏编号 FROM game_info")).fetchall()]
                del_ids = [i for i in db_ids if i not in csv_ids and i != 0]
                if del_ids:
                    conn.execute(text(f"DELETE FROM game_info WHERE 游戏编号 IN ({','.join(map(str, del_ids))})"))
                    print(f"{GREEN}✅ 删除game_info {len(del_ids)}条{NC}")

                # 新增/更新行（新增更新逻辑）
                for _, row in df[df["游戏编号"] != 0].iterrows():
                    game_id = row["游戏编号"]
                    # 检查是否存在，存在则更新，不存在则新增
                    exists = conn.execute(text(f"SELECT 1 FROM game_info WHERE 游戏编号 = {game_id}")).fetchone()
                    if exists:
                        # 构造更新语句
                        update_sql = f"""
                            UPDATE game_info SET 
                            游戏 = '{row['游戏']}', 别名 = '{row['别名']}', 实装时间 = '{row['实装时间']}',
                            更新时间 = '{row['更新时间']}', 数据时间 = '{row['数据时间']}', 开服时间 = '{row['开服时间']}'
                            WHERE 游戏编号 = {game_id}
                        """
                        conn.execute(text(update_sql))
                    else:
                        # 新增
                        insert_sql = f"""
                            INSERT INTO game_info (游戏编号, 游戏, 别名, 实装时间, 更新时间, 数据时间, 开服时间)
                            VALUES ({game_id}, '{row['游戏']}', '{row['别名']}', '{row['实装时间']}', 
                                    '{row['更新时间']}', '{row['数据时间']}', '{row['开服时间']}')
                        """
                        conn.execute(text(insert_sql))
                conn.commit()
                print(f"{GREEN}✅ 同步game_info {len(df)}条（含新增/更新）{NC}")

        # 同步其他表（保留原逻辑，新增编码兼容）
        tables = ["song_info", "author_info", "game_song_rel", "song_author_rel", "game_linkage_rel"]
        for table in tables:
            csv_path = os.path.join(config["CSV_ROOT_DIR"], f"{table}.csv")
            if not os.path.exists(csv_path):
                print(f"{YELLOW}ℹ️ {table}.csv不存在，跳过同步{NC}")
                continue
            
            # 兼容多编码读取
            try:
                df = pd.read_csv(csv_path, encoding="utf-8", dtype=str).fillna("")
            except UnicodeDecodeError:
                df = pd.read_csv(csv_path, encoding="gbk", dtype=str).fillna("")
            
            with engine.connect() as conn:
                conn.execute(text(f"TRUNCATE TABLE {table};"))
                df.to_sql(table, engine, if_exists="append", index=False)
                print(f"{GREEN}✅ 同步{table} {len(df)}条{NC}")
                conn.commit()

        print(f"{GREEN}✅ 所有数据同步完成{NC}")
        return True
    except SQLAlchemyError as e:
        print(f"{RED}❌ 数据库操作失败：{str(e)}{NC}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"{RED}❌ 同步DB失败：{str(e)}{NC}", file=sys.stderr)
        return False

# ===================== 主入口（适配短横线指令） =====================
def main():
    """主入口：支持 clean-old/sync-db 短横线指令"""
    # 前置检查 + 加载配置
    config = pre_check()
    
    if len(sys.argv) < 2:
        print(f"{YELLOW}用法：python3 csv_manage.py [clean-old|sync-db]{NC}")
        sys.exit(1)
    
    cmd = sys.argv[1]
    if cmd == "clean-old":
        success = clean_old_files(config)
    elif cmd == "sync-db":
        success = sync_db(config)
    else:
        print(f"{RED}❌ 未知命令：{cmd}，支持命令：clean-old / sync-db{NC}")
        sys.exit(1)
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()