#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV管理子脚本：修复所有缺陷 + 兼容短横线指令 + 保留核心同步/清理逻辑 + 新增Git同步功能
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

# 新增：Git仓库操作依赖
try:
    from git import Repo
    from git.exc import GitCommandError, InvalidGitRepositoryError, NoSuchPathError
except ImportError:
    pass

# 颜色常量
RED = '\033[0;31m'
GREEN = '\033[0;32m'
YELLOW = '\033[1;33m'
NC = '\033[0m'

# ===================== 前置检查（新增Git配置加载） =====================
def pre_check():
    """检查依赖库和基础配置"""
    # 检查基础依赖库
    try:
        import pandas as pd
        from sqlalchemy import create_engine
    except ImportError as e:
        print(f"{RED}❌ 缺少基础依赖库：{e}，请执行 pip install pandas sqlalchemy pymysql{NC}", file=sys.stderr)
        sys.exit(1)
    
    # 检查Git依赖（sync-git指令需要）
    if len(sys.argv) >= 2 and sys.argv[1] == "sync-git":
        try:
            from git import Repo
        except ImportError:
            print(f"{RED}❌ 缺少Git依赖库：gitpython，请执行 pip install gitpython{NC}", file=sys.stderr)
            sys.exit(1)
    
    # 添加项目根目录到Python路径
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    
    # 加载settings（新增Git相关配置）
    try:
        from config.settings import (
            CSV_ROOT_DIR, ARCHIVE_DIR, CLEAN_CONFIG,
            DB_CONFIG, TARGET_FILES,
            CSV_REPO_URL, CSV_REPO_BRANCH, CSV_REPO_LOCAL_PATH,
            PRIVATE_CSV_REPO_ROOT, REQUIRED_CSV_FILES, CSV_SOURCE_DIR
        )
        return {
            "CSV_ROOT_DIR": CSV_ROOT_DIR,
            "ARCHIVE_DIR": ARCHIVE_DIR,
            "CLEAN_CONFIG": CLEAN_CONFIG,
            "DB_CONFIG": DB_CONFIG,
            "TARGET_FILES": TARGET_FILES,
            # Git同步相关配置
            "CSV_REPO_URL": CSV_REPO_URL,
            "CSV_REPO_BRANCH": CSV_REPO_BRANCH,
            "CSV_REPO_LOCAL_PATH": CSV_REPO_LOCAL_PATH,
            "PRIVATE_CSV_REPO_ROOT": PRIVATE_CSV_REPO_ROOT,
            "REQUIRED_CSV_FILES": REQUIRED_CSV_FILES,
            "CSV_SOURCE_DIR": CSV_SOURCE_DIR
        }
    except ImportError as e:
        print(f"{RED}❌ 读取settings.py失败：{e}{NC}", file=sys.stderr)
        sys.exit(1)

# ===================== Git同步（新增核心功能） =====================
def sync_git(config):
    """
    同步GitHub仓库：
    1. 克隆/拉取仓库到/opt/csv_repo
    2. 拷贝result目录下的文件到项目CSV_SOURCE_DIR
    """
    print(f"{YELLOW}===== 开始同步GitHub仓库 ====={NC}")
    try:
        # 1. 确保仓库本地目录存在且有权限
        repo_path = Path(config["CSV_REPO_LOCAL_PATH"])
        os.makedirs(repo_path, exist_ok=True)
        if not os.access(repo_path, os.W_OK):
            print(f"{RED}❌ 无写入权限：{repo_path}，请检查目录权限{NC}", file=sys.stderr)
            return False
        
        # 2. 克隆/拉取GitHub仓库
        try:
            if repo_path.joinpath(".git").exists():
                # 仓库已存在，拉取最新代码
                repo = Repo(str(repo_path))
                origin = repo.remote(name="origin")
                # 拉取前先检查远程分支
                origin.fetch()
                origin.pull(config["CSV_REPO_BRANCH"])
                print(f"{GREEN}✅ 成功拉取仓库最新代码：{config['CSV_REPO_URL']}（{config['CSV_REPO_BRANCH']}分支）{NC}")
            else:
                # 仓库不存在，克隆
                Repo.clone_from(
                    config["CSV_REPO_URL"],
                    str(repo_path),
                    branch=config["CSV_REPO_BRANCH"]
                )
                print(f"{GREEN}✅ 成功克隆仓库：{config['CSV_REPO_URL']} → {repo_path}{NC}")
        except GitCommandError as e:
            print(f"{RED}❌ Git命令执行失败：{str(e)}（请检查仓库地址/分支/网络）{NC}", file=sys.stderr)
            return False
        except InvalidGitRepositoryError as e:
            print(f"{RED}❌ 无效的Git仓库：{str(e)}{NC}", file=sys.stderr)
            return False
        except NoSuchPathError as e:
            print(f"{RED}❌ 仓库路径不存在：{str(e)}{NC}", file=sys.stderr)
            return False
        
        # 3. 定位仓库内的CSV目录（/opt/csv_repo/result）
        repo_csv_dir = repo_path / config["PRIVATE_CSV_REPO_ROOT"]
        if not repo_csv_dir.exists():
            print(f"{RED}❌ 仓库内CSV目录不存在：{repo_csv_dir}{NC}", file=sys.stderr)
            return False
        
        # 4. 拷贝指定文件到项目CSV_SOURCE_DIR
        os.makedirs(config["CSV_SOURCE_DIR"], exist_ok=True)
        success_count = 0
        fail_count = 0
        
        for csv_file in config["REQUIRED_CSV_FILES"]:
            # 仓库内的源文件路径
            src_file = repo_csv_dir / csv_file
            # 项目内的目标文件路径
            dst_file = Path(config["CSV_SOURCE_DIR"]) / csv_file
            
            if not src_file.exists():
                print(f"{YELLOW}ℹ️ 仓库内文件缺失：{src_file}，跳过{NC}")
                fail_count += 1
                continue
            
            try:
                # 拷贝文件（覆盖已有文件）
                shutil.copy2(str(src_file), str(dst_file))
                print(f"{GREEN}✅ 拷贝成功：{src_file} → {dst_file}{NC}")
                success_count += 1
            except Exception as e:
                print(f"{RED}❌ 拷贝失败：{csv_file}，原因：{str(e)}{NC}")
                fail_count += 1
        
        # 5. 输出汇总
        print(f"{GREEN}✅ Git同步汇总：成功{success_count}个，失败{fail_count}个{NC}")
        if fail_count > 0 and success_count == 0:
            return False
        
        return True
    except Exception as e:
        print(f"{RED}❌ Git同步整体失败：{str(e)}{NC}", file=sys.stderr)
        return False

# ===================== 清理旧文件（保留原有修复逻辑） =====================
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

# ===================== 同步DB（保留原有增强逻辑） =====================
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

# ===================== 主入口（新增sync-git指令） =====================
def main():
    """主入口：支持 clean-old/sync-db/sync-git 短横线指令"""
    # 前置检查 + 加载配置
    config = pre_check()
    
    if len(sys.argv) < 2:
        print(f"{YELLOW}用法：python3 csv_manage.py [clean-old|sync-db|sync-git]{NC}")
        sys.exit(1)
    
    cmd = sys.argv[1]
    if cmd == "clean-old":
        success = clean_old_files(config)
    elif cmd == "sync-db":
        success = sync_db(config)
    elif cmd == "sync-git":  # 新增Git同步指令
        success = sync_git(config)
    else:
        print(f"{RED}❌ 未知命令：{cmd}，支持命令：clean-old / sync-db / sync-git{NC}")
        sys.exit(1)
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()