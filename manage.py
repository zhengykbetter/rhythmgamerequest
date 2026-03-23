#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主管理脚本：适配managers目录 + 自动赋权scripts/managers所有脚本
用法不变，仅路径调整
"""
import os
import sys
import subprocess
from datetime import datetime

# ===================== 基础配置 =====================
sys.path.insert(0, os.getcwd())
RED = '\033[0;31m'
GREEN = '\033[0;32m'
YELLOW = '\033[1;33m'
NC = '\033[0m'

# ===================== 读取配置 =====================
def get_full_config():
    """读取所有配置（兼容默认值）"""
    config = {
        # 原有配置
        "PYTHON_EXEC_PATH": "python3",
        "SYNC_SCRIPT": os.path.join(os.getcwd(), "scripts", "sync_csv_from_remote.py"),
        "LOG_DIR": "./logs",
        "CRON_BACKUP_DIR": "./logs",
        "CRON_TASKS": "",
        "CRON_TASK_MARK": "# 节奏游戏项目定时任务",
        "MAIN_REPO_ROOT": os.getcwd(),
        # 新增/调整配置（核心：managers目录路径）
        "CSV_ROOT_DIR": os.path.join(os.getcwd(), "data", "csv"),
        "ARCHIVE_DIR": os.path.join(os.getcwd(), "data", "csv", "archive"),
        "EXTRACT_SONG_SCRIPT": os.path.join(os.getcwd(), "scripts", "extract_song_data.py"),
        "CRON_MANAGE_SCRIPT": os.path.join(os.getcwd(), "managers", "cron_manage.py"),  # 调整路径
        "CSV_MANAGE_SCRIPT": os.path.join(os.getcwd(), "managers", "csv_manage.py"),    # 调整路径
        "DB_CONFIG": {
            "host": "localhost", "port": 3306, "user": "root",
            "password": "", "database": "rhythmgame", "charset": "utf8mb4"
        }
    }
    # 从settings.py覆盖配置
    try:
        from config.settings import (
            PYTHON_EXEC_PATH, MAIN_REPO_ROOT, LOG_DIR, CRON_BACKUP_DIR,
            CRON_TASKS, CRON_TASK_MARK, CSV_ROOT_DIR, ARCHIVE_DIR,
            EXTRACT_SONG_SCRIPT, CRON_MANAGE_SCRIPT, CSV_MANAGE_SCRIPT, DB_CONFIG
        )
        config.update({k: v for k, v in locals().items() if v and k in config})
    except ImportError:
        print(f"{YELLOW}ℹ️  使用默认配置（settings.py读取失败）{NC}")
    # 创建必要目录（包括managers目录）
    for dir_path in [
        config["LOG_DIR"], config["CSV_ROOT_DIR"], config["ARCHIVE_DIR"],
        os.path.join(config["MAIN_REPO_ROOT"], "managers")  # 确保managers目录存在
    ]:
        os.makedirs(dir_path, exist_ok=True)
    return config

# ===================== 核心优化：starter自动赋权scripts + managers目录 =====================
def starter(config):
    """初始化：自动给scripts/ + managers/目录下所有脚本赋予执行权限（仅需运行一次）"""
    print(f"{YELLOW}===== 初始化脚本执行权限（仅需一次）====={NC}")
    
    # 1. 给主脚本manage.py加权限
    os.chmod(__file__, 0o755)
    print(f"{GREEN}✅ 已赋予manage.py执行权限{NC}")
    
    # 2. 自动遍历scripts目录下所有.py脚本，批量赋权
    scripts_dir = os.path.join(config["MAIN_REPO_ROOT"], "scripts")
    if os.path.exists(scripts_dir):
        for file_name in os.listdir(scripts_dir):
            if file_name.endswith(".py"):
                script_path = os.path.join(scripts_dir, file_name)
                os.chmod(script_path, 0o755)
                print(f"{GREEN}✅ 已赋予{script_path}执行权限{NC}")
    else:
        print(f"{YELLOW}ℹ️ scripts目录不存在，跳过子脚本赋权{NC}")
    
    # 3. 自动遍历managers目录下所有.py脚本，批量赋权（核心新增）
    managers_dir = os.path.join(config["MAIN_REPO_ROOT"], "managers")
    if os.path.exists(managers_dir):
        for file_name in os.listdir(managers_dir):
            if file_name.endswith(".py"):
                script_path = os.path.join(managers_dir, file_name)
                os.chmod(script_path, 0o755)
                print(f"{GREEN}✅ 已赋予{script_path}执行权限{NC}")
    else:
        print(f"{YELLOW}ℹ️ managers目录不存在，跳过子脚本赋权{NC}")
    
    # 4. 单独给同步脚本赋权（兼容原有逻辑）
    if os.path.exists(config["SYNC_SCRIPT"]):
        os.chmod(config["SYNC_SCRIPT"], 0o755)
        print(f"{GREEN}✅ 已赋予{config['SYNC_SCRIPT']}执行权限{NC}")
    
    print(f"{GREEN}===== 权限初始化完成（仅需执行一次）====={NC}")

# ===================== 原有功能（路径调整） =====================
def sync_now(config):
    """手动同步CSV（原有功能，无路径修改）"""
    print(f"{YELLOW}===== 开始手动执行CSV同步 ====={NC}")
    if not os.path.exists(config["SYNC_SCRIPT"]):
        print(f"{RED}❌ 同步脚本不存在：{config['SYNC_SCRIPT']}{NC}")
        return False
    result = subprocess.run(
        [config["PYTHON_EXEC_PATH"], config["SYNC_SCRIPT"]],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        log_file = os.path.join(config["LOG_DIR"], f"sync_csv_{datetime.now().strftime('%Y%m%d')}.log")
        print(f"{GREEN}✅ CSV同步完成！日志：{log_file}{NC}")
        return True
    else:
        print(f"{RED}❌ 同步失败：{result.stderr}{NC}")
        return False

# ===================== Cron功能（路径调整：调用managers/cron_manage.py） =====================
def config_cron(config):
    starter(config)  # 确保权限
    # 检查cron_manage.py是否存在
    if not os.path.exists(config["CRON_MANAGE_SCRIPT"]):
        print(f"{RED}❌ cron_manage.py不存在：{config['CRON_MANAGE_SCRIPT']}{NC}")
        return False
    return subprocess.run(
        [config["PYTHON_EXEC_PATH"], config["CRON_MANAGE_SCRIPT"], "config", f"--config={str(config)}"],
        check=True
    ).returncode == 0

def check_cron(config):
    if not os.path.exists(config["CRON_MANAGE_SCRIPT"]):
        print(f"{RED}❌ cron_manage.py不存在：{config['CRON_MANAGE_SCRIPT']}{NC}")
        return False
    return subprocess.run(
        [config["PYTHON_EXEC_PATH"], config["CRON_MANAGE_SCRIPT"], "check", f"--config={str(config)}"],
        check=True
    ).returncode == 0

def cancel_cron(config):
    if not os.path.exists(config["CRON_MANAGE_SCRIPT"]):
        print(f"{RED}❌ cron_manage.py不存在：{config['CRON_MANAGE_SCRIPT']}{NC}")
        return False
    return subprocess.run(
        [config["PYTHON_EXEC_PATH"], config["CRON_MANAGE_SCRIPT"], "cancel", f"--config={str(config)}"],
        check=True
    ).returncode == 0

# ===================== 新增功能（路径调整：调用managers/csv_manage.py） =====================
def clean_old(config):
    """清理旧文件（调用managers/csv_manage.py）"""
    if not os.path.exists(config["CSV_MANAGE_SCRIPT"]):
        print(f"{RED}❌ csv_manage.py不存在：{config['CSV_MANAGE_SCRIPT']}{NC}")
        return False
    return subprocess.run(
        [config["PYTHON_EXEC_PATH"], config["CSV_MANAGE_SCRIPT"], "clean_old", f"--config={str(config)}"],
        check=True
    ).returncode == 0

def extract(config):
    """转换CSV：调用scripts/extract_song_data.py（路径不变）"""
    print(f"{YELLOW}===== 调用extract_song_data.py转换_raw CSV ====={NC}")
    # 检查extract_song_data.py是否存在
    if not os.path.exists(config["EXTRACT_SONG_SCRIPT"]):
        print(f"{RED}❌ extract_song_data.py不存在：{config['EXTRACT_SONG_SCRIPT']}{NC}")
        return False
    # 调用外部的extract_song_data.py
    result = subprocess.run(
        [config["PYTHON_EXEC_PATH"], config["EXTRACT_SONG_SCRIPT"]],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        print(f"{GREEN}✅ extract_song_data.py执行完成：{result.stdout}{NC}")
        return True
    else:
        print(f"{RED}❌ extract_song_data.py执行失败：{result.stderr}{NC}")
        return False

def sync_db(config):
    """同步DB（调用managers/csv_manage.py）"""
    if not os.path.exists(config["CSV_MANAGE_SCRIPT"]):
        print(f"{RED}❌ csv_manage.py不存在：{config['CSV_MANAGE_SCRIPT']}{NC}")
        return False
    return subprocess.run(
        [config["PYTHON_EXEC_PATH"], config["CSV_MANAGE_SCRIPT"], "sync_db", f"--config={str(config)}"],
        check=True
    ).returncode == 0

def auto_run(config):
    """全自动执行：clean→extract→sync_db→sync-now"""
    print(f"{YELLOW}===== 开始全自动执行 ====={NC}")
    try:
        clean_old(config)
        print("-" * 40)
        extract(config)  # 调用extract_song_data.py
        print("-" * 40)
        sync_db(config)
        print("-" * 40)
        sync_now(config)
        print(f"{GREEN}🎉 全自动执行完成！{NC}")
        return True
    except Exception as e:
        print(f"{RED}❌ 全自动执行失败：{e}{NC}")
        return False

# ===================== 帮助信息（无修改） =====================
def show_help():
    print(f"{YELLOW}===== 主项目管理脚本 ====={NC}")
    print("用法：python3 manage.py [命令]")
    print("\n【原有指令】")
    print("  starter       - 初始化脚本执行权限（仅需运行一次）")
    print("  config-cron   - 配置定时任务（从settings.py读取）")
    print("  check-cron    - 检查当前定时任务配置")
    print("  sync-now      - 手动执行CSV同步（拉取远程+复制）")
    print("  cancel-cron   - 取消本项目crontab任务（保留其他）")
    print("\n【新增指令】")
    print("  clean_old     - 清理旧CSV文件（保留_raw源文件）")
    print("  extract       - 调用extract_song_data.py转换_raw CSV")
    print("  sync_db       - 同步CSV数据到MySQL数据库")
    print("  auto          - 全自动执行（clean_old→extract→sync_db→sync-now）")
    print("  help          - 查看帮助信息")

# ===================== 主入口（无修改） =====================
def main():
    if len(sys.argv) < 2:
        show_help()
        sys.exit(1)

    config = get_full_config()
    command = sys.argv[1]
    commands = {
        # 原有指令
        "starter": lambda: starter(config),
        "config-cron": lambda: config_cron(config),
        "check-cron": lambda: check_cron(config),
        "sync-now": lambda: sync_now(config),
        "cancel-cron": lambda: cancel_cron(config),
        # 新增指令
        "clean_old": lambda: clean_old(config),
        "extract": lambda: extract(config),
        "sync_db": lambda: sync_db(config),
        "auto": lambda: auto_run(config),
        "help": show_help
    }

    if command in commands:
        commands[command]()
    else:
        print(f"{RED}❌ 未知命令：{command}{NC}")
        show_help()
        sys.exit(1)

if __name__ == "__main__":
    main()