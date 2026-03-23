#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主管理脚本：完整功能版
核心指令：
  1. 权限/初始化：starter
  2. Cron管理：config-cron/check-cron/cancel-cron/clear-all-cron（清除所有）
  3. CSV管理：clean_old/extract/sync_db/sync-now
  4. 全自动：auto
  5. 帮助：help

关键特性：
  - CRON默认配置为「每天凌晨2点执行auto」
  - clear-all-cron命令带强制警告+备份，防止误删
  - 所有路径基于项目根目录，无硬编码
"""
import os
import sys
import subprocess
import getpass
from datetime import datetime

# ===================== 基础配置 & 颜色常量 =====================
sys.path.insert(0, os.getcwd())
# 颜色输出（醒目提示）
RED = '\033[0;31m'
GREEN = '\033[0;32m'
YELLOW = '\033[1;33m'
BOLD_RED = '\033[1;31m'
NC = '\033[0m'  # 重置颜色

# ===================== 核心工具函数 =====================
def run_shell_cmd(cmd, capture_output=False):
    """执行shell命令，返回(输出, 错误, 退出码)"""
    if capture_output:
        # 优化：使用 list 传递参数，避免sh解析错误
        if isinstance(cmd, str):
            cmd = cmd.split()
        result = subprocess.run(
            cmd, shell=False, capture_output=True, text=True, 
            executable="/bin/bash"  # 指定bash解析，避免sh的语法兼容问题
        )
        return result.stdout, result.stderr, result.returncode
    else:
        if isinstance(cmd, str):
            cmd = cmd.split()
        subprocess.run(
            cmd, shell=False, 
            executable="/bin/bash"
        )
        return "", "", 0

def get_full_config():
    """读取settings.py配置（兼容默认值，容错单个变量缺失）"""
    # 基础默认配置（已有）
    default_config = {
        "PYTHON_EXEC_PATH": "python3",
        "MAIN_REPO_ROOT": os.getcwd(),
        "LOG_DIR": os.path.join(os.getcwd(), "logs"),
        "CRON_BACKUP_DIR": os.path.join(os.getcwd(), "logs"),
        "CRON_TASK_MARK": "# 节奏游戏项目定时任务",
        "CRON_TASKS": [
            f"0 2 * * * python3 {os.path.join(os.getcwd(), 'manage.py')} auto > {os.path.join(os.getcwd(), 'logs', 'auto_cron.log')} 2>&1"
        ],
        "SYNC_SCRIPT": os.path.join(os.getcwd(), "scripts", "sync_csv_from_remote.py"),
        "EXTRACT_SONG_SCRIPT": os.path.join(os.getcwd(), "scripts", "extract_song_data.py"),
        "CRON_MANAGE_SCRIPT": os.path.join(os.getcwd(), "managers", "cron_manage.py"),
        "CSV_MANAGE_SCRIPT": os.path.join(os.getcwd(), "managers", "csv_manage.py"),
        "CSV_ROOT_DIR": os.path.join(os.getcwd(), "data", "csv"),
        "ARCHIVE_DIR": os.path.join(os.getcwd(), "data", "csv", "archive"),
        "DB_CONFIG": {
            "host": "localhost", "port": 3306, "user": "root",
            "password": "", "database": "rhythmgame", "charset": "utf8mb4"
        }
    }

    # ========== 优化：逐个导入变量，容错缺失 ==========
    try:
        from config.settings import *  # 导入所有变量
        # 仅覆盖默认配置中存在、且settings.py中已定义的变量
        for key in default_config.keys():
            if key in locals() and locals()[key] is not None:
                default_config[key] = locals()[key]
        print(f"{GREEN}✅ 成功读取settings.py配置{NC}")
    except ImportError as e:
        # 仅打印警告，使用默认配置
        print(f"{YELLOW}ℹ️  读取settings.py部分变量失败（{e}），使用默认配置{NC}")
    except Exception as e:
        print(f"{YELLOW}ℹ️  settings.py读取异常（{e}），使用默认配置{NC}")

    # 创建必要目录（已有）
    for dir_path in [
        default_config["LOG_DIR"], default_config["CRON_BACKUP_DIR"],
        default_config["CSV_ROOT_DIR"], default_config["ARCHIVE_DIR"],
        os.path.dirname(default_config["CRON_MANAGE_SCRIPT"]),
        os.path.dirname(default_config["CSV_MANAGE_SCRIPT"])
    ]:
        os.makedirs(dir_path, exist_ok=True)

    return default_config

# ===================== 1. 初始化权限（仅需执行一次） =====================
def starter(config):
    """自动给scripts/managers目录下所有脚本赋权"""
    print(f"{YELLOW}===== 初始化脚本执行权限（仅需一次）====={NC}")
    
    # 给主脚本赋权
    os.chmod(__file__, 0o755)
    print(f"{GREEN}✅ 已赋予{__file__}执行权限{NC}")
    
    # 批量赋权：scripts目录
    scripts_dir = os.path.join(config["MAIN_REPO_ROOT"], "scripts")
    if os.path.exists(scripts_dir):
        for fname in os.listdir(scripts_dir):
            if fname.endswith(".py"):
                fpath = os.path.join(scripts_dir, fname)
                os.chmod(fpath, 0o755)
                print(f"{GREEN}✅ 已赋予{fpath}执行权限{NC}")
    
    # 批量赋权：managers目录
    managers_dir = os.path.join(config["MAIN_REPO_ROOT"], "managers")
    if os.path.exists(managers_dir):
        for fname in os.listdir(managers_dir):
            if fname.endswith(".py"):
                fpath = os.path.join(managers_dir, fname)
                os.chmod(fpath, 0o755)
                print(f"{GREEN}✅ 已赋予{fpath}执行权限{NC}")
    
    print(f"{GREEN}===== 权限初始化完成 ====={NC}")

# ===================== 2. Cron核心管理（含清除所有Cron） =====================
def config_cron(config):
    """配置Cron（默认每天2点执行auto）"""
    starter(config)
    if not os.path.exists(config["CRON_MANAGE_SCRIPT"]):
        print(f"{RED}❌ cron_manage.py不存在：{config['CRON_MANAGE_SCRIPT']}{NC}")
        return False
    # 调用managers/cron_manage.py配置
    return run_shell_cmd(
        f"{config['PYTHON_EXEC_PATH']} {config['CRON_MANAGE_SCRIPT']} config --config='{str(config)}'",
        check=True
    )[2] == 0

def check_cron(config):
    """检查当前Cron配置"""
    if not os.path.exists(config["CRON_MANAGE_SCRIPT"]):
        print(f"{RED}❌ cron_manage.py不存在：{config['CRON_MANAGE_SCRIPT']}{NC}")
        return False
    run_shell_cmd(f"{config['PYTHON_EXEC_PATH']} {config['CRON_MANAGE_SCRIPT']} check --config='{str(config)}'")
    return True

def cancel_cron(config):
    """仅清除本项目的Cron任务（保留其他）"""
    if not os.path.exists(config["CRON_MANAGE_SCRIPT"]):
        print(f"{RED}❌ cron_manage.py不存在：{config['CRON_MANAGE_SCRIPT']}{NC}")
        return False
    return run_shell_cmd(
        f"{config['PYTHON_EXEC_PATH']} {config['CRON_MANAGE_SCRIPT']} cancel --config='{str(config)}'",
        check=True
    )[2] == 0

def clear_all_cron(config):
    """⚠️ 强制清除服务器上所有Cron任务（带警告+备份）⚠️"""
    # 第一步：强制警告（醒目红色）
    print(f"{BOLD_RED}===== 危险操作警告 ====={NC}")
    print(f"{BOLD_RED}此命令将删除当前用户（{getpass.getuser()}）的所有Cron任务！{NC}")
    print(f"{BOLD_RED}包括非本项目的所有定时任务，且无法恢复（除了备份）！{NC}")
    print(f"{YELLOW}===== 备份提示 ====={NC}")
    backup_path = os.path.join(config["CRON_BACKUP_DIR"], f"cron_full_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    print(f"即将备份当前所有Cron任务到：{backup_path}")
    
    # 第二步：用户确认（必须输入Y/y才执行）
    confirm = input(f"{BOLD_RED}请输入 'Y' 确认执行清除（输入其他则取消）：{NC}")
    if confirm.strip().upper() != "Y":
        print(f"{GREEN}✅ 用户取消操作，未清除任何Cron任务{NC}")
        return True

    # 第三步：备份所有Cron任务
    run_shell_cmd(f"crontab -l > {backup_path} 2>/dev/null")
    print(f"{GREEN}✅ 已备份所有Cron任务到：{backup_path}{NC}")

    # 第四步：清除所有Cron
    returncode = run_shell_cmd("crontab -r", capture_output=True)[2]
    if returncode == 0:
        print(f"{GREEN}✅ 所有Cron任务已完全清除！{NC}")
        # 验证清除结果
        cron_out = run_shell_cmd("crontab -l 2>/dev/null", capture_output=True)[0]
        if not cron_out:
            print(f"{GREEN}✅ 验证：当前无任何Cron任务{NC}")
        else:
            print(f"{YELLOW}ℹ️  验证异常：仍有Cron任务残留 → {cron_out}{NC}")
        return True
    else:
        print(f"{RED}❌ 清除失败！错误：{run_shell_cmd('crontab -r', capture_output=True)[1]}{NC}")
        return False

# ===================== 3. CSV/DB管理 =====================
def clean_old(config):
    """清理旧CSV文件（保留_raw）"""
    if not os.path.exists(config["CSV_MANAGE_SCRIPT"]):
        print(f"{RED}❌ csv_manage.py不存在：{config['CSV_MANAGE_SCRIPT']}{NC}")
        return False
    return run_shell_cmd(
        f"{config['PYTHON_EXEC_PATH']} {config['CSV_MANAGE_SCRIPT']} clean_old --config='{str(config)}'",
        check=True
    )[2] == 0

def extract(config):
    """转换_raw CSV（调用extract_song_data.py）"""
    print(f"{YELLOW}===== 调用extract_song_data.py转换CSV ====={NC}")
    if not os.path.exists(config["EXTRACT_SONG_SCRIPT"]):
        print(f"{RED}❌ extract_song_data.py不存在：{config['EXTRACT_SONG_SCRIPT']}{NC}")
        return False
    result = run_shell_cmd(f"{config['PYTHON_EXEC_PATH']} {config['EXTRACT_SONG_SCRIPT']}", capture_output=True)
    if result[2] == 0:
        print(f"{GREEN}✅ CSV转换完成：{result[0]}{NC}")
        return True
    else:
        print(f"{RED}❌ CSV转换失败：{result[1]}{NC}")
        return False

def sync_db(config):
    """同步CSV到数据库"""
    if not os.path.exists(config["CSV_MANAGE_SCRIPT"]):
        print(f"{RED}❌ csv_manage.py不存在：{config['CSV_MANAGE_SCRIPT']}{NC}")
        return False
    return run_shell_cmd(
        f"{config['PYTHON_EXEC_PATH']} {config['CSV_MANAGE_SCRIPT']} sync_db --config='{str(config)}'",
        check=True
    )[2] == 0

def sync_now(config):
    """手动同步远程CSV"""
    print(f"{YELLOW}===== 手动同步远程CSV ====={NC}")
    if not os.path.exists(config["SYNC_SCRIPT"]):
        print(f"{RED}❌ 同步脚本不存在：{config['SYNC_SCRIPT']}{NC}")
        return False
    result = run_shell_cmd(f"{config['PYTHON_EXEC_PATH']} {config['SYNC_SCRIPT']}", capture_output=True)
    if result[2] == 0:
        log_file = os.path.join(config["LOG_DIR"], f"sync_csv_{datetime.now().strftime('%Y%m%d')}.log")
        print(f"{GREEN}✅ 远程CSV同步完成！日志：{log_file}{NC}")
        return True
    else:
        print(f"{RED}❌ 远程CSV同步失败：{result[1]}{NC}")
        return False

def auto_run(config):
    """全自动流程：clean_old → extract → sync_db → sync_now"""
    print(f"{YELLOW}===== 开始全自动执行 ====={NC}")
    try:
        clean_old(config)
        print("-" * 40)
        extract(config)
        print("-" * 40)
        sync_db(config)
        print("-" * 40)
        sync_now(config)
        print(f"{GREEN}🎉 全自动执行完成！{NC}")
        return True
    except Exception as e:
        print(f"{RED}❌ 全自动执行失败：{str(e)}{NC}")
        return False

# ===================== 4. 帮助信息 =====================
def show_help():
    """展示完整帮助信息"""
    print(f"{YELLOW}===== 主项目管理脚本（完整功能版）====={NC}")
    print("用法：python3 manage.py [命令]")
    print("\n【1. 初始化/权限】")
    print("  starter            - 初始化脚本执行权限（仅需一次）")
    print("\n【2. Cron管理（核心）】")
    print("  config-cron        - 配置Cron（默认每天2点执行auto）")
    print("  check-cron         - 检查当前Cron配置")
    print("  cancel-cron        - 仅清除本项目的Cron任务（保留其他）")
    print(f"  clear-all-cron     - {BOLD_RED}清除当前用户所有Cron任务（危险！）{NC}")
    print("\n【3. CSV/DB管理】")
    print("  clean_old          - 清理旧CSV文件（保留_raw源文件）")
    print("  extract            - 转换_raw CSV为目标格式")
    print("  sync_db            - 同步CSV数据到MySQL")
    print("  sync-now           - 手动同步远程CSV文件")
    print("\n【4. 全自动】")
    print("  auto               - 一键执行：clean_old→extract→sync_db→sync-now")
    print("\n【5. 其他】")
    print("  help               - 查看此帮助信息")

# ===================== 主入口 =====================
def main():
    if len(sys.argv) < 2:
        show_help()
        sys.exit(1)

    # 读取配置
    config = get_full_config()
    # 命令映射（覆盖所有功能）
    command_map = {
        # 初始化
        "starter": lambda: starter(config),
        # Cron管理
        "config-cron": lambda: config_cron(config),
        "check-cron": lambda: check_cron(config),
        "cancel-cron": lambda: cancel_cron(config),
        "clear-all-cron": lambda: clear_all_cron(config),
        # CSV/DB
        "clean_old": lambda: clean_old(config),
        "extract": lambda: extract(config),
        "sync_db": lambda: sync_db(config),
        "sync-now": lambda: sync_now(config),
        # 全自动
        "auto": lambda: auto_run(config),
        # 帮助
        "help": show_help
    }

    # 执行命令
    command = sys.argv[1]
    if command in command_map:
        success = command_map[command]()
        sys.exit(0 if success else 1)
    else:
        print(f"{RED}❌ 未知命令：{command}{NC}")
        show_help()
        sys.exit(1)

if __name__ == "__main__":
    main()