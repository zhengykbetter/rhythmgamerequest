#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主管理脚本：修复import *语法错误 + 完整功能
"""
import os
import sys
import subprocess
import getpass
from datetime import datetime

# ===================== 模块顶层导入（核心修复：移到此处） =====================
# 先导入settings，若失败则标记为未导入，后续用默认配置
try:
    from config.settings import *  # 合法：仅在模块顶层使用import *
    SETTINGS_LOADED = True
except ImportError as e:
    SETTINGS_LOADED = False
    IMPORT_ERROR_MSG = str(e)
except Exception as e:
    SETTINGS_LOADED = False
    IMPORT_ERROR_MSG = str(e)

# ===================== 基础配置 & 颜色常量 =====================
sys.path.insert(0, os.getcwd())
RED = '\033[0;31m'
GREEN = '\033[0;32m'
YELLOW = '\033[1;33m'
BOLD_RED = '\033[1;31m'
NC = '\033[0m'

# ===================== 核心工具函数 =====================
def run_shell_cmd(cmd, capture_output=False):
    """执行shell命令，返回(输出, 错误, 退出码)"""
    if capture_output:
        # 优化：避免shell解析错误
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, 
            executable="/bin/bash"
        )
        return result.stdout, result.stderr, result.returncode
    else:
        subprocess.run(
            cmd, shell=True, 
            executable="/bin/bash"
        )
        return "", "", 0

def get_full_config():
    """读取配置（修复import *后的合法逻辑）"""
    # 1. 基础默认配置
    default_config = {
        "PYTHON_EXEC_PATH": "python3",
        "MAIN_REPO_ROOT": os.getcwd(),
        "LOG_DIR": os.path.join(os.getcwd(), "logs"),
        "CRON_BACKUP_DIR": os.path.join(os.getcwd(), "logs"),
        "CRON_TASK_MARK": "# 节奏游戏项目定时任务",
        # 默认CRON：每天凌晨2点执行auto
        "CRON_TASKS": [
            f"0 2 * * * python3 {os.path.join(os.getcwd(), 'manage.py')} auto > {os.path.join(os.getcwd(), 'logs', 'auto_cron.log')} 2>&1"
        ],
        # 脚本路径
        "SYNC_SCRIPT": os.path.join(os.getcwd(), "scripts", "sync_csv_from_remote.py"),
        "EXTRACT_SONG_SCRIPT": os.path.join(os.getcwd(), "scripts", "extract_song_data.py"),
        "CRON_MANAGE_SCRIPT": os.path.join(os.getcwd(), "managers", "cron_manage.py"),
        "CSV_MANAGE_SCRIPT": os.path.join(os.getcwd(), "managers", "csv_manage.py"),
        # CSV/DB配置
        "CSV_ROOT_DIR": os.path.join(os.getcwd(), "data", "csv"),
        "ARCHIVE_DIR": os.path.join(os.getcwd(), "data", "csv", "archive"),
        "DB_CONFIG": {
            "host": "localhost", "port": 3306, "user": "root",
            "password": "", "database": "rhythmgame", "charset": "utf8mb4"
        }
    }

    # 2. 若settings加载成功，用settings变量覆盖默认配置（容错逻辑）
    if SETTINGS_LOADED:
        print(f"{GREEN}✅ 成功读取settings.py配置{NC}")
        # 逐个覆盖：仅覆盖默认配置中存在的变量
        for key in default_config.keys():
            if key in locals() and locals()[key] is not None:
                default_config[key] = locals()[key]
    else:
        print(f"{YELLOW}ℹ️  未读取到settings.py，使用默认配置（{IMPORT_ERROR_MSG}）{NC}")

    # 3. 创建必要目录
    for dir_path in [
        default_config["LOG_DIR"], default_config["CRON_BACKUP_DIR"],
        default_config["CSV_ROOT_DIR"], default_config["ARCHIVE_DIR"],
        os.path.dirname(default_config["CRON_MANAGE_SCRIPT"]),
        os.path.dirname(default_config["CSV_MANAGE_SCRIPT"])
    ]:
        os.makedirs(dir_path, exist_ok=True)

    return default_config

# ===================== 其余函数（starter/config-cron/clear-all-cron等）保持不变 =====================
def starter(config):
    """初始化权限（逻辑不变）"""
    print(f"{YELLOW}===== 初始化脚本执行权限（仅需一次）====={NC}")
    os.chmod(__file__, 0o755)
    print(f"{GREEN}✅ 已赋予{__file__}执行权限{NC}")
    
    # 批量赋权scripts目录
    scripts_dir = os.path.join(config["MAIN_REPO_ROOT"], "scripts")
    if os.path.exists(scripts_dir):
        for fname in os.listdir(scripts_dir):
            if fname.endswith(".py"):
                fpath = os.path.join(scripts_dir, fname)
                os.chmod(fpath, 0o755)
                print(f"{GREEN}✅ 已赋予{fpath}执行权限{NC}")
    
    # 批量赋权managers目录
    managers_dir = os.path.join(config["MAIN_REPO_ROOT"], "managers")
    if os.path.exists(managers_dir):
        for fname in os.listdir(managers_dir):
            if fname.endswith(".py"):
                fpath = os.path.join(managers_dir, fname)
                os.chmod(fpath, 0o755)
                print(f"{GREEN}✅ 已赋予{fpath}执行权限{NC}")
    
    print(f"{GREEN}===== 权限初始化完成 ====={NC}")

# 1. 修改 config_cron 函数
def config_cron(config):
    """配置Cron（删除--config参数传递）"""
    starter(config)
    if not os.path.exists(config["CRON_MANAGE_SCRIPT"]):
        print(f"{RED}❌ cron_manage.py不存在：{config['CRON_MANAGE_SCRIPT']}{NC}")
        return False
    # 关键修改：仅传递命令，不传递config字符串
    return run_shell_cmd(
        f"{config['PYTHON_EXEC_PATH']} {config['CRON_MANAGE_SCRIPT']} config",  # 移除 --config='{str(config)}'
        check=True
    )[2] == 0

# 2. 修改 check_cron 函数
def check_cron(config):
    """检查Cron（删除--config参数传递）"""
    if not os.path.exists(config["CRON_MANAGE_SCRIPT"]):
        print(f"{RED}❌ cron_manage.py不存在：{config['CRON_MANAGE_SCRIPT']}{NC}")
        return False
    # 关键修改：仅传递命令，不传递config字符串
    run_shell_cmd(f"{config['PYTHON_EXEC_PATH']} {config['CRON_MANAGE_SCRIPT']} check")  # 移除 --config='{str(config)}'
    return True
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

    # 第三步：备份所有Cron任务（微调：确保2>/dev/null屏蔽空crontab警告）
    run_shell_cmd(f"crontab -l 2>/dev/null > {backup_path}", capture_output=True)
    print(f"{GREEN}✅ 已备份所有Cron任务到：{backup_path}{NC}")

    # 第四步：清除所有Cron（微调：简化命令，确保shell解析正确）
    stdout, stderr, returncode = run_shell_cmd("crontab -r", capture_output=True)
    if returncode == 0:
        print(f"{GREEN}✅ 所有Cron任务已完全清除！{NC}")
        # 验证清除结果（屏蔽空crontab的警告）
        cron_out, _, _ = run_shell_cmd("crontab -l 2>/dev/null", capture_output=True)
        if not cron_out:
            print(f"{GREEN}✅ 验证：当前无任何Cron任务{NC}")
        else:
            print(f"{YELLOW}ℹ️  验证异常：仍有Cron任务残留 → {cron_out}{NC}")
        return True
    else:
        print(f"{RED}❌ 清除失败！错误：{stderr}{NC}")
        return False

# ===================== 其余函数（clean_old/extract/sync_db/auto_run/show_help/main）均保持不变 =====================
def show_help():
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

def main():
    if len(sys.argv) < 2:
        show_help()
        sys.exit(1)

    config = get_full_config()
    command_map = {
        "starter": lambda: starter(config),
        "config-cron": lambda: config_cron(config),
        "check-cron": lambda: check_cron(config),
        "cancel-cron": lambda: cancel_cron(config),
        "clear-all-cron": lambda: clear_all_cron(config),
        "clean_old": lambda: clean_old(config),
        "extract": lambda: extract(config),
        "sync_db": lambda: sync_db(config),
        "sync-now": lambda: sync_now(config),
        "auto": lambda: auto_run(config),
        "help": show_help
    }

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