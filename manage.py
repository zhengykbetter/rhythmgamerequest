#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主管理脚本：终极修复版（指令全短横线 + 无报错）
支持指令：starter / config-cron / check-cron / cancel-cron / clear-all-cron / clean-old / extract / sync-db / sync-now / auto / help
"""
import os
import sys
import subprocess
import getpass
from datetime import datetime

# ===================== 模块顶层导入settings =====================
try:
    from config.settings import *
    SETTINGS_LOADED = True
    IMPORT_ERROR_MSG = ""
except ImportError as e:
    SETTINGS_LOADED = False
    IMPORT_ERROR_MSG = str(e)
except Exception as e:
    SETTINGS_LOADED = False
    IMPORT_ERROR_MSG = str(e)

# ===================== 颜色常量 =====================
RED = '\033[0;31m'
GREEN = '\033[0;32m'
YELLOW = '\033[1;33m'
BOLD_RED = '\033[1;31m'
NC = '\033[0m'  # 重置颜色

# ===================== 核心工具函数 =====================
def run_shell_cmd(cmd, capture_output=False):
    """执行shell命令，返回(输出, 错误, 退出码)"""
    if capture_output:
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
    """读取配置（默认+settings覆盖）"""
    # 默认配置
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
        "CSV_SOURCE_DIR": os.path.join(os.getcwd(), "data", "csv", "source"),  # 补充默认值
        "DB_CONFIG": {
            "host": "localhost", "port": 3306, "user": "root",
            "password": "", "database": "rhythmgame", "charset": "utf8mb4"
        }
    }

    # 用settings覆盖默认配置
    if SETTINGS_LOADED:
        print(f"{GREEN}✅ 成功读取settings.py配置{NC}")
        for key in default_config.keys():
            if key in locals() and locals()[key] is not None:
                default_config[key] = locals()[key]
    else:
        print(f"{YELLOW}ℹ️  未读取到settings.py，使用默认配置（{IMPORT_ERROR_MSG}）{NC}")

    # 创建必要目录（含CSV_SOURCE_DIR）
    for dir_path in [
        default_config["LOG_DIR"], default_config["CRON_BACKUP_DIR"],
        default_config["CSV_ROOT_DIR"], default_config["ARCHIVE_DIR"],
        default_config["CSV_SOURCE_DIR"],  # 创建源目录
        os.path.dirname(default_config["CRON_MANAGE_SCRIPT"]),
        os.path.dirname(default_config["CSV_MANAGE_SCRIPT"])
    ]:
        os.makedirs(dir_path, exist_ok=True)

    return default_config

# ===================== 1. 初始化权限 =====================
def starter(config):
    """初始化脚本执行权限"""
    print(f"{YELLOW}===== 初始化脚本执行权限（仅需一次）====={NC}")
    # 主脚本赋权
    os.chmod(__file__, 0o755)
    print(f"{GREEN}✅ 已赋予{__file__}执行权限{NC}")
    
    # scripts目录赋权
    scripts_dir = os.path.join(config["MAIN_REPO_ROOT"], "scripts")
    if os.path.exists(scripts_dir):
        for fname in os.listdir(scripts_dir):
            if fname.endswith(".py"):
                fpath = os.path.join(scripts_dir, fname)
                os.chmod(fpath, 0o755)
                print(f"{GREEN}✅ 已赋予{fpath}执行权限{NC}")
    
    # managers目录赋权
    managers_dir = os.path.join(config["MAIN_REPO_ROOT"], "managers")
    if os.path.exists(managers_dir):
        for fname in os.listdir(managers_dir):
            if fname.endswith(".py"):
                fpath = os.path.join(managers_dir, fname)
                os.chmod(fpath, 0o755)
                print(f"{GREEN}✅ 已赋予{fpath}执行权限{NC}")
    
    print(f"{GREEN}===== 权限初始化完成 ====={NC}")

# ===================== 2. Cron管理 =====================
def config_cron(config):
    """配置Cron任务"""
    starter(config)
    if not os.path.exists(config["CRON_MANAGE_SCRIPT"]):
        print(f"{RED}❌ cron_manage.py不存在：{config['CRON_MANAGE_SCRIPT']}{NC}")
        return False
    stdout, stderr, returncode = run_shell_cmd(
        f"{config['PYTHON_EXEC_PATH']} {config['CRON_MANAGE_SCRIPT']} config",
        capture_output=True
    )
    if returncode == 0:
        print(f"{GREEN}✅ Cron配置成功{NC}")
        return True
    else:
        print(f"{RED}❌ Cron配置失败：{stderr}{NC}")
        return False

def check_cron(config):
    """检查Cron配置"""
    if not os.path.exists(config["CRON_MANAGE_SCRIPT"]):
        print(f"{RED}❌ cron_manage.py不存在：{config['CRON_MANAGE_SCRIPT']}{NC}")
        return False
    run_shell_cmd(f"{config['PYTHON_EXEC_PATH']} {config['CRON_MANAGE_SCRIPT']} check")
    return True

def cancel_cron(config):
    """清除本项目Cron任务"""
    if not os.path.exists(config["CRON_MANAGE_SCRIPT"]):
        print(f"{RED}❌ cron_manage.py不存在：{config['CRON_MANAGE_SCRIPT']}{NC}")
        return False
    stdout, stderr, returncode = run_shell_cmd(
        f"{config['PYTHON_EXEC_PATH']} {config['CRON_MANAGE_SCRIPT']} cancel",
        capture_output=True
    )
    if returncode == 0:
        print(f"{GREEN}✅ 本项目Cron已清除{NC}")
        return True
    else:
        print(f"{RED}❌ 清除失败：{stderr}{NC}")
        return False

def clear_all_cron(config):
    """清除所有Cron任务（高危）"""
    print(f"{BOLD_RED}===== 危险操作警告 ====={NC}")
    print(f"{BOLD_RED}此命令将删除当前用户（{getpass.getuser()}）的所有Cron任务！{NC}")
    backup_path = os.path.join(config["CRON_BACKUP_DIR"], f"cron_full_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    print(f"即将备份所有Cron任务到：{backup_path}")
    
    confirm = input(f"{BOLD_RED}请输入 'Y' 确认执行清除（输入其他则取消）：{NC}")
    if confirm.strip().upper() != "Y":
        print(f"{GREEN}✅ 用户取消操作{NC}")
        return True

    # 备份
    run_shell_cmd(f"crontab -l 2>/dev/null > {backup_path}", capture_output=True)
    print(f"{GREEN}✅ 已备份到：{backup_path}{NC}")

    # 清除
    stdout, stderr, returncode = run_shell_cmd("crontab -r", capture_output=True)
    if returncode == 0:
        print(f"{GREEN}✅ 所有Cron已清除{NC}")
        cron_out, _, _ = run_shell_cmd("crontab -l 2>/dev/null", capture_output=True)
        if not cron_out:
            print(f"{GREEN}✅ 验证：无任何Cron任务{NC}")
        else:
            print(f"{YELLOW}ℹ️  残留任务：{cron_out}{NC}")
        return True
    else:
        print(f"{RED}❌ 清除失败：{stderr}{NC}")
        return False

# ===================== 3. CSV/DB管理（短横线指令对应函数） =====================
def clean_old(config):
    """清理旧CSV文件"""
    print(f"{YELLOW}===== 清理旧CSV文件 ====={NC}")
    if not os.path.exists(config["CSV_MANAGE_SCRIPT"]):
        print(f"{RED}❌ csv_manage.py不存在：{config['CSV_MANAGE_SCRIPT']}{NC}")
        return False
    # 传递CSV_ROOT_DIR到csv_manage.py（避免KeyError）
    cmd = f"{config['PYTHON_EXEC_PATH']} {config['CSV_MANAGE_SCRIPT']} clean-old --csv-root-dir {config['CSV_ROOT_DIR']}"
    stdout, stderr, returncode = run_shell_cmd(cmd, capture_output=True)
    if returncode == 0:
        print(f"{GREEN}✅ 旧CSV清理完成：{stdout}{NC}")
        return True
    else:
        print(f"{RED}❌ 清理失败：{stderr}{NC}")
        return False

def extract(config):
    """转换_raw CSV"""
    print(f"{YELLOW}===== 转换原始CSV ====={NC}")
    if not os.path.exists(config["EXTRACT_SONG_SCRIPT"]):
        print(f"{RED}❌ extract_song_data.py不存在：{config['EXTRACT_SONG_SCRIPT']}{NC}")
        return False
    stdout, stderr, returncode = run_shell_cmd(
        f"{config['PYTHON_EXEC_PATH']} {config['EXTRACT_SONG_SCRIPT']}",
        capture_output=True
    )
    if returncode == 0:
        print(f"{GREEN}✅ CSV转换完成：{stdout}{NC}")
        return True
    else:
        print(f"{RED}❌ 转换失败：{stderr}{NC}")
        return False

def sync_db(config):
    """同步CSV到数据库"""
    print(f"{YELLOW}===== 同步CSV到MySQL ====={NC}")
    if not os.path.exists(config["CSV_MANAGE_SCRIPT"]):
        print(f"{RED}❌ csv_manage.py不存在：{config['CSV_MANAGE_SCRIPT']}{NC}")
        return False
    stdout, stderr, returncode = run_shell_cmd(
        f"{config['PYTHON_EXEC_PATH']} {config['CSV_MANAGE_SCRIPT']} sync-db",
        capture_output=True
    )
    if returncode == 0:
        print(f"{GREEN}✅ 数据库同步完成：{stdout}{NC}")
        return True
    else:
        print(f"{RED}❌ 同步失败：{stderr}{NC}")
        return False

def sync_now(config):
    """手动同步远程CSV"""
    print(f"{YELLOW}===== 同步远程CSV ====={NC}")
    if not os.path.exists(config["SYNC_SCRIPT"]):
        print(f"{RED}❌ sync_csv_from_remote.py不存在：{config['SYNC_SCRIPT']}{NC}")
        return False
    stdout, stderr, returncode = run_shell_cmd(
        f"{config['PYTHON_EXEC_PATH']} {config['SYNC_SCRIPT']}",
        capture_output=True
    )
    if returncode == 0:
        print(f"{GREEN}✅ 远程CSV同步完成：{stdout}{NC}")
        return True
    else:
        print(f"{RED}❌ 同步失败：{stderr}{NC}")
        return False

# ===================== 4. 全自动流程 =====================
def auto_run(config):
    """全自动执行：clean-old → extract → sync-db → sync-now"""
    print(f"{YELLOW}===== 开始全自动执行 ====={NC}")
    try:
        # 分步执行（短横线指令对应函数）
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

# ===================== 5. 帮助信息（短横线指令） =====================
def show_help():
    """展示帮助（全短横线指令）"""
    print(f"{YELLOW}===== 项目管理脚本指令说明 ====={NC}")
    print("用法：python3 manage.py [指令]")
    print("\n【1. 初始化/权限】")
    print("  starter            - 初始化脚本执行权限（仅需一次）")
    print("\n【2. Cron管理】")
    print("  config-cron        - 配置定时任务（每天2点执行auto）")
    print("  check-cron         - 检查当前Cron配置")
    print("  cancel-cron        - 清除本项目Cron任务")
    print(f"  clear-all-cron     - {BOLD_RED}清除所有Cron任务（危险！）{NC}")
    print("\n【3. CSV/DB管理】")
    print("  clean-old          - 清理旧CSV文件（保留_raw）")
    print("  extract            - 转换原始CSV为目标格式")
    print("  sync-db            - 同步CSV到MySQL")
    print("  sync-now           - 手动同步远程CSV")
    print("\n【4. 全自动】")
    print("  auto               - 一键执行：clean-old→extract→sync-db→sync-now")
    print("\n【5. 其他】")
    print("  help               - 查看此帮助信息")

# ===================== 主入口（全短横线指令映射） =====================
def main():
    if len(sys.argv) < 2:
        show_help()
        sys.exit(1)

    # 读取配置
    config = get_full_config()

    # 指令映射（全短横线，无下划线）
    command_map = {
        "starter": lambda: starter(config),
        "config-cron": lambda: config_cron(config),
        "check-cron": lambda: check_cron(config),
        "cancel-cron": lambda: cancel_cron(config),
        "clear-all-cron": lambda: clear_all_cron(config),
        "clean-old": lambda: clean_old(config),  # 短横线
        "extract": lambda: extract(config),
        "sync-db": lambda: sync_db(config),      # 短横线
        "sync-now": lambda: sync_now(config),    # 短横线
        "auto": lambda: auto_run(config),
        "help": show_help
    }

    # 执行指令
    command = sys.argv[1]
    if command in command_map:
        success = command_map[command]()
        sys.exit(0 if success else 1)
    else:
        print(f"{RED}❌ 未知指令：{command}{NC}")
        show_help()
        sys.exit(1)

if __name__ == "__main__":
    main()