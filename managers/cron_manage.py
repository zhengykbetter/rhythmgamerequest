#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
cron_manage.py：重构版（直接读settings，无命令行复杂参数）
"""
import os
import sys
import subprocess
from pathlib import Path

# ===================== 直接加载settings.py配置 =====================
# 添加项目根目录到Python路径（确保能导入config.settings）
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # managers/ → 项目根

try:
    from config.settings import (
        PYTHON_EXEC_PATH, MAIN_REPO_ROOT, LOG_DIR, CRON_BACKUP_DIR,
        CRON_TASK_MARK, CRON_TASKS
    )
    SETTINGS_LOADED = True
except ImportError as e:
    print(f"❌ 读取settings.py失败：{e}")
    sys.exit(1)

# ===================== 工具函数 =====================
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

# ===================== Cron核心逻辑 =====================
def config_cron():
    """配置Cron任务（直接用settings中的配置）"""
    print(f"===== 配置Cron任务（每天2点执行auto）=====")
    # 1. 备份当前Cron
    backup_path = os.path.join(CRON_BACKUP_DIR, f"cron_backup_{os.popen('date +%Y%m%d_%H%M%S').read().strip()}.log")
    run_shell_cmd(f"crontab -l > {backup_path} 2>/dev/null")
    print(f"✅ 已备份当前Cron到：{backup_path}")

    # 2. 清除旧的本项目Cron任务 + 添加新任务
    # 核心：用echo输出纯Cron命令，避免特殊字符解析错误
    cron_cmd = f"""
    (crontab -l 2>/dev/null | grep -v '{CRON_TASK_MARK}'; 
     echo '{CRON_TASK_MARK}'; 
     echo '{CRON_TASKS[0]}') | crontab -
    """
    # 执行命令（清理换行符，避免解析错误）
    cron_cmd = cron_cmd.replace("\n", "").strip()
    stdout, stderr, returncode = run_shell_cmd(cron_cmd, capture_output=True)
    
    if returncode == 0:
        print(f"✅ Cron任务配置成功！")
        return True
    else:
        print(f"❌ Cron配置失败：{stderr}")
        return False

def check_cron():
    """检查当前Cron配置"""
    print(f"===== 当前服务器本项目cron配置 =====")
    # 过滤出本项目的Cron任务
    stdout, stderr, _ = run_shell_cmd(f"crontab -l 2>/dev/null | grep -A 10 '{CRON_TASK_MARK}'", capture_output=True)
    if stdout:
        print(stdout)
    else:
        print(f"ℹ️  未找到本项目的Cron任务")
    
    print(f"===== settings.py中配置 =====")
    print(CRON_TASKS)
    return True

def cancel_cron():
    """仅清除本项目的Cron任务"""
    print(f"===== 清除本项目Cron任务 =====")
    # 过滤掉本项目的任务，保留其他
    cron_cmd = f"crontab -l 2>/dev/null | grep -v '{CRON_TASK_MARK}' | crontab -"
    stdout, stderr, returncode = run_shell_cmd(cron_cmd, capture_output=True)
    
    if returncode == 0:
        print(f"✅ 本项目Cron任务已清除！")
        return True
    else:
        print(f"❌ 清除失败：{stderr}")
        return False

# ===================== 主入口（仅接收简单命令，无复杂参数） =====================
def main():
    if len(sys.argv) < 2:
        print("用法：python3 cron_manage.py [config/check/cancel]")
        sys.exit(1)
    
    command = sys.argv[1]
    if command == "config":
        config_cron()
    elif command == "check":
        check_cron()
    elif command == "cancel":
        cancel_cron()
    else:
        print(f"❌ 未知命令：{command}")
        sys.exit(1)

if __name__ == "__main__":
    main()