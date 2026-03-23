#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
cron管理子脚本：独立处理cron配置/检查/取消
被manage.py调用，无需单独执行
"""
import os
import sys
import tempfile
import subprocess
from datetime import datetime

# 颜色常量
RED = '\033[0;31m'
GREEN = '\033[0;32m'
YELLOW = '\033[1;33m'
NC = '\033[0m'

def run_shell_cmd(cmd, capture_output=False):
    """执行shell命令，返回(输出, 错误, 退出码)"""
    if capture_output:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        return result.stdout, result.stderr, result.returncode
    else:
        subprocess.run(cmd, shell=True)
        return "", "", 0

def config_cron(config):
    """配置定时任务"""
    # 验证cron配置
    if not config["CRON_TASKS"]:
        print(f"{RED}❌ 读取cron配置失败！{NC}")
        return False

    # 创建备份目录 + 备份原有crontab
    os.makedirs(config["CRON_BACKUP_DIR"], exist_ok=True)
    backup_file = os.path.join(config["CRON_BACKUP_DIR"], f"cron_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    run_shell_cmd(f"crontab -l > {backup_file} 2>/dev/null")
    print(f"{GREEN}✅ 已备份原有crontab到{backup_file}{NC}")

    # 过滤原有非本项目任务
    existing_cron, _, _ = run_shell_cmd(f"crontab -l 2>/dev/null | grep -v '{config['CRON_TASK_MARK']}'", capture_output=True)

    # 生成新cron配置
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
        temp_file.write(existing_cron)
        temp_file.write(f"\n# 节奏游戏项目定时任务（自动生成）\n")
        temp_file.write(config["CRON_TASKS"])
        temp_path = temp_file.name

    # 导入新配置
    _, stderr, returncode = run_shell_cmd(f"crontab {temp_path}", capture_output=True)
    if returncode == 0:
        print(f"{GREEN}✅ 定时任务配置成功！{NC}")
        os.unlink(temp_path)
        # 展示本项目cron
        print(f"{YELLOW}当前本项目定时任务：{NC}")
        run_shell_cmd(f"crontab -l | grep -A 100 '{config['CRON_TASK_MARK']}'")
        return True
    else:
        print(f"{RED}❌ 配置失败！已恢复原有配置：{stderr}{NC}")
        run_shell_cmd(f"crontab {backup_file}")
        os.unlink(temp_path)
        return False

def check_cron(config):
    """检查cron配置"""
    print(f"{YELLOW}===== 当前服务器本项目cron配置 ====={NC}")
    cron_out, _, _ = run_shell_cmd(f"crontab -l | grep -A 100 '{config['CRON_TASK_MARK']}' || echo '{RED}❌ 无本项目任务{NC}'", capture_output=True)
    print(cron_out)

    print(f"\n{YELLOW}===== settings.py中配置 ====={NC}")
    print(config["CRON_TASKS"] if config["CRON_TASKS"] else f"{RED}❌ 读取失败{NC}")

def cancel_cron(config):
    """取消本项目cron任务"""
    # 备份当前cron
    os.makedirs(config["CRON_BACKUP_DIR"], exist_ok=True)
    backup_file = os.path.join(config["CRON_BACKUP_DIR"], f"cron_backup_cancel_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    run_shell_cmd(f"crontab -l > {backup_file} 2>/dev/null")
    print(f"{GREEN}✅ 已备份当前crontab到{backup_file}{NC}")

    # 过滤本项目任务
    cron_content, _, _ = run_shell_cmd("crontab -l 2>/dev/null", capture_output=True)
    if config["CRON_TASK_MARK"] not in cron_content:
        print(f"{YELLOW}ℹ️  无本项目cron任务，无需取消{NC}")
        return True

    new_cron = [line for line in cron_content.split('\n') if config["CRON_TASK_MARK"] not in line]
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
        temp_file.write('\n'.join(new_cron).strip())
        temp_path = temp_file.name

    _, stderr, returncode = run_shell_cmd(f"crontab {temp_path}", capture_output=True)
    if returncode == 0:
        print(f"{GREEN}✅ 本项目cron任务已取消！{NC}")
        print(f"{YELLOW}剩余定时任务：{NC}")
        run_shell_cmd("crontab -l || echo '❌ 无剩余任务'")
        os.unlink(temp_path)
        return True
    else:
        print(f"{RED}❌ 取消失败！已恢复：{stderr}{NC}")
        run_shell_cmd(f"crontab {backup_file}")
        os.unlink(temp_path)
        return False

if __name__ == "__main__":
    # 仅用于测试，实际由manage.py调用
    if len(sys.argv) < 2:
        print("用法：python3 cron_manage.py [config/check/cancel] --config '{...}'")
        sys.exit(1)
    cmd = sys.argv[1]
    config = eval(sys.argv[2].replace('--config=', '')) if len(sys.argv) > 2 else {}
    if cmd == "config":
        config_cron(config)
    elif cmd == "check":
        check_cron(config)
    elif cmd == "cancel":
        cancel_cron(config)