#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Web 自动化部署脚本
统一读取 config/settings.py 配置
功能：清理进程 → 启动Gunicorn → 重启Nginx
"""
import subprocess
import time
import signal
import os
from pathlib import Path

# 导入项目核心配置（所有参数从这里读取，无硬编码！）
from config.settings import (
    MAIN_REPO_ROOT,
    WEB_APP_DIR,
    GUNICORN_BIND_HOST,
    GUNICORN_BIND_PORT,
    FLASK_APP_ENTRY,
    GUNICORN_WORKERS,
    COLORS
)

# ===================== 工具函数 =====================
def run_command(cmd: str, desc: str = ""):
    """执行系统命令，带日志输出"""
    if desc:
        print(f"{COLORS['GREEN']}[INFO]{COLORS['NC']} {desc}")
    try:
        subprocess.run(cmd, shell=True, check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        print(f"{COLORS['RED']}[ERROR]{COLORS['NC']} 执行失败: {cmd}\n{e.stderr.decode()}")
        exit(1)

def kill_process(name: str):
    """强制杀死进程（gunicorn/nginx）"""
    run_command(f"pkill {name} 2>/dev/null || true", f"清理旧的 {name} 进程")

# ===================== 核心部署逻辑 =====================
if __name__ == "__main__":
    print(f"{COLORS['YELLOW']}===== RhythmGameQuery 自动化部署 ====={COLORS['NC']}")
    print(f"项目根目录: {MAIN_REPO_ROOT}")
    print(f"运行应用目录: {WEB_APP_DIR}")
    print(f"Gunicorn监听: {GUNICORN_BIND_HOST}:{GUNICORN_BIND_PORT}")

    # 1. 清理所有旧服务
    kill_process("gunicorn")
    kill_process("nginx")
    time.sleep(1)

    # 2. 修复目录权限（避免502）
    run_command(f"chmod -R 755 {MAIN_REPO_ROOT}", "修复项目权限")

    # 3. 启动 Gunicorn（关键：指定应用目录，不切换工作路径）
    gunicorn_cmd = (
        f"nohup gunicorn -w {GUNICORN_WORKERS} "
        f"--chdir {WEB_APP_DIR} "
        f"--bind {GUNICORN_BIND_HOST}:{GUNICORN_BIND_PORT} "
        f"{FLASK_APP_ENTRY} &>/dev/null &"
    )
    run_command(gunicorn_cmd, "启动 Gunicorn 后端服务")
    time.sleep(2)

    # 4. 重启 Nginx 网关
    run_command("systemctl restart nginx", "重启 Nginx 网关服务")

    # 完成
    print(f"\n{COLORS['GREEN']}✅ 部署成功！{COLORS['NC']}")
    print("访问地址: http://rhythmgamequery.cn")
    print("更新代码后，重新执行此脚本即可生效！")