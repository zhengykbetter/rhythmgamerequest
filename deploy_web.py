#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
重构版适配：适配新的模块化架构
"""
import subprocess
import time
import os
from config.settings import (
    MAIN_REPO_ROOT,
    WEB_APP_DIR,         # 需要确保这个指向项目根目录
    GUNICORN_BIND_HOST,
    GUNICORN_BIND_PORT,
    FLASK_APP_ENTRY,     # 需要更新这个配置
    GUNICORN_WORKERS,
    COLORS
)

# 固定虚拟环境路径（保持不变）
VENV_GUNICORN = "/opt/main_project/venv/bin/gunicorn"

def run_cmd(cmd, desc):
    print(f"{COLORS['GREEN']}[INFO]{COLORS['NC']} {desc}")
    subprocess.Popen(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

if __name__ == "__main__":
    print(f"{COLORS['YELLOW']}===== RhythmGameQuery 自动化部署 (重构版) ====={COLORS['NC']}")
    print(f"运行应用根目录: {WEB_APP_DIR}")

    # 1. 清理进程
    run_cmd("pkill -f gunicorn 2>/dev/null", "清理旧进程")
    run_cmd("pkill -f nginx 2>/dev/null", "清理Nginx")
    time.sleep(0.5)

    # 2. 权限
    run_cmd(f"chmod -R 755 {MAIN_REPO_ROOT}", "修复权限")
    time.sleep(0.5)

    # 3. 启动 Gunicorn
    # 注意：--chdir 确保指向包含 run.py 的目录
    gunicorn_cmd = (
        f"{VENV_GUNICORN} -w {GUNICORN_WORKERS} "
        f"--chdir {WEB_APP_DIR} "
        f"--bind {GUNICORN_BIND_HOST}:{GUNICORN_BIND_PORT} "
        f"{FLASK_APP_ENTRY} -D"
    )
    run_cmd(gunicorn_cmd, "启动 Gunicorn 后端服务")
    time.sleep(1)

    # 4. 启动Nginx
    run_cmd("systemctl restart nginx", "重启 Nginx 网关")

    # 完成
    print(f"\n{COLORS['GREEN']}✅ 部署成功！新架构已生效！{COLORS['NC']}")