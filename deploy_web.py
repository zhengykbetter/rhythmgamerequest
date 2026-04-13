#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
重构版适配：支持 debug 参数，实时查看调试信息
"""
import subprocess
import time
import os
import sys
from config.settings import (
    MAIN_REPO_ROOT,
    WEB_APP_DIR,
    GUNICORN_BIND_HOST,
    GUNICORN_BIND_PORT,
    FLASK_APP_ENTRY,
    GUNICORN_WORKERS,
    COLORS
)

VENV_GUNICORN = "/opt/main_project/venv/bin/gunicorn"

def run_cmd(cmd, desc, debug=False):
    print(f"{COLORS['GREEN']}[INFO]{COLORS['NC']} {desc}")
    if debug:
        # Debug模式：前台运行，实时输出所有日志
        subprocess.run(cmd, shell=True)
    else:
        # 正常模式：后台静默运行
        subprocess.Popen(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

if __name__ == "__main__":
    # 解析命令行参数：判断是否为 debug 模式
    DEBUG_MODE = len(sys.argv) > 1 and sys.argv[1].lower() == "debug"

    print(f"{COLORS['YELLOW']}===== RhythmGameQuery 自动化部署 (重构版) ====={COLORS['NC']}")
    print(f"运行应用根目录: {WEB_APP_DIR}")
    
    # 修复：使用原有COLORS，不新增CYAN
    if DEBUG_MODE:
        print(f"{COLORS['YELLOW']}🔧 [DEBUG 模式] 前台运行，实时显示调试信息{COLORS['NC']}")
    else:
        print(f"{COLORS['GREEN']}🚀 [正常模式] 后台静默部署{COLORS['NC']}")

    # 1. 清理进程（无论是否 debug 都需要）
    run_cmd("pkill -f gunicorn 2>/dev/null", "清理旧进程", debug=False)
    run_cmd("pkill -f nginx 2>/dev/null", "清理Nginx", debug=False)
    time.sleep(0.5)

    # 2. 权限
    run_cmd(f"chmod -R 755 {MAIN_REPO_ROOT}", "修复权限", debug=False)
    time.sleep(0.5)

    # 3. 启动 Gunicorn（核心：区分 debug 模式）
    if DEBUG_MODE:
        # Debug模式：去掉 -D（后台），实时输出日志
        gunicorn_cmd = (
            f"{VENV_GUNICORN} -w {GUNICORN_WORKERS} "
            f"--chdir {WEB_APP_DIR} "
            f"--bind {GUNICORN_BIND_HOST}:{GUNICORN_BIND_PORT} "
            f"--access-logfile - --error-logfile - "
            f"{FLASK_APP_ENTRY}"
        )
        print(f"\n{COLORS['YELLOW']}🔍 Debug 模式已启动，按 Ctrl+C 停止{COLORS['NC']}")
        print(f"{COLORS['YELLOW']}--------------------------------------------------{COLORS['NC']}\n")
        run_cmd(gunicorn_cmd, "启动 Gunicorn 调试服务", debug=True)
    else:
        # 正常模式：后台静默运行
        gunicorn_cmd = (
            f"{VENV_GUNICORN} -w {GUNICORN_WORKERS} "
            f"--chdir {WEB_APP_DIR} "
            f"--bind {GUNICORN_BIND_HOST}:{GUNICORN_BIND_PORT} "
            f"{FLASK_APP_ENTRY} -D"
        )
        run_cmd(gunicorn_cmd, "启动 Gunicorn 后端服务", debug=False)
        time.sleep(1)

        # 4. 启动Nginx
        run_cmd("systemctl restart nginx", "重启 Nginx 网关", debug=False)

        # 完成
        print(f"\n{COLORS['GREEN']}✅ 部署成功！新架构已生效！{COLORS['NC']}")