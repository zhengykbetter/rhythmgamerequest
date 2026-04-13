#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
完全修复版：Debug模式下也启动Nginx，100%复现生产环境
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

def run_cmd(cmd, desc, debug=False, block=False):
    print(f"{COLORS['GREEN']}[INFO]{COLORS['NC']} {desc}")
    if debug and block:
        subprocess.run(cmd, shell=True)
    else:
        subprocess.Popen(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

if __name__ == "__main__":
    DEBUG_MODE = len(sys.argv) > 1 and sys.argv[1].lower() == "debug"

    print(f"{COLORS['YELLOW']}===== RhythmGameQuery 自动化部署 ====={COLORS['NC']}")
    print(f"运行应用根目录: {WEB_APP_DIR}")
    
    if DEBUG_MODE:
        print(f"{COLORS['YELLOW']}🔧 [DEBUG 模式] 100%复现生产环境 + 实时调试信息{COLORS['NC']}")
    else:
        print(f"{COLORS['GREEN']}🚀 [正常模式] 后台静默部署{COLORS['NC']}")

    # 1. 清理进程
    run_cmd("pkill -f gunicorn 2>/dev/null", "清理旧进程")
    run_cmd("pkill -f nginx 2>/dev/null", "清理Nginx")
    time.sleep(0.5)

    # 2. 权限
    run_cmd(f"chmod -R 755 {MAIN_REPO_ROOT}", "修复权限")
    time.sleep(0.5)

    # 3. 启动 Gunicorn
    if DEBUG_MODE:
        # Debug模式：后台启动Gunicorn（让Nginx能连上）+ 实时看日志
        gunicorn_cmd = (
            f"{VENV_GUNICORN} -w {GUNICORN_WORKERS} "
            f"--chdir {WEB_APP_DIR} "
            f"--bind {GUNICORN_BIND_HOST}:{GUNICORN_BIND_PORT} "
            f"--access-logfile /tmp/gunicorn_access.log --error-logfile /tmp/gunicorn_error.log "
            f"{FLASK_APP_ENTRY} -D"
        )
        run_cmd(gunicorn_cmd, "启动 Gunicorn 后端服务")
        time.sleep(1)
        
        # 4. 启动Nginx（和生产环境完全一致）
        run_cmd("systemctl restart nginx", "重启 Nginx 网关")
        
        print(f"\n{COLORS['YELLOW']}🔍 Debug 模式已启动！{COLORS['NC']}")
        print(f"{COLORS['YELLOW']}   - 外界访问地址：和生产环境完全一致{COLORS['NC']}")
        print(f"{COLORS['YELLOW']}   - 实时查看日志：tail -f /tmp/gunicorn_access.log /tmp/gunicorn_error.log{COLORS['NC']}")
        print(f"{COLORS['YELLOW']}   - 按 Ctrl+C 停止查看，服务继续在后台运行{COLORS['NC']}")
        print(f"{COLORS['YELLOW']}--------------------------------------------------{COLORS['NC']}\n")
        
        # 实时跟踪日志
        try:
            subprocess.run("tail -f /tmp/gunicorn_access.log /tmp/gunicorn_error.log", shell=True)
        except KeyboardInterrupt:
            print(f"\n{COLORS['YELLOW']}👋 停止查看日志，服务仍在后台运行{COLORS['NC']}")
    else:
        # 正常模式
        gunicorn_cmd = (
            f"{VENV_GUNICORN} -w {GUNICORN_WORKERS} "
            f"--chdir {WEB_APP_DIR} "
            f"--bind {GUNICORN_BIND_HOST}:{GUNICORN_BIND_PORT} "
            f"{FLASK_APP_ENTRY} -D"
        )
        run_cmd(gunicorn_cmd, "启动 Gunicorn 后端服务")
        time.sleep(1)
        run_cmd("systemctl restart nginx", "重启 Nginx 网关")
        print(f"\n{COLORS['GREEN']}✅ 部署成功！{COLORS['NC']}")