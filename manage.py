#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主项目管理入口（仅做命令解析，核心逻辑拆分到manager目录）
用法：
./manage.py starter          # 初始化脚本权限
./manage.py set-cron        # 配置定时任务（原config-cron）
./manage.py check-cron      # 检查定时任务
./manage.py cancel-cron     # 取消本项目定时任务
./manage.py clear-all-cron  # 删除所有定时任务（高危）
./manage.py sync-now        # 同步CSV文件
./manage.py clean-remote-csv # 清理远程CSV文件
./manage.py help            # 查看帮助
"""
import os
import sys
from pathlib import Path

# ========== 基础配置 & 路径初始化 ==========
MAIN_REPO_ROOT = Path(__file__).parent
sys.path.insert(0, str(MAIN_REPO_ROOT))  # 加入主目录到Python路径

# 导入配置和子模块
from config.settings import COLORS
from manager import cron_manager, csv_manager

# ========== 通用工具函数 ==========
def print_color(msg: str, color: str = "NC") -> None:
    """带颜色打印（引用settings统一颜色）"""
    print(f"{COLORS.get(color, COLORS['NC'])}{msg}{COLORS['NC']}")

# ========== 初始化函数（仅权限+日志目录） ==========
def starter():
    """初始化：给脚本加执行权限 + 创建日志目录"""
    print_color("===== 开始初始化脚本执行权限 =====", "YELLOW")
    
    # 1. 给核心脚本加权限
    scripts = [
        Path(__file__).absolute(),  # manage.py
        MAIN_REPO_ROOT / "scripts" / "sync_csv_from_remote.py",
        *list((MAIN_REPO_ROOT / "manager").glob("*.py"))  # manager下所有py文件
    ]
    for script in scripts:
        if script.exists():
            os.chmod(script, 0o755)
            print_color(f"✅ 已给{script.name}赋予执行权限", "GREEN")
    
    # 2. 创建日志目录
    from config.settings import LOG_DIR
    os.makedirs(LOG_DIR, exist_ok=True)
    print_color(f"✅ 已创建日志目录：{LOG_DIR}", "GREEN")
    print_color("===== 脚本权限初始化完成 =====", "GREEN")

# ========== 帮助函数（更新改名后的命令） ==========
def show_help():
    """展示帮助信息"""
    help_text = f"""
{COLORS['YELLOW']}===== 主项目一键管理脚本 ====={COLORS['NC']}
用法：./manage.py [命令]
命令列表：
  starter           - 初始化：给所有脚本赋予执行权限（首次部署必做）
  set-cron          - 配置定时任务（从settings.py读取配置）
  check-cron        - 检查当前定时任务配置
  cancel-cron       - 取消本项目的crontab任务（保留服务器其他任务）
  clear-all-cron    - {COLORS['BOLD_RED']}删除所有crontab任务（高危，带警告）{COLORS['NC']}
  sync-now          - 手动同步CSV文件（从/opt/csv_repo拉取并复制到主项目）
  clean-remote-csv  - 删除/opt/csv_repo/result下的所有CSV文件（方便同步）
  help              - 查看帮助
    """
    print(help_text.strip())

# ========== 命令映射 & 主函数（更新set-cron） ==========
def main():
    if len(sys.argv) < 2:
        show_help()
        sys.exit(0)
    
    command = sys.argv[1]
    command_map = {
        "starter": starter,
        "set-cron": cron_manager.set_cron,  # 改名：config-cron → set-cron
        "check-cron": cron_manager.check_cron,
        "cancel-cron": cron_manager.cancel_cron,
        "clear-all-cron": cron_manager.clear_all_cron,
        "sync-now": csv_manager.sync_now,
        "clean-remote-csv": csv_manager.clean_remote_csv,
        "help": show_help
    }
    
    if command in command_map:
        try:
            command_map[command]()
        except Exception as e:
            print_color(f"❌ 执行命令{command}失败：{str(e)}", "RED")
            sys.exit(1)
    else:
        print_color(f"❌ 未知命令：{command}", "RED")
        show_help()
        sys.exit(1)

if __name__ == "__main__":
    main()