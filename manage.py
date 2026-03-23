#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主项目管理入口（支持两种模式）：
1. 命令行模式：./manage.py [指令]（如 ./manage.py set-cron）
2. 交互式模式：./manage.py（无参数，自动初始化+进入管理员交互界面）
新增指令：
- split_csv：调用csv_manager.split_csv生成标准化CSV表
- auto-update：自动询问执行sync-now、split_csv（支持拓展）
- clean-csv：清理本地data_csv目录下的CSV文件
"""
import os
import sys
import readline  # 优化输入体验（支持上下键历史记录）
from pathlib import Path

# ========== 基础配置 & 路径初始化 ==========
MAIN_REPO_ROOT = Path(__file__).parent  # manage.py所在目录 = 主项目根目录
sys.path.insert(0, str(MAIN_REPO_ROOT))  # 加入Python路径

# 导入配置和子模块
from config.settings import COLORS, CSV_TARGET_DIR
from manager import cron_manager
# 导入csv_manager的所有函数（含clean_csv）
from manager.csv_manager import sync_now, clean_remote_csv, clean_csv, split_csv

# ========== 通用工具函数 ==========
def print_color(msg: str, color: str = "NC") -> None:
    """带颜色打印（引用settings统一颜色）"""
    print(f"{COLORS.get(color, COLORS['NC'])}{msg}{COLORS['NC']}")

def confirm_action(prompt: str) -> bool:
    """交互式确认：返回True（确认）/False（取消），兼容y/Y/是/yes等输入"""
    while True:
        user_input = input(COLORS["YELLOW"] + prompt + COLORS["NC"]).strip().lower()
        if user_input in ["y", "yes", "是", "确认"]:
            return True
        elif user_input in ["n", "no", "否", "取消"]:
            return False
        else:
            print_color("❌ 输入无效，请输入 y/yes/是 或 n/no/否", "RED")

# ========== 核心初始化函数 ==========
def starter():
    """初始化：给脚本加执行权限 + 创建日志目录"""
    print_color("===== 开始初始化脚本执行权限 =====", "YELLOW")
    
    # 1. 给核心脚本加权限（包含extract_song_data.py）
    scripts = [
        Path(__file__).absolute(),  # manage.py
        MAIN_REPO_ROOT / "scripts" / "sync_csv_from_remote.py",
        MAIN_REPO_ROOT / "scripts" / "extract_song_data.py",  # split_csv依赖的脚本
        *list((MAIN_REPO_ROOT / "manager").glob("*.py"))
    ]
    for script in scripts:
        if script.exists():
            os.chmod(script, 0o755)
            print_color(f"✅ 已给{script.name}赋予执行权限", "GREEN")
    
    # 2. 创建输出目录
    os.makedirs(CSV_TARGET_DIR, exist_ok=True)
    print_color(f"✅ 已创建CSV目标目录：{CSV_TARGET_DIR}", "GREEN")
    print_color("===== 脚本权限初始化完成 =====\n", "GREEN")

# ========== 自动更新流程函数 ==========
def auto_update():
    """自动更新流程：仅询问sync-now、split_csv（移除clean-csv步骤）"""
    print_color("===== 执行auto-update指令：自动更新流程 =====", "YELLOW")
    
    # 移除clean-csv步骤，仅保留sync-now和split_csv
    update_steps = [
        ("sync-now", "同步远程CSV文件到本地", sync_now),
        ("split_csv", "拆分原始CSV为标准化表", split_csv)
    ]
    
    # 依次询问并执行每个步骤
    for cmd, desc, cmd_func in update_steps:
        if confirm_action(f"\n是否执行「{cmd}」（{desc}）？(y/n) "):
            print_color(f"\n📌 开始执行：{cmd}（{desc}）", "GREEN")
            try:
                cmd_func()
                print_color(f"✅ {cmd} 执行完成", "GREEN")
            except Exception as e:
                print_color(f"❌ {cmd} 执行失败：{str(e)}", "RED")
        else:
            print_color(f"🚫 取消执行：{cmd}", "YELLOW")
    
    print_color("\n🎉 auto-update自动更新流程结束！", "GREEN")

def show_help():
    """展示帮助信息（保留clean-csv指令说明）"""
    print_color("\n===== 管理员模式 - 指令列表 =====\n", "YELLOW")
    # 按类别分组展示指令
    base_cmds = [(k, v[0]) for k, v in COMMAND_MAP.items() if k in ["starter", "help", "exit", "quit", "q"]]
    cron_cmds = [(k, v[0]) for k, v in COMMAND_MAP.items() if "cron" in k]
    csv_cmds = [(k, v[0]) for k, v in COMMAND_MAP.items() if k in ["sync-now", "clean-remote-csv", "clean-csv", "split_csv", "auto-update"]]
    
    print_color("【基础指令】", "GREEN")
    for cmd, desc in base_cmds:
        print(f"  {cmd:<15} - {desc}")
    
    print_color("\n【Cron定时任务指令】", "GREEN")
    for cmd, desc in cron_cmds:
        print(f"  {cmd:<15} - {desc}")
    
    print_color("\n【CSV管理指令】", "GREEN")
    for cmd, desc in csv_cmds:
        print(f"  {cmd:<15} - {desc}")
    
    print_color("\n注：输入 exit/quit/q 可退出管理员模式\n", "YELLOW")

# ========== 核心命令映射 ==========
COMMAND_MAP = {
    # 基础指令
    "starter": ("初始化脚本权限", starter),
    "help": ("查看帮助信息", show_help),
    "exit": ("退出管理员模式", None),
    "quit": ("退出管理员模式", None),
    "q": ("退出管理员模式", None),
    # Cron相关指令
    "set-cron": ("配置定时任务", cron_manager.set_cron),
    "check-cron": ("检查定时任务", cron_manager.check_cron),
    "cancel-cron": ("取消本项目定时任务", cron_manager.cancel_cron),
    "clear-all-cron": ("删除所有定时任务（高危）", cron_manager.clear_all_cron),
    # CSV相关指令（保留clean-csv，仅auto-update不调用）
    "sync-now": ("同步远程CSV文件到本地", sync_now),
    "clean-remote-csv": ("清理远程CSV文件", clean_remote_csv),
    "clean-csv": ("清理本地data_csv目录下的CSV文件", clean_csv),
    "split_csv": ("拆分原始CSV为标准化表", split_csv),
    "auto-update": ("自动更新流程（sync-now + split_csv）", auto_update)
}

# ========== 交互式管理员模式 ==========
def interactive_mode():
    """交互式管理员模式核心逻辑：自动初始化 + 指令交互"""
    # 欢迎语
    print_color("="*50, "YELLOW")
    print_color("欢迎进入项目管理员模式！", "GREEN")
    print_color("="*50, "YELLOW")
    
    # 自动执行starter初始化
    print_color("\n📌 正在自动执行脚本权限初始化（减少手动操作）...\n", "YELLOW")
    try:
        starter()
    except Exception as e:
        print_color(f"⚠️  自动初始化失败（可手动执行 starter 指令重试）：{str(e)}\n", "RED")
    
    # 提示使用说明
    print_color("输入 'help' 查看所有可用指令，输入 'exit/quit/q' 退出\n", "YELLOW")
    
    # 循环接收指令
    while True:
        try:
            cmd_input = input(COLORS["GREEN"] + "\n管理员 > " + COLORS["NC"]).strip()
            if not cmd_input:
                continue
            
            cmd = cmd_input.lower()
            # 退出指令
            if cmd in ["exit", "quit", "q"]:
                print_color("感谢使用，管理员模式已退出！", "GREEN")
                break
            
            # 执行指令
            if cmd not in COMMAND_MAP:
                print_color(f"❌ 未知指令：{cmd}，输入 'help' 查看可用指令", "RED")
                continue
            
            cmd_desc, cmd_func = COMMAND_MAP[cmd]
            if cmd_func:
                print_color(f"\n===== 执行指令：{cmd}（{cmd_desc}）=====", "YELLOW")
                try:
                    cmd_func()
                    print_color(f"\n✅ 指令 '{cmd}' 执行完成", "GREEN")
                except Exception as e:
                    print_color(f"\n❌ 指令 '{cmd}' 执行失败：{str(e)}", "RED")
        except KeyboardInterrupt:
            print_color("\n\n⚠️  检测到中断信号，管理员模式将退出", "YELLOW")
            break
        except EOFError:
            print_color("\n\n⚠️  检测到EOF，管理员模式将退出", "YELLOW")
            break

# ========== 主函数（适配两种模式） ==========
def main():
    # 模式1：命令行模式
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        if command in COMMAND_MAP:
            cmd_desc, cmd_func = COMMAND_MAP[command]
            if cmd_func:
                try:
                    cmd_func()
                except Exception as e:
                    print_color(f"❌ 执行命令{command}失败：{str(e)}", "RED")
                    sys.exit(1)
            else:
                print_color("退出程序", "GREEN")
                sys.exit(0)
        else:
            print_color(f"❌ 未知命令：{command}，输入 './manage.py help' 查看帮助", "RED")
            sys.exit(1)
    # 模式2：交互式模式
    else:
        interactive_mode()

if __name__ == "__main__":
    main()