#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主项目管理入口（支持两种模式）：
1. 命令行模式：./manage.py [指令]（如 ./manage.py set-cron）
2. 交互式模式：./manage.py（无参数，自动初始化+进入管理员交互界面）
"""
import os
import sys
import readline  # 优化输入体验（支持上下键历史记录）
from pathlib import Path

# ========== 基础配置 & 路径初始化 ==========
MAIN_REPO_ROOT = Path(__file__).parent
sys.path.insert(0, str(MAIN_REPO_ROOT))  # 加入主目录到Python路径

# 导入配置和子模块
from config.settings import COLORS
from manager import cron_manager, csv_manager

# ========== 通用工具函数（最先定义） ==========
def print_color(msg: str, color: str = "NC") -> None:
    """带颜色打印（引用settings统一颜色）"""
    print(f"{COLORS.get(color, COLORS['NC'])}{msg}{COLORS['NC']}")

# ========== 核心功能函数（先定义，后引用） ==========
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
    print_color("===== 脚本权限初始化完成 =====\n", "GREEN")

def show_help():
    """展示帮助信息（支持交互式/命令行模式）"""
    print_color("\n===== 管理员模式 - 指令列表 =====\n", "YELLOW")
    # 按类别分组展示指令
    base_cmds = [(k, v[0]) for k, v in COMMAND_MAP.items() if k in ["starter", "help", "exit", "quit", "q"]]
    cron_cmds = [(k, v[0]) for k, v in COMMAND_MAP.items() if "cron" in k]
    csv_cmds = [(k, v[0]) for k, v in COMMAND_MAP.items() if "csv" in k or k == "sync-now"]
    
    print_color("【基础指令】", "GREEN")
    for cmd, desc in base_cmds:
        print(f"  {cmd:<15} - {desc}")
    
    print_color("\n【Cron定时任务指令】", "GREEN")
    for cmd, desc in cron_cmds:
        print(f"  {cmd:<15} - {desc}")
    
    print_color("\n【CSV文件管理指令】", "GREEN")
    for cmd, desc in csv_cmds:
        print(f"  {cmd:<15} - {desc}")
    
    print_color("\n注：输入 exit/quit/q 可退出管理员模式\n", "YELLOW")

# ========== 核心命令映射（最后定义，确保所有函数已定义） ==========
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
    # CSV相关指令
    "sync-now": ("同步CSV文件", csv_manager.sync_now),
    "clean-remote-csv": ("清理远程CSV文件", csv_manager.clean_remote_csv)
}

# ========== 交互式管理员模式（自动执行starter） ==========
def interactive_mode():
    """交互式管理员模式核心逻辑：自动初始化 + 指令交互"""
    # 欢迎语
    print_color("="*50, "YELLOW")
    print_color("欢迎进入项目管理员模式！", "GREEN")
    print_color("="*50, "YELLOW")
    
    # 核心修改：自动执行starter初始化
    print_color("\n📌 正在自动执行脚本权限初始化（减少手动操作）...\n", "YELLOW")
    try:
        starter()  # 自动运行初始化函数
    except Exception as e:
        print_color(f"⚠️  自动初始化失败（可手动执行 starter 指令重试）：{str(e)}\n", "RED")
    
    # 提示使用说明
    print_color("输入 'help' 查看所有可用指令，输入 'exit/quit/q' 退出\n", "YELLOW")
    
    # 循环接收指令
    while True:
        try:
            # 提示输入指令
            cmd_input = input(COLORS["GREEN"] + "\n管理员 > " + COLORS["NC"]).strip()
            if not cmd_input:  # 空输入，重新提示
                continue
            
            # 转换为小写（兼容大小写输入）
            cmd = cmd_input.lower()
            
            # 退出指令
            if cmd in ["exit", "quit", "q"]:
                print_color("感谢使用，管理员模式已退出！", "GREEN")
                break
            
            # 检查指令是否存在
            if cmd not in COMMAND_MAP:
                print_color(f"❌ 未知指令：{cmd}，输入 'help' 查看可用指令", "RED")
                continue
            
            # 执行指令（排除退出指令）
            cmd_desc, cmd_func = COMMAND_MAP[cmd]
            if cmd_func:
                print_color(f"\n===== 执行指令：{cmd}（{cmd_desc}）=====", "YELLOW")
                try:
                    cmd_func()  # 执行指令函数
                    print_color(f"\n✅ 指令 '{cmd}' 执行完成", "GREEN")
                except Exception as e:
                    print_color(f"\n❌ 指令 '{cmd}' 执行失败：{str(e)}", "RED")
            else:
                # 理论上不会走到这里（退出指令已提前处理）
                pass
        
        except KeyboardInterrupt:  # 捕获Ctrl+C
            print_color("\n\n⚠️  检测到中断信号，管理员模式将退出", "YELLOW")
            break
        except EOFError:  # 捕获Ctrl+D
            print_color("\n\n⚠️  检测到EOF，管理员模式将退出", "YELLOW")
            break

# ========== 主函数（适配两种模式） ==========
def main():
    # 模式1：命令行模式（传参数）
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
                # 命令行模式下输入exit/quit/q，直接退出
                print_color("退出程序", "GREEN")
                sys.exit(0)
        else:
            print_color(f"❌ 未知命令：{command}，输入 './manage.py help' 查看帮助", "RED")
            sys.exit(1)
    # 模式2：交互式模式（无参数）
    else:
        interactive_mode()

if __name__ == "__main__":
    main()