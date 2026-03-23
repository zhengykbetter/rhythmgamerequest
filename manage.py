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
- update-db：CSV同步到数据库（支持 all/单表名/clear 参数）
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
# 新增：导入db_manager（具体实现后续补充，先占位）
from manager import db_manager

# ========== 通用工具函数 ==========
# （保留原有print_color、confirm_action函数，无改动）
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

# ========== 新增：update-db指令处理函数 ==========
def handle_update_db(*args):
    """
    处理update-db指令（参数支持：all/单表名/clear）
    :param args: 指令参数，如 ('all',) / ('game_song_rel',) / ('clear',)
    """
    if not args:
        print_color("❌ update-db指令需指定参数：all/表名/clear（如 update-db all）", "RED")
        return
    
    sub_cmd = args[0].lower()
    print_color(f"\n===== 执行update-db指令：{sub_cmd} =====\n", "YELLOW")
    
    try:
        if sub_cmd == "all":
            # 后续在db_manager实现：同步所有表的CSV到DB
            db_manager.update_db_all()
            print_color("✅ update-db all 执行完成：所有CSV同步到数据库", "GREEN")
        elif sub_cmd == "clear":
            # 后续在db_manager实现：清空数据库所有表数据
            if confirm_action("⚠️  确认清空数据库所有表数据？（高危操作）(y/n) "):
                db_manager.clear_db_all()
                print_color("✅ update-db clear 执行完成：数据库已清空", "GREEN")
            else:
                print_color("🚫 取消执行update-db clear", "YELLOW")
        else:
            # 后续在db_manager实现：同步指定单表的CSV到DB
            table_name = sub_cmd
            db_manager.update_db_single(table_name)
            print_color(f"✅ update-db {table_name} 执行完成：指定表同步到数据库", "GREEN")
    except Exception as e:
        print_color(f"❌ update-db {sub_cmd} 执行失败：{str(e)}", "RED")

# ========== 自动更新流程函数（修改：追加update-db all询问） ==========
def auto_update():
    """自动更新流程：sync-now、split_csv + 追加update-db all询问"""
    print_color("===== 执行auto-update指令：自动更新流程 =====", "YELLOW")
    
    # 基础更新步骤：sync-now + split_csv
    update_steps = [
        ("sync-now", "同步远程CSV文件到本地", sync_now),
        ("split_csv", "拆分原始CSV为标准化表", split_csv)
    ]
    
    # 依次询问并执行基础步骤
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
    
    # 新增：询问是否执行update-db all
    if confirm_action("\n是否执行「update-db all」（同步所有CSV到数据库）？(y/n) "):
        print_color("\n📌 开始执行：update-db all", "GREEN")
        try:
            db_manager.update_db_all()
            print_color("✅ update-db all 执行完成", "GREEN")
        except Exception as e:
            print_color(f"❌ update-db all 执行失败：{str(e)}", "RED")
    else:
        print_color("🚫 取消执行update-db all", "YELLOW")
    
    print_color("\n🎉 auto-update自动更新流程结束！", "GREEN")

# ========== 帮助信息函数（修改：补充update-db说明） ==========
def show_help():
    """展示帮助信息（新增update-db指令说明）"""
    print_color("\n===== 管理员模式 - 指令列表 =====\n", "YELLOW")
    # 按类别分组展示指令
    base_cmds = [(k, v[0]) for k, v in COMMAND_MAP.items() if k in ["starter", "help", "exit", "quit", "q"]]
    cron_cmds = [(k, v[0]) for k, v in COMMAND_MAP.items() if "cron" in k]
    csv_cmds = [(k, v[0]) for k, v in COMMAND_MAP.items() if k in ["sync-now", "clean-remote-csv", "clean-csv", "split_csv", "auto-update"]]
    # 新增：DB管理指令分组
    db_cmds = [(k, v[0]) for k, v in COMMAND_MAP.items() if k == "update-db"]
    
    print_color("【基础指令】", "GREEN")
    for cmd, desc in base_cmds:
        print(f"  {cmd:<15} - {desc}")
    
    print_color("\n【Cron定时任务指令】", "GREEN")
    for cmd, desc in cron_cmds:
        print(f"  {cmd:<15} - {desc}")
    
    print_color("\n【CSV管理指令】", "GREEN")
    for cmd, desc in csv_cmds:
        print(f"  {cmd:<15} - {desc}")
    
    # 新增：DB管理指令说明
    print_color("\n【数据库同步指令】", "GREEN")
    for cmd, desc in db_cmds:
        print(f"  {cmd:<15} - {desc}")
        print(f"    ➤ 用法：update-db all（同步所有表）/ update-db 表名（同步单表）/ update-db clear（清空数据库）")
    
    print_color("\n注：输入 exit/quit/q 可退出管理员模式\n", "YELLOW")

# ========== 核心命令映射（修改：新增update-db） ==========
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
    "sync-now": ("同步远程CSV文件到本地", sync_now),
    "clean-remote-csv": ("清理远程CSV文件", clean_remote_csv),
    "clean-csv": ("清理本地data_csv目录下的CSV文件", clean_csv),
    "split_csv": ("拆分原始CSV为标准化表", split_csv),
    "auto-update": ("自动更新流程（sync-now + split_csv + update-db all）", auto_update),
    # 新增：数据库同步指令（带参数，func为handle_update_db）
    "update-db": ("CSV同步到数据库（支持all/表名/clear参数）", handle_update_db)
}

# ========== 交互式管理员模式（修改：支持带参数指令） ==========
def interactive_mode():
    """交互式管理员模式核心逻辑：支持带参数指令（如update-db all）"""
    # 欢迎语 + 自动初始化（保留原有逻辑，无改动）
    print_color("="*50, "YELLOW")
    print_color("欢迎进入项目管理员模式！", "GREEN")
    print_color("="*50, "YELLOW")
    
    print_color("\n📌 正在自动执行脚本权限初始化（减少手动操作）...\n", "YELLOW")
    try:
        starter()
    except Exception as e:
        print_color(f"⚠️  自动初始化失败（可手动执行 starter 指令重试）：{str(e)}\n", "RED")
    
    print_color("输入 'help' 查看所有可用指令，输入 'exit/quit/q' 退出\n", "YELLOW")
    
    # 循环接收指令（修改：支持拆分指令+参数）
    while True:
        try:
            cmd_input = input(COLORS["GREEN"] + "\n管理员 > " + COLORS["NC"]).strip()
            if not cmd_input:
                continue
            
            # 拆分指令和参数（如 "update-db all" → cmd="update-db", args=["all"]）
            cmd_parts = cmd_input.strip().split()
            cmd = cmd_parts[0].lower()
            args = cmd_parts[1:] if len(cmd_parts) > 1 else []
            
            # 退出指令
            if cmd in ["exit", "quit", "q"]:
                print_color("感谢使用，管理员模式已退出！", "GREEN")
                break
            
            # 执行指令（支持带参数）
            if cmd not in COMMAND_MAP:
                print_color(f"❌ 未知指令：{cmd}，输入 'help' 查看可用指令", "RED")
                continue
            
            cmd_desc, cmd_func = COMMAND_MAP[cmd]
            if cmd_func:
                print_color(f"\n===== 执行指令：{cmd_input}（{cmd_desc}）=====", "YELLOW")
                try:
                    # 调用指令函数，传递参数
                    cmd_func(*args)
                    print_color(f"\n✅ 指令 '{cmd_input}' 执行完成", "GREEN")
                except Exception as e:
                    print_color(f"\n❌ 指令 '{cmd_input}' 执行失败：{str(e)}", "RED")
        except KeyboardInterrupt:
            print_color("\n\n⚠️  检测到中断信号，管理员模式将退出", "YELLOW")
            break
        except EOFError:
            print_color("\n\n⚠️  检测到EOF，管理员模式将退出", "YELLOW")
            break

# ========== 主函数（修改：支持命令行模式带参数） ==========
def main():
    # 模式1：命令行模式（支持带参数，如 ./manage.py update-db all）
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        args = sys.argv[2:] if len(sys.argv) > 2 else []
        
        if command in COMMAND_MAP:
            cmd_desc, cmd_func = COMMAND_MAP[command]
            if cmd_func:
                try:
                    cmd_func(*args)  # 传递参数给指令函数
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

# ========== 保留原有starter函数（无改动） ==========
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

if __name__ == "__main__":
    main()