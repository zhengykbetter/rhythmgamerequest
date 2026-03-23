#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主项目一键管理脚本（Python版，替代原manage.sh）
用法：
chmod +x manage.py
./manage.py starter          # 初始化：给所有脚本赋予执行权限（首次部署必做）
./manage.py config-cron     # 配置定时任务（从settings.py读取配置）
./manage.py check-cron      # 检查cron
./manage.py sync-now        # 手动同步
./manage.py cancel-cron     # 取消本项目的crontab任务（保留其他任务）
./manage.py clear-all-cron  # 删所有crontab（高危，带警告）
./manage.py clean-remote-csv # 删外部result目录的所有CSV（方便同步）
./manage.py help            # 查看帮助
"""
import os
import sys
import shutil
import subprocess
import datetime
from pathlib import Path

# ========== 基础配置 & 颜色输出 ==========
# 强制将主仓库根目录加入Python路径（解决ModuleNotFoundError）
MAIN_REPO_ROOT = Path(__file__).parent
sys.path.insert(0, str(MAIN_REPO_ROOT))

# 导入settings配置（核心：直接导入，替代shell的python -c读取）
from config.settings import (
    PYTHON_EXEC_PATH, MAIN_REPO_ROOT as CONFIG_MAIN_REPO_ROOT,
    LOG_DIR, CRON_BACKUP_DIR, CRON_TASKS, CRON_TASK_MARK,
    CSV_SOURCE_DIR
)

# 颜色输出（ANSI码）
COLORS = {
    "RED": "\033[0;31m",
    "GREEN": "\033[0;32m",
    "YELLOW": "\033[1;33m",
    "BOLD_RED": "\033[1;31m",
    "NC": "\033[0m"  # 重置颜色
}

# 临时文件路径（替代原shell的TEMP_CRON_FILE）
TEMP_CRON_FILE = MAIN_REPO_ROOT / "scripts" / "temp_cron_config"


def print_color(msg: str, color: str = "NC") -> None:
    """带颜色打印输出"""
    print(f"{COLORS.get(color, COLORS['NC'])}{msg}{COLORS['NC']}")


# ========== 核心功能函数 ==========
def starter():
    """初始化：给所有脚本赋予执行权限 + 创建日志目录"""
    print_color("===== 开始初始化脚本执行权限 =====", "YELLOW")
    
    # 给manage.py本身加执行权限
    manage_path = Path(__file__).absolute()
    os.chmod(manage_path, 0o755)
    print_color(f"✅ 已给{manage_path}赋予执行权限", "GREEN")
    
    # 同步脚本路径
    sync_script = CONFIG_MAIN_REPO_ROOT / "scripts" / "sync_csv_from_remote.py"
    if sync_script.exists():
        os.chmod(sync_script, 0o755)
        print_color(f"✅ 已给{sync_script}赋予执行权限", "GREEN")
    else:
        print_color(f"ℹ️  同步脚本{sync_script}不存在，跳过权限设置", "YELLOW")
    
    # CSV→DB脚本权限（可选）
    csv_to_db_script = CONFIG_MAIN_REPO_ROOT / "scripts" / "csv_to_db.py"
    if csv_to_db_script.exists():
        os.chmod(csv_to_db_script, 0o755)
        print_color(f"✅ 已给{csv_to_db_script}赋予执行权限", "GREEN")
    
    # 创建日志目录
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        print_color(f"✅ 已创建日志目录：{LOG_DIR}", "GREEN")
    except Exception as e:
        # 容错：创建默认日志目录
        default_log = MAIN_REPO_ROOT / "logs"
        os.makedirs(default_log, exist_ok=True)
        print_color(f"ℹ️  配置读取失败，已创建默认日志目录：{default_log} | 错误：{str(e)}", "YELLOW")
    
    print_color("===== 脚本权限初始化完成 =====", "GREEN")


def config_cron():
    """配置定时任务：备份原有crontab + 导入本项目任务"""
    # 先执行初始化
    starter()
    print_color("===== 开始配置定时任务（从settings.py读取）=====", "YELLOW")
    
    # 1. 验证CRON_TASKS配置
    if not CRON_TASKS:
        print_color("❌ 读取cron配置失败！settings.py中CRON_TASKS为空", "RED")
        sys.exit(1)
    
    # 2. 创建cron备份目录
    try:
        os.makedirs(CRON_BACKUP_DIR, exist_ok=True)
    except Exception:
        CRON_BACKUP_DIR = MAIN_REPO_ROOT / "logs"
        os.makedirs(CRON_BACKUP_DIR, exist_ok=True)
    
    # 3. 备份原有crontab
    backup_file = CRON_BACKUP_DIR / f"cron_backup_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    try:
        # 执行crontab -l并保存到备份文件
        result = subprocess.run(
            ["crontab", "-l"], capture_output=True, text=True, check=False
        )
        with open(backup_file, "w", encoding="utf-8") as f:
            f.write(result.stdout)
        print_color(f"✅ 已备份原有crontab到{backup_file}", "GREEN")
    except Exception as e:
        print_color(f"ℹ️  crontab备份失败（可能无原有任务）：{str(e)}", "YELLOW")
    
    # 4. 过滤原有crontab（保留非本项目任务）
    existing_cron = ""
    try:
        result = subprocess.run(["crontab", "-l"], capture_output=True, text=True, check=False)
        existing_cron = "\n".join([
            line for line in result.stdout.splitlines()
            if CRON_TASK_MARK not in line
        ])
    except Exception:
        existing_cron = ""  # 无原有任务
    
    # 5. 生成新的cron配置文件
    os.makedirs(TEMP_CRON_FILE.parent, exist_ok=True)
    with open(TEMP_CRON_FILE, "w", encoding="utf-8") as f:
        # 写入原有非本项目任务
        if existing_cron:
            f.write(existing_cron.strip() + "\n")
        # 写入本项目任务
        f.write("\n# 节奏游戏项目定时任务（自动生成，请勿手动修改）\n")
        f.write("\n".join(CRON_TASKS) + "\n")
    
    # 6. 导入新的cron配置
    try:
        subprocess.run(
            ["crontab", str(TEMP_CRON_FILE)],
            check=True, capture_output=True, text=True
        )
        print_color("✅ 定时任务配置成功！", "GREEN")
        # 展示本项目cron配置
        print_color("当前本项目定时任务配置：", "YELLOW")
        result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
        cron_lines = [line for line in result.stdout.splitlines() if CRON_TASK_MARK in line]
        print("\n".join(cron_lines) if cron_lines else "无本项目任务")
    except subprocess.CalledProcessError as e:
        print_color(f"❌ 定时任务配置失败！已恢复原有配置 | 错误：{e.stderr}", "RED")
        # 恢复备份
        if backup_file.exists():
            subprocess.run(["crontab", str(backup_file)], check=False)
        sys.exit(1)
    finally:
        # 删除临时文件
        if TEMP_CRON_FILE.exists():
            os.remove(TEMP_CRON_FILE)


def check_cron():
    """检查cron配置：对比服务器实际配置和settings.py配置"""
    print_color("===== 当前服务器定时任务配置（本项目）=====", "YELLOW")
    try:
        result = subprocess.run(["crontab", "-l"], capture_output=True, text=True, check=False)
        cron_lines = [line for line in result.stdout.splitlines() if CRON_TASK_MARK in line]
        if cron_lines:
            print("\n".join(cron_lines))
        else:
            print_color("❌ 未检测到本项目定时任务", "RED")
    except Exception as e:
        print_color(f"❌ 读取crontab失败：{str(e)}", "RED")
    
    print_color("\n===== settings.py中的cron配置 =====", "YELLOW")
    print("\n".join(CRON_TASKS) if CRON_TASKS else "❌ 读取settings.py配置失败")


def sync_now():
    """手动同步CSV：调用sync_csv_from_remote.py"""
    # 先执行初始化
    starter()
    print_color("===== 开始手动执行CSV同步 =====", "YELLOW")
    
    # 确保日志目录存在
    os.makedirs(LOG_DIR, exist_ok=True)
    
    # 同步脚本路径
    sync_script = CONFIG_MAIN_REPO_ROOT / "scripts" / "sync_csv_from_remote.py"
    if not sync_script.exists():
        print_color(f"❌ 同步脚本不存在：{sync_script}", "RED")
        sys.exit(1)
    
    # 执行同步脚本
    try:
        result = subprocess.run(
            [PYTHON_EXEC_PATH, str(sync_script)],
            check=True, capture_output=True, text=True
        )
        # 打印脚本输出
        print(result.stdout)
        log_file = LOG_DIR / f"sync_csv_{datetime.datetime.now().strftime('%Y%m%d')}.log"
        print_color(f"✅ CSV手动同步完成！日志请查看{log_file}", "GREEN")
    except subprocess.CalledProcessError as e:
        print_color(f"❌ CSV手动同步失败！错误输出：{e.stderr}", "RED")
        sys.exit(1)


def cancel_cron():
    """取消本项目cron任务：保留其他任务，仅删除本项目标记的任务"""
    print_color("===== 开始取消本项目的crontab任务 =====", "YELLOW")
    
    # 创建备份目录
    try:
        os.makedirs(CRON_BACKUP_DIR, exist_ok=True)
    except Exception:
        CRON_BACKUP_DIR = MAIN_REPO_ROOT / "logs"
        os.makedirs(CRON_BACKUP_DIR, exist_ok=True)
    
    # 备份当前crontab
    backup_file = CRON_BACKUP_DIR / f"cron_backup_before_cancel_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    try:
        result = subprocess.run(["crontab", "-l"], capture_output=True, text=True, check=False)
        with open(backup_file, "w", encoding="utf-8") as f:
            f.write(result.stdout)
        print_color(f"✅ 已备份当前crontab到{backup_file}", "GREEN")
    except Exception as e:
        print_color(f"ℹ️  crontab备份失败：{str(e)}", "YELLOW")
    
    # 过滤本项目任务
    try:
        result = subprocess.run(["crontab", "-l"], capture_output=True, text=True, check=False)
        new_cron = "\n".join([
            line for line in result.stdout.splitlines()
            if CRON_TASK_MARK not in line
        ]).strip()
        
        # 导入过滤后的crontab
        if new_cron:
            # 写入临时文件再导入
            with open(TEMP_CRON_FILE, "w", encoding="utf-8") as f:
                f.write(new_cron + "\n")
            subprocess.run(["crontab", str(TEMP_CRON_FILE)], check=True)
        else:
            # 无剩余任务，清空crontab
            subprocess.run(["crontab", "-r"], check=True)
        
        print_color("✅ 本项目的crontab任务已成功取消！", "GREEN")
        # 展示剩余任务
        print_color("当前剩余定时任务：", "YELLOW")
        result = subprocess.run(["crontab", "-l"], capture_output=True, text=True, check=False)
        print(result.stdout if result.stdout else "❌ 暂无剩余定时任务")
    except subprocess.CalledProcessError as e:
        print_color(f"❌ 取消本项目crontab任务失败！已恢复原有配置 | 错误：{e.stderr}", "RED")
        # 恢复备份
        if backup_file.exists():
            subprocess.run(["crontab", str(backup_file)], check=False)
        sys.exit(1)
    finally:
        # 删除临时文件
        if TEMP_CRON_FILE.exists():
            os.remove(TEMP_CRON_FILE)


def clear_all_cron():
    """高危：删除所有crontab任务（带警告+二次确认）"""
    print_color("===== 高危操作警告 =====", "BOLD_RED")
    print_color("此命令将删除当前用户的所有crontab任务（包括其他项目）！", "BOLD_RED")
    print_color(f"备份目录：{CRON_BACKUP_DIR}", "YELLOW")
    
    # 备份所有crontab
    try:
        os.makedirs(CRON_BACKUP_DIR, exist_ok=True)
        backup_file = CRON_BACKUP_DIR / f"cron_full_backup_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        result = subprocess.run(["crontab", "-l"], capture_output=True, text=True, check=False)
        with open(backup_file, "w", encoding="utf-8") as f:
            f.write(result.stdout)
        print_color(f"✅ 已备份所有crontab到{backup_file}", "GREEN")
    except Exception as e:
        print_color(f"ℹ️  crontab全量备份失败：{str(e)}", "YELLOW")
    
    # 二次确认
    confirm = input(COLORS["BOLD_RED"] + "请输入 YES 确认删除所有crontab：" + COLORS["NC"])
    if confirm != "YES":
        print_color("ℹ️  用户取消操作，未删除任何crontab", "YELLOW")
        return
    
    # 执行删除
    try:
        subprocess.run(["crontab", "-r"], check=True, capture_output=True, text=True)
        print_color("✅ 所有crontab任务已成功删除！", "GREEN")
        # 验证
        result = subprocess.run(["crontab", "-l"], capture_output=True, text=True, check=False)
        if result.stdout:
            print_color(f"ℹ️  仍有残留任务：{result.stdout}", "YELLOW")
        else:
            print_color("✅ 验证：无任何crontab任务", "GREEN")
    except subprocess.CalledProcessError as e:
        print_color(f"❌ 删除所有crontab失败！已恢复备份 | 错误：{e.stderr}", "RED")
        # 恢复备份
        if backup_file.exists():
            subprocess.run(["crontab", str(backup_file)], check=False)
        sys.exit(1)


def clean_remote_csv():
    """删除外部result目录的所有CSV文件（方便同步，带确认）"""
    print_color("===== 开始删除外部result目录的CSV文件 =====", "YELLOW")
    
    # 验证CSV源目录
    if not CSV_SOURCE_DIR or not Path(CSV_SOURCE_DIR).exists():
        print_color(f"❌ CSV源目录不存在：{CSV_SOURCE_DIR}", "RED")
        return
    
    # 查找CSV文件
    csv_files = list(Path(CSV_SOURCE_DIR).glob("*.csv"))
    if not csv_files:
        print_color(f"ℹ️  CSV源目录{CSV_SOURCE_DIR}下无CSV文件，无需删除", "YELLOW")
        return
    
    # 列出要删除的文件
    print_color("即将删除以下文件：", "YELLOW")
    for f in csv_files:
        print(f"  {f}")
    
    # 确认删除
    confirm = input(COLORS["YELLOW"] + "请输入 YES 确认删除：" + COLORS["NC"])
    if confirm != "YES":
        print_color("ℹ️  用户取消操作，未删除任何文件", "YELLOW")
        return
    
    # 执行删除
    try:
        for f in csv_files:
            os.remove(f)
        print_color(f"✅ 已成功删除{CSV_SOURCE_DIR}下的所有CSV文件", "GREEN")
    except Exception as e:
        print_color(f"❌ 删除CSV文件失败：{str(e)}", "RED")
        sys.exit(1)


def show_help():
    """展示帮助信息"""
    help_text = f"""
{COLORS['YELLOW']}===== 主项目一键管理脚本（Python版） ====={COLORS['NC']}
用法：./manage.py [命令]
命令列表：
  starter           - 初始化：给所有脚本赋予执行权限（首次部署必做）
  config-cron       - 配置定时任务（从settings.py读取配置）
  check-cron        - 检查当前定时任务配置
  sync-now          - 手动执行CSV同步（拉取私有仓库+复制到主仓库）
  cancel-cron       - 取消本项目的crontab任务（保留服务器其他任务）
  clear-all-cron    - {COLORS['BOLD_RED']}删除所有crontab任务（高危，带警告）{COLORS['NC']}
  clean-remote-csv  - 删除外部result目录的所有CSV文件（方便同步）
  help              - 查看帮助
    """
    print(help_text.strip())


# ========== 主函数：解析命令行参数 ==========
def main():
    if len(sys.argv) < 2:
        show_help()
        sys.exit(0)
    
    command = sys.argv[1]
    command_map = {
        "starter": starter,
        "config-cron": config_cron,
        "check-cron": check_cron,
        "sync-now": sync_now,
        "cancel-cron": cancel_cron,
        "clear-all-cron": clear_all_cron,
        "clean-remote-csv": clean_remote_csv,
        "help": show_help
    }
    
    if command in command_map:
        command_map[command]()
    else:
        print_color(f"❌ 未知命令：{command}", "RED")
        show_help()
        sys.exit(1)


if __name__ == "__main__":
    main()