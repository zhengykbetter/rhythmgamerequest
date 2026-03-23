#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cron任务管理模块（独立处理所有Cron相关命令）
修复：new crontab file is missing newline before EOF 报错
"""
import os
import sys
import subprocess
import datetime
from pathlib import Path

# ========== 基础配置 ==========
MAIN_REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(MAIN_REPO_ROOT))

# 导入统一配置
from config.settings import (
    COLORS, LOG_DIR, CRON_BACKUP_DIR,
    CRON_TASKS, CRON_TASK_MARK
)

# 临时文件路径
TEMP_CRON_FILE = MAIN_REPO_ROOT / "scripts" / "temp_cron_config"

# ========== 通用工具函数 ==========
def print_color(msg: str, color: str = "NC") -> None:
    """带颜色打印（引用settings统一颜色）"""
    print(f"{COLORS.get(color, COLORS['NC'])}{msg}{COLORS['NC']}")

# ========== Cron核心功能（修复换行符+改名set-cron） ==========
def set_cron():  # 改名：config_cron → set_cron
    """配置定时任务：修复换行符报错 + 兼容空crontab"""
    print_color("===== 开始配置定时任务 =====", "YELLOW")
    if not CRON_TASKS:
        print_color("❌ CRON_TASKS配置为空，配置失败", "RED")
        sys.exit(1)
    
    # 1. 备份原有crontab（兼容空crontab：no crontab for root）
    os.makedirs(CRON_BACKUP_DIR, exist_ok=True)
    backup_file = CRON_BACKUP_DIR / f"cron_backup_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    try:
        # 兼容空crontab，避免报错
        subprocess.run(
            ["crontab", "-l"],
            stdout=open(backup_file, "w", encoding="utf-8"),
            stderr=subprocess.DEVNULL,  # 忽略no crontab for root的错误输出
            check=False
        )
        print_color(f"✅ 已备份原有crontab到{backup_file}", "GREEN")
    except Exception as e:
        print_color(f"ℹ️  crontab备份提示：{str(e)}（空crontab属正常）", "YELLOW")
    
    # 2. 读取原有crontab（兼容空crontab）
    result = subprocess.run(
        ["crontab", "-l"],
        capture_output=True,
        text=True,
        stderr=subprocess.DEVNULL  # 忽略空crontab错误
    )
    existing_cron = result.stdout.strip()  # 去除首尾空白
    
    # 3. 生成新配置（核心修复：确保末尾有换行符）
    # 过滤掉本项目原有任务
    new_cron_lines = [line for line in existing_cron.splitlines() if CRON_TASK_MARK not in line]
    # 添加本项目新任务
    new_cron_lines.append("\n# 节奏游戏项目定时任务（自动生成）")
    new_cron_lines.extend(CRON_TASKS)
    # 拼接并确保末尾有换行符（解决EOF无换行报错）
    new_cron = "\n".join(new_cron_lines).strip() + "\n"  # 关键：末尾加换行
    
    # 4. 写入临时文件并导入
    os.makedirs(TEMP_CRON_FILE.parent, exist_ok=True)
    with open(TEMP_CRON_FILE, "w", encoding="utf-8") as f:
        f.write(new_cron)
    
    try:
        # 导入新crontab
        subprocess.run(
            ["crontab", str(TEMP_CRON_FILE)],
            check=True,
            capture_output=True,
            text=True
        )
        print_color("✅ 定时任务配置成功！", "GREEN")
        # 展示本项目配置
        cron_lines = [
            line for line in subprocess.run(["crontab", "-l"], capture_output=True, text=True).stdout.splitlines()
            if CRON_TASK_MARK in line
        ]
        print_color("当前本项目定时任务：", "YELLOW")
        print("\n".join(cron_lines) if cron_lines else "无")
    except subprocess.CalledProcessError as e:
        print_color(f"❌ 配置失败！错误：{e.stderr or e.stdout}", "RED")
        # 尝试恢复备份（兼容空备份）
        if os.path.exists(backup_file) and os.path.getsize(backup_file) > 0:
            subprocess.run(["crontab", str(backup_file)], check=False)
            print_color("✅ 已恢复原有crontab备份", "GREEN")
        sys.exit(1)
    finally:
        # 清理临时文件
        if TEMP_CRON_FILE.exists():
            os.remove(TEMP_CRON_FILE)

def check_cron():
    """检查cron配置"""
    print_color("===== 当前本项目Cron配置 =====", "YELLOW")
    result = subprocess.run(
        ["crontab", "-l"],
        capture_output=True,
        text=True,
        stderr=subprocess.DEVNULL
    )
    cron_lines = [line for line in result.stdout.splitlines() if CRON_TASK_MARK in line]
    print("\n".join(cron_lines) if cron_lines else "❌ 未检测到本项目Cron任务")
    print_color("\n===== settings.py中Cron配置 =====", "YELLOW")
    print("\n".join(CRON_TASKS) if CRON_TASKS else "❌ 配置为空")

def cancel_cron():
    """取消本项目Cron任务"""
    print_color("===== 开始取消本项目Cron任务 =====", "YELLOW")
    # 备份 + 过滤本项目任务
    backup_file = CRON_BACKUP_DIR / f"cron_backup_cancel_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    subprocess.run(
        ["crontab", "-l"],
        stdout=open(backup_file, "w", encoding="utf-8"),
        stderr=subprocess.DEVNULL,
        check=False
    )
    
    existing_cron = subprocess.run(
        ["crontab", "-l"],
        capture_output=True,
        text=True,
        stderr=subprocess.DEVNULL
    ).stdout
    
    # 过滤本项目任务 + 确保末尾有换行
    new_cron = "\n".join([line for line in existing_cron.splitlines() if CRON_TASK_MARK not in line]).strip()
    if new_cron:
        new_cron += "\n"  # 空crontab无需加
    
    # 导入过滤后的配置
    if new_cron:
        with open(TEMP_CRON_FILE, "w", encoding="utf-8") as f:
            f.write(new_cron)
        subprocess.run(["crontab", str(TEMP_CRON_FILE)], check=True)
    else:
        subprocess.run(["crontab", "-r"], check=True)
    
    print_color("✅ 本项目Cron任务已取消！", "GREEN")
    print_color("剩余Cron任务：", "YELLOW")
    print(subprocess.run(["crontab", "-l"], capture_output=True, text=True, stderr=subprocess.DEVNULL).stdout or "无")
    if TEMP_CRON_FILE.exists():
        os.remove(TEMP_CRON_FILE)

def clear_all_cron():
    """高危：删除所有Cron任务（带二次确认）"""
    print_color("===== 高危操作警告 =====", "BOLD_RED")
    print_color("此命令将删除当前用户的所有Cron任务！", "BOLD_RED")
    
    # 备份全量Cron
    backup_file = CRON_BACKUP_DIR / f"cron_full_backup_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    subprocess.run(
        ["crontab", "-l"],
        stdout=open(backup_file, "w", encoding="utf-8"),
        stderr=subprocess.DEVNULL,
        check=False
    )
    print_color(f"✅ 已备份所有Cron任务到{backup_file}", "GREEN")
    
    # 二次确认
    if input(COLORS["BOLD_RED"] + "输入 YES 确认删除所有Cron：" + COLORS["NC"]) != "YES":
        print_color("ℹ️  用户取消操作", "YELLOW")
        return
    
    # 执行删除
    try:
        subprocess.run(["crontab", "-r"], check=True)
        print_color("✅ 所有Cron任务已删除！", "GREEN")
    except subprocess.CalledProcessError as e:
        print_color(f"❌ 删除失败：{e.stderr}", "RED")
        subprocess.run(["crontab", str(backup_file)], check=False)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] in ["set-cron", "check-cron", "cancel-cron", "clear-all-cron"]:
        locals()[sys.argv[1]]()
    else:
        print_color("请指定有效命令：set-cron/check-cron/cancel-cron/clear-all-cron", "RED")