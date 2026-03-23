#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV文件管理模块（极简版：复用sync_csv_from_remote.py核心逻辑）
新增：split_csv 函数 → 调用extract_song_data.py生成标准化CSV表
"""
import os
import sys
import time
import subprocess
from pathlib import Path

# ========== 基础配置：确保能导入sync_csv_from_remote.py ==========
MAIN_REPO_ROOT = Path(__file__).parent.parent  # /opt/main_project
sys.path.insert(0, str(MAIN_REPO_ROOT / "scripts"))  # 加入scripts目录到Python路径
sys.path.insert(0, str(MAIN_REPO_ROOT))

# 1. 导入统一颜色配置
from config.settings import COLORS, CSV_TARGET_DIR
# 2. 直接导入sync_csv_from_remote.py的核心函数（关键：复用所有逻辑）
import sync_csv_from_remote as csv_sync

# ========== 仅保留轻量工具函数（颜色打印） ==========
def print_color(msg: str, color: str = "NC") -> None:
    """带颜色打印（引用settings统一配置）"""
    print(f"{COLORS.get(color, COLORS['NC'])}{msg}{COLORS['NC']}")

# ========== 核心功能：仅封装入口，全部复用sync_csv_from_remote的逻辑 ==========
def sync_now():
    """CSV同步主入口（极简：直接调用sync_csv_from_remote的main流程）"""
    print_color("===== 开始CSV同步流程 =====", "YELLOW")
    start_time = time.time()
    
    try:
        # 直接调用sync_csv_from_remote的完整同步流程（无需重复写代码）
        csv_sync.main()
        total_time = round((time.time() - start_time) * 1000, 2)
        print_color(f"✅ CSV同步流程执行完成（总耗时{total_time}ms）", "GREEN")
    except SystemExit as e:
        # 捕获sync_csv_from_remote中sys.exit(1)的异常，友好提示
        total_time = round((time.time() - start_time) * 1000, 2)
        print_color(f"❌ CSV同步失败（总耗时{total_time}ms），详情见日志", "RED")
        sys.exit(1)
    except Exception as e:
        total_time = round((time.time() - start_time) * 1000, 2)
        print_color(f"❌ CSV同步异常（总耗时{total_time}ms）：{str(e)}", "RED")
        sys.exit(1)

def clean_remote_csv():
    """清理远程CSV文件（sync_csv_from_remote中无此功能，保留独立实现）"""
    print_color("===== 开始清理远程CSV文件 =====", "YELLOW")
    from config.settings import CSV_SOURCE_DIR
    
    if not os.path.exists(CSV_SOURCE_DIR):
        print_color(f"❌ CSV源目录不存在：{CSV_SOURCE_DIR}", "RED")
        return
    
    # 查找并确认删除CSV文件
    csv_files = [f for f in os.listdir(CSV_SOURCE_DIR) if f.endswith(".csv")]
    if not csv_files:
        print_color("ℹ️  无CSV文件需要清理", "YELLOW")
        return
    
    print_color(f"即将删除以下文件：{csv_files}", "YELLOW")
    if input(COLORS["YELLOW"] + "输入 YES 确认删除：" + COLORS["NC"]) != "YES":
        print_color("ℹ️  用户取消操作", "YELLOW")
        return
    
    # 执行删除
    try:
        for f in csv_files:
            os.remove(os.path.join(CSV_SOURCE_DIR, f))
        print_color(f"✅ 已删除{CSV_SOURCE_DIR}下的所有CSV文件", "GREEN")
    except Exception as e:
        print_color(f"❌ 删除失败：{str(e)}", "RED")
        sys.exit(1)

def split_csv():
    """新增：调用extract_song_data.py，拆分原始CSV为多张标准化表"""
    print_color("===== 开始执行split_csv：生成标准化CSV表 =====", "YELLOW")
    start_time = time.time()
    
    # 1. 校验extract_song_data.py是否存在
    extract_script = MAIN_REPO_ROOT / "scripts" / "extract_song_data.py"
    if not extract_script.exists():
        print_color(f"❌ 错误：提取脚本不存在 → {extract_script}", "RED")
        print_color("⚠️  请确认extract_song_data.py放在scripts目录下", "YELLOW")
        sys.exit(1)
    
    # 2. 确保输出目录存在
    os.makedirs(CSV_TARGET_DIR, exist_ok=True)
    
    # 3. 调用外部脚本（捕获输出和错误）
    try:
        print_color(f"📝 正在执行：python {extract_script}", "GREEN")
        # 执行脚本，实时输出日志
        result = subprocess.run(
            [sys.executable, str(extract_script)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
            cwd=str(MAIN_REPO_ROOT)  # 工作目录=主项目根
        )
        # 输出脚本执行结果
        print(result.stdout)
        
        total_time = round((time.time() - start_time) * 1000, 2)
        if result.returncode == 0:
            print_color(f"✅ split_csv执行完成（总耗时{total_time}ms）", "GREEN")
        else:
            print_color(f"❌ split_csv执行失败（返回码：{result.returncode}，总耗时{total_time}ms）", "RED")
            sys.exit(1)
    except Exception as e:
        total_time = round((time.time() - start_time) * 1000, 2)
        print_color(f"❌ split_csv执行异常（总耗时{total_time}ms）：{str(e)}", "RED")
        sys.exit(1)

# ========== 测试入口（可选） ==========
if __name__ == "__main__":
    if len(sys.argv) > 1:
        command_map = {
            "sync-now": sync_now,
            "clean-remote-csv": clean_remote_csv,
            "split_csv": split_csv  # 新增测试入口
        }
        cmd = sys.argv[1]
        if cmd in command_map:
            command_map[cmd]()
        else:
            print_color("未知命令：仅支持 sync-now / clean-remote-csv / split_csv", "RED")