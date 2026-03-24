#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库管理核心模块：
- 调用csv_incremental_update.py完成CSV→DB同步（all/单表）
- 调用csv_incremental_update.py完成删除所有表（clear，复用原有drop_all_tables）
适配manage.py的update-db指令，完全复用原脚本逻辑，无额外数据库连接
"""
import subprocess
import sys
from pathlib import Path
from config.settings import COLORS

# ========== 路径动态初始化（适配项目目录结构，无硬编码） ==========
# 推导路径：db_manager.py → manager目录 → 项目根目录 → scripts/csv_incremental_update.py
MANAGER_DIR = Path(__file__).absolute().parent  # manager目录（当前文件所在目录）
PROJECT_ROOT = MANAGER_DIR.parent              # 项目根目录（manage.py所在目录）
CSV_UPDATE_SCRIPT = PROJECT_ROOT / "scripts" / "csv_incremental_update.py"  # 同步脚本路径

# 校验脚本是否存在（提前拦截错误）
if not CSV_UPDATE_SCRIPT.exists():
    print(f"{COLORS['RED']}❌ 核心脚本不存在：{CSV_UPDATE_SCRIPT}{COLORS['NC']}")
    sys.exit(1)

# ========== 通用工具函数（和manage.py日志风格统一） ==========
def print_color(msg: str, color: str = "NC") -> None:
    """带颜色打印，复用settings中的COLORS配置"""
    print(f"{COLORS.get(color, COLORS['NC'])}{msg}{COLORS['NC']}")

# ========== 核心业务函数（精准承接manage.py的update-db指令） ==========
def update_db_all():
    """同步所有CSV到数据库 → 调用 csv_incremental_update.py all"""
    try:
        # 调用脚本：python3 csv_incremental_update.py all
        result = subprocess.run(
            [sys.executable, str(CSV_UPDATE_SCRIPT), "all"],
            check=True,          # 非0退出码时抛出异常
            capture_output=True, # 捕获stdout/stderr
            encoding="utf-8"     # 解码输出为字符串
        )
        # 打印脚本输出（便于调试）
        if result.stdout:
            print_color(f"📝 同步脚本输出：\n{result.stdout}", "BLUE")
    except subprocess.CalledProcessError as e:
        # 捕获脚本执行失败，向上抛异常让manage.py处理
        error_msg = f"同步所有表失败：{e.stderr.strip()}"
        print_color(f"❌ {error_msg}", "RED")
        raise Exception(error_msg)

def update_db_single(table_name: str):
    """同步指定单表 → 调用 csv_incremental_update.py [表名]"""
    # 参数校验
    if not table_name or not isinstance(table_name, str):
        raise ValueError("单表同步必须指定有效表名（如：game_song_rel）")
    
    try:
        # 调用脚本：python3 csv_incremental_update.py [表名]
        result = subprocess.run(
            [sys.executable, str(CSV_UPDATE_SCRIPT), table_name],
            check=True,
            capture_output=True,
            encoding="utf-8"
        )
        if result.stdout:
            print_color(f"📝 同步脚本输出：\n{result.stdout}", "BLUE")
    except subprocess.CalledProcessError as e:
        error_msg = f"同步表[{table_name}]失败：{e.stderr.strip()}"
        print_color(f"❌ {error_msg}", "RED")
        raise Exception(error_msg)

def clear_db_all():
    """删除所有表（clear）→ 调用 csv_incremental_update.py clear（复用drop_all_tables）"""
    try:
        # 调用脚本：python3 csv_incremental_update.py clear
        result = subprocess.run(
            [sys.executable, str(CSV_UPDATE_SCRIPT), "clear"],
            check=True,
            capture_output=True,
            encoding="utf-8"
        )
        if result.stdout:
            print_color(f"📝 清理脚本输出：\n{result.stdout}", "BLUE")
    except subprocess.CalledProcessError as e:
        error_msg = f"删除所有表失败：{e.stderr.strip()}"
        print_color(f"❌ {error_msg}", "RED")
        raise Exception(error_msg)

# ========== 测试入口（可选，便于单独调试） ==========
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="数据库管理工具（测试入口）")
    parser.add_argument(
        "action", 
        choices=["all", "single", "clear"], 
        help="操作类型：all(同步所有表)/single(同步单表)/clear(删除所有表)"
    )
    parser.add_argument("table", nargs="?", help="单表名（仅single操作时需要）")
    
    args = parser.parse_args()
    try:
        if args.action == "all":
            update_db_all()
        elif args.action == "single":
            if not args.table:
                print_color("❌ single模式必须指定表名！", "RED")
                sys.exit(1)
            update_db_single(args.table)
        elif args.action == "clear":
            clear_db_all()
        print_color("\n✅ 操作执行完成！", "GREEN")
    except Exception as e:
        print_color(f"\n❌ 操作执行失败：{str(e)}", "RED")
        sys.exit(1)