#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库管理核心模块：
- 调用csv_incremental_update.py完成CSV→DB同步（all/单表）
- 实现数据库清空逻辑（clear）
适配manage.py的update-db指令，保持代码简洁、路径动态适配
"""
import subprocess
import sys
from pathlib import Path
from config.settings import COLORS

# ========== 路径动态初始化（适配项目结构） ==========
# db_manager.py 所在目录 → manager目录 → 项目根目录 → scripts目录
MANAGER_DIR = Path(__file__).parent  # manager目录
PROJECT_ROOT = MANAGER_DIR.parent    # 项目根目录（manage.py所在目录）
CSV_UPDATE_SCRIPT = PROJECT_ROOT / "scripts" / "csv_incremental_update.py"  # 同步脚本路径

# 确保脚本存在
if not CSV_UPDATE_SCRIPT.exists():
    print(f"{COLORS['RED']}❌ 同步脚本不存在：{CSV_UPDATE_SCRIPT}{COLORS['NC']}")
    sys.exit(1)

# ========== 通用工具函数（复用颜色打印） ==========
def print_color(msg: str, color: str = "NC") -> None:
    """带颜色打印（和manage.py风格统一）"""
    print(f"{COLORS.get(color, COLORS['NC'])}{msg}{COLORS['NC']}")

# ========== 核心业务函数（承接manage的update-db指令） ==========
def update_db_all():
    """同步所有CSV到数据库（调用csv_incremental_update.py all）"""
    try:
        # 调用同步脚本：python3 csv_incremental_update.py all
        result = subprocess.run(
            [sys.executable, str(CSV_UPDATE_SCRIPT), "all"],
            check=True,
            capture_output=True,
            encoding="utf-8"
        )
        if result.stdout:
            print_color(f"📝 脚本输出：{result.stdout}", "BLUE")
    except subprocess.CalledProcessError as e:
        print_color(f"❌ 同步所有表失败：{e.stderr}", "RED")
        raise Exception(f"CSV→DB同步失败：{e.stderr}")

def update_db_single(table_name: str):
    """同步指定单表的CSV到数据库（调用csv_incremental_update.py 表名）"""
    if not table_name:
        raise ValueError("单表同步需指定表名（如 game_song_rel）")
    
    try:
        # 调用同步脚本：python3 csv_incremental_update.py 表名
        result = subprocess.run(
            [sys.executable, str(CSV_UPDATE_SCRIPT), table_name],
            check=True,
            capture_output=True,
            encoding="utf-8"
        )
        if result.stdout:
            print_color(f"📝 脚本输出：{result.stdout}", "BLUE")
    except subprocess.CalledProcessError as e:
        print_color(f"❌ 同步表{table_name}失败：{e.stderr}", "RED")
        raise Exception(f"同步表{table_name}失败：{e.stderr}")

def clear_db_all():
    """清空数据库所有表数据（高危操作，适配原有表结构）"""
    try:
        # 1. 导入数据库连接（复用csv_incremental_update的get_mysql_engine）
        sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
        from csv_incremental_update import get_mysql_engine
        
        # 2. 定义需清空的表列表（和TABLE_RULES一致）
        tables = [
            "game_info", "author_info", "song_info",
            "game_song_rel", "song_author_rel", "game_linkage_rel"
        ]
        
        # 3. 连接数据库并清空表
        engine = get_mysql_engine()
        with engine.connect() as conn:
            # 禁用外键约束（避免清空顺序报错）
            conn.execute("SET FOREIGN_KEY_CHECKS = 0;")
            for table in tables:
                conn.execute(f"TRUNCATE TABLE {table};")
                print_color(f"✅ 已清空表：{table}", "GREEN")
            conn.execute("SET FOREIGN_KEY_CHECKS = 1;")
            conn.commit()
        
        print_color("✅ 数据库所有表已清空", "GREEN")
    except Exception as e:
        print_color(f"❌ 清空数据库失败：{str(e)}", "RED")
        raise Exception(f"清空数据库失败：{str(e)}")

# ========== 测试入口（可选） ==========
if __name__ == "__main__":
    # 测试调用示例
    import argparse
    parser = argparse.ArgumentParser(description="DB管理工具")
    parser.add_argument("action", choices=["all", "single", "clear"], help="操作类型")
    parser.add_argument("table", nargs="?", help="单表名（仅single时需要）")
    
    args = parser.parse_args()
    if args.action == "all":
        update_db_all()
    elif args.action == "single":
        if not args.table:
            print_color("❌ single模式需指定表名", "RED")
            sys.exit(1)
        update_db_single(args.table)
    elif args.action == "clear":
        clear_db_all()