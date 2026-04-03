import os
import json
from pathlib import Path

class Config:
    # ========== 1. 基础路径锚点 ==========
    BASE_DIR = Path(__file__).parent.parent  # frontend/
    FRONTEND_ROOT = BASE_DIR
    
    # ========== 2. 目录结构定义 ==========
    BLOGSHOW_DIR = BASE_DIR / "blogshow"
    DATA_ISSUES_DIR = BASE_DIR / "data_issues"
    DATA_DIR = BASE_DIR / "data"
    HISTORY_TODAY_CACHE_DIR = DATA_DIR / "history_today"

    # ========== 3. 文件路径定义 ==========
    LOG_PATH = BLOGSHOW_DIR / "update.log"
    COUNT_PATH = BLOGSHOW_DIR / "visit_count.txt"
    ISSUES_PATH = DATA_ISSUES_DIR / "issues.json"
    
    @staticmethod
    def get_benchmark_csv_path(version="v1"):
        return Config.DATA_DIR / f"benchmark_{version}.csv"
    
    CONTRIBUTORS_PATH = DATA_DIR / "contributors.json"
    BENCHMARK_ISSUES_PATH = DATA_DIR / "benchmark_issues.json"

    # ========== 4. 集中初始化 (目录+文件) ==========
    @classmethod
    def initialize(cls):
        dirs_to_create = [
            cls.BLOGSHOW_DIR,
            cls.DATA_ISSUES_DIR,
            cls.DATA_DIR,
            cls.HISTORY_TODAY_CACHE_DIR
        ]
        for d in dirs_to_create:
            d.mkdir(parents=True, exist_ok=True)

        files_to_init = [
            (cls.ISSUES_PATH, "[]"),
            (cls.COUNT_PATH, "0"),
            (cls.LOG_PATH, ""),
            (cls.CONTRIBUTORS_PATH, "[]"),
            (cls.BENCHMARK_ISSUES_PATH, "[]")
        ]
        
        for file_path, default_content in files_to_init:
            if not file_path.exists():
                if file_path.suffix == '.json':
                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(default_content)
                else:
                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(default_content)

    # ========== 5. 博客内容加载 ==========
    @classmethod
    def _load_blog(cls):
        if cls.LOG_PATH.exists():
            try:
                with open(cls.LOG_PATH, "r", encoding="utf-8") as f:
                    return f.read()
            except Exception:
                pass
        return ""

# 执行初始化
Config.initialize()

# 【修复】将 UPDATE_BLOG 挂载为 Config 类的静态属性，以兼容 routes/main.py 的调用方式
Config.UPDATE_BLOG = Config._load_blog()