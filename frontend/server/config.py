import os
import json
from pathlib import Path

class Config:
    # ========== 1. 基础路径锚点 ==========
    BASE_DIR = Path(__file__).parent.parent  # frontend/
    FRONTEND_ROOT = BASE_DIR
    
    # ========== 2. 【核心改动】外部数据仓库配置 ==========
    # 默认外部仓库路径 (项目根目录同级的 external_data 文件夹，可通过环境变量 EXTERNAL_DATA_ROOT 覆盖)
    # 这里使用 .parents[1] 跳出 frontend，再跳出 main_project，到达项目根目录的上级
    DEFAULT_EXTERNAL_ROOT = BASE_DIR.parents[1] / "external_data_repo"
    EXTERNAL_DATA_ROOT = Path(os.getenv("EXTERNAL_DATA_ROOT", str(DEFAULT_EXTERNAL_ROOT)))

    # ========== 3. 目录结构定义 (全部迁移到外部仓库) ==========
    BLOGSHOW_DIR = EXTERNAL_DATA_ROOT / "blogshow"
    DATA_ISSUES_DIR = EXTERNAL_DATA_ROOT / "data_issues"
    DATA_DIR = EXTERNAL_DATA_ROOT / "data"
    HISTORY_TODAY_CACHE_DIR = DATA_DIR / "history_today"
    DATA_QUERIES_DIR = EXTERNAL_DATA_ROOT / "data_queries"

    # ========== 4. 文件路径定义 (无需改动，自动跟随目录变更) ==========
    LOG_PATH = BLOGSHOW_DIR / "update.log"
    COUNT_PATH = BLOGSHOW_DIR / "visit_count.txt"
    ISSUES_PATH = DATA_ISSUES_DIR / "issues.json"
    USER_QUERIES_LOG_PATH = DATA_QUERIES_DIR / "user_queries.txt"
    
    @staticmethod
    def get_benchmark_csv_path(version="v1"):
        return Config.DATA_DIR / f"benchmark_{version}.csv"
    
    CONTRIBUTORS_PATH = DATA_DIR / "contributors.json"
    BENCHMARK_ISSUES_PATH = DATA_DIR / "benchmark_issues.json"

    # ========== 5. 集中初始化 (无需改动，自动在外部路径创建目录) ==========
    @classmethod
    def initialize(cls):
        dirs_to_create = [
            cls.BLOGSHOW_DIR,
            cls.DATA_ISSUES_DIR,
            cls.DATA_DIR,
            cls.HISTORY_TODAY_CACHE_DIR,
            cls.DATA_QUERIES_DIR
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

    # ========== 6. 博客内容加载 ==========
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
Config.UPDATE_BLOG = Config._load_blog()