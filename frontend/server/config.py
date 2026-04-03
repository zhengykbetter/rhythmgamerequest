import os
import json
from pathlib import Path

class Config:
    # ========== 1. 基础路径锚点 ==========
    BASE_DIR = Path(__file__).parent.parent  # frontend/
    FRONTEND_ROOT = BASE_DIR
    # 注意：MAIN_PROJECT_ROOT (frontend/..) 因跨出前端范围，暂不纳入此处统一管理
    
    # ========== 2. 目录结构定义 ==========
    # 原有目录
    BLOGSHOW_DIR = BASE_DIR / "blogshow"
    DATA_ISSUES_DIR = BASE_DIR / "data_issues"
    
    # 新增数据目录 (对应 frontend/data)
    DATA_DIR = BASE_DIR / "data"
    
    # 缓存目录
    HISTORY_TODAY_CACHE_DIR = DATA_DIR / "history_today"

    # ========== 3. 文件路径定义 ==========
    # 原有文件
    LOG_PATH = BLOGSHOW_DIR / "update.log"
    COUNT_PATH = BLOGSHOW_DIR / "visit_count.txt"
    ISSUES_PATH = DATA_ISSUES_DIR / "issues.json"
    
    # Benchmark 相关文件
    @staticmethod
    def get_benchmark_csv_path(version="v1"):
        return Config.DATA_DIR / f"benchmark_{version}.csv"
    
    CONTRIBUTORS_PATH = DATA_DIR / "contributors.json"
    BENCHMARK_ISSUES_PATH = DATA_DIR / "benchmark_issues.json"

    # ========== 4. 集中初始化 (目录+文件) ==========
    @classmethod
    def initialize(cls):
        # 1. 创建所有目录
        dirs_to_create = [
            cls.BLOGSHOW_DIR,
            cls.DATA_ISSUES_DIR,
            cls.DATA_DIR,
            cls.HISTORY_TODAY_CACHE_DIR
        ]
        for d in dirs_to_create:
            d.mkdir(parents=True, exist_ok=True)

        # 2. 初始化空文件 (如果不存在)
        files_to_init = [
            (cls.ISSUES_PATH, "[]"),
            (cls.COUNT_PATH, "0"),
            (cls.LOG_PATH, ""),  # 日志文件可以为空
            (cls.CONTRIBUTORS_PATH, "[]"),
            (cls.BENCHMARK_ISSUES_PATH, "[]")
        ]
        
        for file_path, default_content in files_to_init:
            if not file_path.exists():
                # 根据后缀判断写入方式
                if file_path.suffix == '.json':
                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(default_content)
                else:
                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(default_content)

    # ========== 5. 预加载内容 (保持原有功能) ==========
    # 注意：为了避免循环导入和启动副作用，建议将此改为 @classmethod 按需调用
    # 或者在应用启动显式调用
    @classmethod
    def load_blog(cls):
        if cls.LOG_PATH.exists():
            with open(cls.LOG_PATH, "r", encoding="utf-8") as f:
                return f.read()
        return ""

# 执行初始化 (在模块导入时执行一次)
Config.initialize()
# 预加载博客 (可选，视具体启动逻辑而定)
UPDATE_BLOG = Config.load_blog()