import os
import json
from pathlib import Path

class Config:
    # 基础路径
    BASE_DIR = Path(__file__).parent.parent
    BLOGSHOW_DIR = BASE_DIR / "blogshow"
    DATA_ISSUES_DIR = BASE_DIR / "data_issues"

    # 文件路径
    LOG_PATH = BLOGSHOW_DIR / "update.log"
    COUNT_PATH = BLOGSHOW_DIR / "visit_count.txt"
    ISSUES_PATH = DATA_ISSUES_DIR / "issues.json"

    # 确保目录和文件存在
    os.makedirs(DATA_ISSUES_DIR, exist_ok=True)
    os.makedirs(BLOGSHOW_DIR, exist_ok=True)

    if not ISSUES_PATH.exists():
        with open(ISSUES_PATH, "w", encoding="utf-8") as f:
            json.dump([], f)

    if not COUNT_PATH.exists():
        with open(COUNT_PATH, "w") as f:
            f.write("0")

    # 预加载更新日志
    with open(LOG_PATH, "r", encoding="utf-8") as f:
        UPDATE_BLOG = f.read()