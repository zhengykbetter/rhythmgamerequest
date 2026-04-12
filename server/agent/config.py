import os
import sys
from dotenv import load_dotenv

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)

# 加载 .env
load_dotenv(os.path.join(ROOT, ".env"))

# LLM 配置
LLM_CONFIG = {
    "api_key": os.getenv("LLM_API_KEY", ""),
    "base_url": os.getenv("LLM_BASE_URL", ""),
    "model": os.getenv("LLM_MODEL", "deepseek-chat"),
    "temperature": 0.1,
}

from config.table_schemas import TABLE_RULES