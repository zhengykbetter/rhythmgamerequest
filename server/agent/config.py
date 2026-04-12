import os
from dotenv import load_dotenv

# 项目根路径
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(ROOT, ".env"))

# LLM 配置（完全沿用你的参数）
LLM_CONFIG = {
    "api_key": os.getenv("LLM_API_KEY", ""),
    "base_url": os.getenv("LLM_BASE_URL", ""),
    "model": os.getenv("LLM_MODEL", "deepseek-chat"),
    "temperature": 0.1,
}

# 表结构（从你的原代码迁移）
from config.table_schemas import TABLE_RULES