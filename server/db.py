#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库连接工具（复刻原版，100%兼容现有配置）
"""
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# 加载项目根路径
MAIN_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(MAIN_PROJECT_ROOT, ".env"))

# 导入项目数据库配置
from config.settings import DB_CONFIG

MYSQL_CONFIG = {
    "host": DB_CONFIG["host"],
    "port": DB_CONFIG["port"],
    "user": DB_CONFIG["user"],
    "password": DB_CONFIG["password"],
    "database": DB_CONFIG["db"],
    "charset": DB_CONFIG["charset"]
}

def get_mysql_engine():
    """
    复刻原版数据库引擎
    完全和 csv_to_db.py 保持一致
    """
    conn_str = (
        f"mysql+pymysql://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@"
        f"{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}?charset={MYSQL_CONFIG['charset']}"
    )
    return create_engine(
        conn_str,
        pool_pre_ping=True,
        pool_recycle=3600
    )