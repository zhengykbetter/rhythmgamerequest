#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV→数据库脚本（开源）
读取主仓库data_csv目录的CSV，写入数据库（完整流程开源）
"""
import pandas as pd
from sqlalchemy import create_engine
import logging
from pathlib import Path

# 配置（开源可见，敏感信息通过.env读取，不硬编码）
CSV_DIR = Path(__file__).parent.parent / "data_csv"
LOG_DIR = Path(__file__).parent.parent / "logs"
DB_CONFIG = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": "127.0.0.1",
    "db": "rhythmgame",
    "charset": "utf8mb4"
}

# 日志配置（开源流程）
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - CSV→DB：%(message)s",
    handlers=[logging.FileHandler(LOG_DIR / "csv_to_db.log", encoding="utf-8"), logging.StreamHandler()]
)
logger = logging.getLogger("csv_to_db")

def load_csv_to_db():
    """读取CSV并写入数据库（开源核心逻辑）"""
    # 读取主仓库的CSV（从忽略目录读取）
    game_info_df = pd.read_csv(CSV_DIR / "game_info.csv", encoding="utf-8-sig")
    song_info_df = pd.read_csv(CSV_DIR / "song_info.csv", encoding="utf-8-sig")
    
    # 数据库连接
    engine = create_engine(f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['db']}?charset={DB_CONFIG['charset']}")
    
    # 写入数据库（示例：覆盖/增量）
    game_info_df.to_sql("game_info", con=engine, if_exists="replace", index=False)
    song_info_df.to_sql("song_info", con=engine, if_exists="replace", index=False)
    
    logger.info(f"CSV→DB完成：game_info({len(game_info_df)}行)，song_info({len(song_info_df)}行)")

if __name__ == "__main__":
    load_csv_to_db()