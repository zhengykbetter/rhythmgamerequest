#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV→数据库脚本（开源+配置分离）
"""
import sys
import pandas as pd
from sqlalchemy import create_engine
import logging

# 新增：添加主仓库根目录到Python路径
sys.path.append(str(__file__).rsplit('/', 2)[0])
from config.settings import CSV_TARGET_DIR, LOG_DIR, DB_CONFIG

# ===================== 日志配置（引用配置文件） =====================
def setup_logger():
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = LOG_DIR / "csv_to_db.log"
    
    logger = logging.getLogger("csv_to_db")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger
    
    fmt = "%(asctime)s - %(levelname)s - CSV→DB：%(message)s"
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(fmt))
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(fmt))
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger

logger = setup_logger()

# ===================== 核心逻辑（引用配置） =====================
def load_csv_to_db():
    """读取CSV并写入数据库（引用配置文件的路径/数据库配置）"""
    try:
        # 读取主仓库的CSV（引用配置的目标目录）
        game_info_df = pd.read_csv(CSV_TARGET_DIR / "game_info.csv", encoding="utf-8-sig")
        song_info_df = pd.read_csv(CSV_TARGET_DIR / "song_info.csv", encoding="utf-8-sig")
        logger.info(f"读取CSV成功：game_info({len(game_info_df)}行)，song_info({len(song_info_df)}行)")
        
        # 数据库连接（引用配置的DB参数）
        db_url = (
            f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
            f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['db']}"
            f"?charset={DB_CONFIG['charset']}"
        )
        engine = create_engine(db_url)
        
        # 写入数据库
        game_info_df.to_sql("game_info", con=engine, if_exists="replace", index=False)
        song_info_df.to_sql("song_info", con=engine, if_exists="replace", index=False)
        
        logger.info("CSV→DB写入完成")
    except Exception as e:
        logger.error(f"CSV→DB执行失败：{str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    load_csv_to_db()