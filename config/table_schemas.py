#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库表结构配置文件
集中管理所有表的字段定义、主键、日期字段等配置
便于维护和版本控制
"""

TABLE_RULES = {
    "create_order": ["game_info", "author_info", "song_info", "game_song_rel", "song_author_rel", "game_linkage_rel"],
    "game_info": {
        "primary_key": "游戏编号",
        "auto_cols": ["最新更新时间", "update_timestamp"],
        "date_cols": ["实装时间", "更新时间", "数据时间", "开服时间"],
        "field_types": {
            "游戏编号": "VARCHAR(100)",
            "游戏": "VARCHAR(200)",
            "别名": "VARCHAR(200)",
            "实装时间": "DATE",
            "更新时间": "DATE",
            "数据时间": "DATE",
            "开服时间": "DATE",
            "最新更新时间": "DATETIME",
            "update_timestamp": "DATETIME"
        },
        "foreign_keys": []
    },
    "author_info": {
        "primary_key": "author_id",
        "auto_cols": ["最新更新时间", "update_timestamp"],
        "date_cols": [],
        "field_types": {
            "author_id": "VARCHAR(50)",
            "作者名": "VARCHAR(1000)",
            "别名": "VARCHAR(500)",
            "备注": "VARCHAR(500)",
            "最新更新时间": "DATETIME",
            "update_timestamp": "DATETIME"
        },
        "foreign_keys": []
    },
    "song_info": {
        "primary_key": "song_id",
        "auto_cols": ["最新更新时间", "update_timestamp"],
        "date_cols": [],
        "field_types": {
            "song_id": "VARCHAR(50)",
            "歌名": "VARCHAR(1000)",
            "别名": "VARCHAR(1000)",
            "作者": "VARCHAR(1000)",
            "本家": "VARCHAR(200)",
            "最新更新时间": "DATETIME",
            "update_timestamp": "DATETIME"
        },
        "foreign_keys": []
    },
    # ===================== 【仅修改此处】game_song_rel 表结构 =====================
    "game_song_rel": {
        "primary_key": "rel_id",
        "auto_cols": ["最新更新时间", "update_timestamp"],
        "date_cols": ["收录时间"],
        "field_types": {
            "rel_id": "VARCHAR(200)",
            "游戏编号": "INT",  # 🔧 核心修改：从 VARCHAR(100) 改为 INT（数字游戏ID）
            "游戏名": "VARCHAR(200)",  # 🔧 新增：冗余游戏名字段
            "song_id": "VARCHAR(50)",
            "本家": "VARCHAR(200)",
            "收录时间": "DATE",
            "最新更新时间": "DATETIME",
            "update_timestamp": "DATETIME"
        },
        "foreign_keys": []
    },
    # ============================================================================
    "song_author_rel": {
        "primary_key": "rel_id",
        "auto_cols": ["最新更新时间", "update_timestamp"],
        "date_cols": [],
        "field_types": {
            "rel_id": "VARCHAR(100)",
            "song_id": "VARCHAR(50)",
            "author_id": "VARCHAR(50)",
            "曲名": "VARCHAR(1000)",
            "作者名": "VARCHAR(1000)",
            "最新更新时间": "DATETIME",
            "update_timestamp": "DATETIME"
        },
        "foreign_keys": []
    },
    "game_linkage_rel": {
        "primary_key": "rel_id",
        "auto_cols": ["最新更新时间", "update_timestamp"],
        "date_cols": ["联动时间"],
        "field_types": {
            "rel_id": "VARCHAR(200)",
            "游戏1编号": "VARCHAR(100)",
            "游戏2编号": "VARCHAR(50)",
            "游戏1名称": "VARCHAR(200)",
            "游戏2名称": "VARCHAR(200)",
            "联动名称": "VARCHAR(200)",
            "联动时间": "DATE",
            "联动版本": "VARCHAR(50)",
            "说明": "VARCHAR(500)",
            "最新更新时间": "DATETIME",
            "update_timestamp": "DATETIME"
        },
        "foreign_keys": []
    }
}

TYPE_MAPPING = {
    "int64": "INT",
    "float64": "FLOAT",
    "object": "VARCHAR(255)",
    "datetime64[ns]": "DATE",
    "bool": "TINYINT(1)"
}