#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
✅ Benchmark 第一阶段服务
功能：读取CSV题库、读写JSON贡献者/Issue、计算统计数据
"""
import os
import csv
import json
from datetime import datetime

# 项目根目录（精准匹配 frontend 根目录）
FRONTEND_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(FRONTEND_ROOT, "data")

# ===================== CSV 题库读取 =====================
def load_questions(version="v1"):
    """
    读取指定版本的CSV题库
    返回：[{id: int, question: str}, ...]
    """
    csv_path = os.path.join(DATA_DIR, f"benchmark_{version}.csv")
    if not os.path.exists(csv_path):
        return []
    
    questions = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t") # 严格按你给的制表符分隔
        for row in reader:
            try:
                questions.append({
                    "id": int(row["序号"]),
                    "question": row["问题"]
                })
            except Exception as e:
                continue # 跳过格式错误的行
    return sorted(questions, key=lambda x: x["id"])

# ===================== JSON 贡献者读写 =====================
def load_contributors():
    json_path = os.path.join(DATA_DIR, "contributors.json")
    if not os.path.exists(json_path):
        return []
    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_contributors(contributors):
    json_path = os.path.join(DATA_DIR, "contributors.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(contributors, f, ensure_ascii=False, indent=2)

def add_contributor(name, total_score, question_scores, version="v1"):
    contributors = load_contributors()
    new_contributor = {
        "id": len(contributors) + 1,
        "name": name.strip() if name else "匿名用户",
        "total_score": total_score,
        "question_scores": question_scores,
        "benchmark_version": version,
        "submit_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    contributors.append(new_contributor)
    save_contributors(contributors)
    return new_contributor

# ===================== 统计数据计算 =====================
def get_statistics():
    """
    计算已测试人次、当前均分
    返回：{total_tests: int, avg_score: float or None}
    """
    contributors = load_contributors()
    total_tests = len(contributors)
    if total_tests == 0:
        return {"total_tests": 0, "avg_score": None}
    
    total_score_sum = sum([c["total_score"] for c in contributors])
    avg_score = round(total_score_sum / total_tests, 1)
    return {"total_tests": total_tests, "avg_score": avg_score}