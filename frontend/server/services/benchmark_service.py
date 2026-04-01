#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
✅ Benchmark 第一阶段服务（修复版）
功能：读取CSV题库、读写JSON贡献者/Issue、计算统计数据
"""
import os
import csv
import json
from datetime import datetime

# 精准路径计算（100%匹配你的目录结构）
CURRENT_FILE = os.path.abspath(__file__)
# 三级父目录：services → server → frontend
FRONTEND_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_FILE)))
DATA_DIR = os.path.join(FRONTEND_ROOT, "data")

# 打印路径，方便调试
print(f"【调试】Frontend根目录：{FRONTEND_ROOT}")
print(f"【调试】数据目录：{DATA_DIR}")

# ===================== CSV 题库读取（修复版） =====================
def load_questions(version="v1"):
    """
    读取指定版本的CSV题库
    返回：[{id: int, question: str}, ...]
    """
    csv_filename = f"benchmark_{version}.csv"
    csv_path = os.path.join(DATA_DIR, csv_filename)
    
    # 1. 检查文件是否存在
    if not os.path.exists(csv_path):
        print(f"【错误】CSV文件不存在：{csv_path}")
        return []
    
    # 2. 检查文件权限
    if not os.access(csv_path, os.R_OK):
        print(f"【错误】CSV文件无读权限：{csv_path}")
        return []

    questions = []
    # 3. 用utf-8-sig兼容BOM，逗号分隔匹配你的CSV格式
    try:
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f, delimiter=",") # 严格匹配你的逗号分隔
            # 校验表头
            if not {"序号", "问题"}.issubset(reader.fieldnames):
                print(f"【错误】CSV表头不匹配！当前表头：{reader.fieldnames}")
                print(f"【要求】表头必须包含：序号,问题")
                return []
            
            # 逐行读取
            for row_num, row in enumerate(reader, start=2): # 行号从2开始（表头是1）
                try:
                    question_id = int(row["序号"].strip())
                    question_text = row["问题"].strip()
                    if not question_text:
                        print(f"【警告】第{row_num}行问题为空，跳过")
                        continue
                    questions.append({
                        "id": question_id,
                        "question": question_text
                    })
                except ValueError as e:
                    print(f"【警告】第{row_num}行序号格式错误，跳过：{e}")
                    continue
    except Exception as e:
        print(f"【错误】CSV文件读取失败：{str(e)}")
        return []

    print(f"【调试】成功加载{len(questions)}道题目")
    return sorted(questions, key=lambda x: x["id"])

# ===================== JSON 贡献者读写（无改动） =====================
def load_contributors():
    json_path = os.path.join(DATA_DIR, "contributors.json")
    if not os.path.exists(json_path):
        return []
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"【错误】贡献者文件读取失败：{str(e)}")
        return []

def save_contributors(contributors):
    json_path = os.path.join(DATA_DIR, "contributors.json")
    try:
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(contributors, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"【错误】贡献者文件写入失败：{str(e)}")

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

# ===================== 统计数据计算（无改动） =====================
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