#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import csv
import json
import os  # 【修复】重新添加 os 导入
from datetime import datetime
from pathlib import Path

# 导入 Config
import sys
CURRENT_FILE = Path(__file__).resolve()
if str(CURRENT_FILE.parents[1]) not in sys.path:
    sys.path.insert(0, str(CURRENT_FILE.parents[1]))

from servee.config import Config


# ===================== CSV 题库读取（使用 Config） =====================
def load_questions(version="v1"):
    csv_path = Config.get_benchmark_csv_path(version)
    
    if not csv_path.exists():
        print(f"【错误】CSV文件不存在：{csv_path}")
        return []
    
    if not os.access(csv_path, os.R_OK):
        print(f"【错误】CSV文件无读权限：{csv_path}")
        return []

    questions = []
    try:
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f, delimiter=",")
            if not {"序号", "问题"}.issubset(reader.fieldnames):
                print(f"【错误】CSV表头不匹配！当前表头：{reader.fieldnames}")
                return []
            
            for row_num, row in enumerate(reader, start=2):
                try:
                    q_id = int(row["序号"].strip())
                    q_text = row["问题"].strip()
                    if q_text:
                        questions.append({"id": q_id, "question": q_text})
                except ValueError as e:
                    print(f"【警告】第{row_num}行跳过：{e}")
                    continue
    except Exception as e:
        print(f"【错误】CSV读取失败：{str(e)}")
        return []

    return sorted(questions, key=lambda x: x["id"])

# ===================== JSON 贡献者读写（使用 Config） =====================
def load_contributors():
    if not Config.CONTRIBUTORS_PATH.exists():
        return []
    try:
        with open(Config.CONTRIBUTORS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"【错误】贡献者读取失败：{str(e)}")
        return []

def save_contributors(contributors):
    try:
        with open(Config.CONTRIBUTORS_PATH, "w", encoding="utf-8") as f:
            json.dump(contributors, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"【错误】贡献者写入失败：{str(e)}")

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
    contributors = load_contributors()
    total_tests = len(contributors)
    if total_tests == 0:
        return {"total_tests": 0, "avg_score": None}
    total_score_sum = sum([c["total_score"] for c in contributors])
    avg_score = round(total_score_sum / total_tests, 1)
    return {"total_tests": total_tests, "avg_score": avg_score}

# ===================== Benchmark Issue 读写（使用 Config） =====================
def load_benchmark_issues():
    if not Config.BENCHMARK_ISSUES_PATH.exists():
        return []
    try:
        with open(Config.BENCHMARK_ISSUES_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"【错误】Benchmark Issue读取失败：{str(e)}")
        return []

def save_benchmark_issues(issues):
    try:
        with open(Config.BENCHMARK_ISSUES_PATH, "w", encoding="utf-8") as f:
            json.dump(issues, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"【错误】Benchmark Issue写入失败：{str(e)}")

def add_benchmark_issue(name, contact, content, question_id=None, version="v1"):
    issues = load_benchmark_issues()
    new_issue = {
        "id": len(issues) + 1,
        "name": name.strip(),
        "contact": contact.strip(),
        "content": content.strip(),
        "question_id": question_id,
        "benchmark_version": version,
        "submit_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    issues.append(new_issue)
    save_benchmark_issues(issues)
    return new_issue