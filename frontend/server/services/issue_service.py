import json
from datetime import datetime
from server.config import Config

def load_issues():
    with open(Config.ISSUES_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def save_issues(issues):
    with open(Config.ISSUES_PATH, "w", encoding="utf-8") as f:
        json.dump(issues, f, ensure_ascii=False, indent=2)

def add_issue(name, contact, content):
    issues = load_issues()
    new_issue = {
        "id": len(issues) + 1,
        "name": name,
        "contact": contact,
        "content": content,
        "status": "待处理",
        "create_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    issues.append(new_issue)
    save_issues(issues)
    return new_issue