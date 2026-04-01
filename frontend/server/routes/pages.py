from flask import Blueprint, render_template, request, redirect, url_for, flash
from server.services.issue_service import load_issues, add_issue # 导入 service

pages_bp = Blueprint('pages', __name__)

def render_under_construction(title):
    return render_template('under_construction.html', page_title=title)

@pages_bp.route('/benchmark')
def benchmark():
    return render_under_construction("性能基准测试 (Benchmark)")

@pages_bp.route('/constraints')
def constraints():
    return render_under_construction("数据库约束说明")

@pages_bp.route('/roadmap')
def roadmap():
    return render_under_construction("长期规划 (Roadmap)")

# ===================== 新增：独立的 Issues 页面 =====================
@pages_bp.route('/issues', methods=['GET', 'POST'])
def issues_page():
    if request.method == 'POST':
        # 处理表单提交
        name = request.form.get('name', '').strip()
        contact = request.form.get('contact', '').strip()
        content = request.form.get('content', '').strip()
        
        if name and content:
            add_issue(name, contact, content)
            # 提交成功，重定向回首页或者显示成功页
            return render_template('issues.html', success=True)
        else:
            return render_template('issues.html', error="请填写称呼和内容")
    
    # GET 请求，显示表单
    return render_template('issues.html')