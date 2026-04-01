from flask import Blueprint, render_template, request
from server.services.issue_service import add_issue
from server.services.benchmark_service import get_statistics

pages_bp = Blueprint('pages', __name__)

def render_under_construction(title):
    return render_template('under_construction.html', page_title=title)

# ===================== 新增：Benchmark 第一阶段路由（优先级最高） =====================
@pages_bp.route('/benchmark')
def benchmark_welcome():
    """
    Benchmark 第一阶段：欢迎页
    包含：文本、统计数据、进入测试按钮
    """
    stats = get_statistics()
    return render_template('benchmark_welcome.html', stats=stats)

@pages_bp.route('/benchmark/test')
def benchmark_test():
    """
    Benchmark 第一阶段：测试页
    包含：所有题目、查询、打分、底部总分署名提交
    """
    from server.services.benchmark_service import load_questions
    questions = load_questions(version="v1")
    return render_template('benchmark_test.html', questions=questions)

# ===================== 新增：独立的 Issues 页面（完整保留你的原有代码） =====================
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

# ===================== 原有施工页路由（保留，调整了 Benchmark 路由避免冲突） =====================
@pages_bp.route('/constraints')
def constraints():
    return render_under_construction("数据库约束说明")

@pages_bp.route('/roadmap')
def roadmap():
    return render_under_construction("长期规划")