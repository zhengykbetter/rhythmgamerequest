from flask import Blueprint, render_template

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

@pages_bp.route('/issues')
def issues():
    return render_under_construction("关于 Issues") # 暂时也用施工中，或者你可以单独做页面