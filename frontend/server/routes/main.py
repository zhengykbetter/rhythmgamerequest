from flask import Blueprint, render_template, request
from server.services.visit_service import get_visit_count, get_luck
from server.services.db_service import get_dashboard_stats  # <-- 新增
from server.config import Config
from config.version import APP_VERSION

main_bp = Blueprint('main', __name__)

@main_bp.route('/')
def index():
    user_ip = request.remote_addr
    
    # 【新增】从数据库获取统计数据
    stats = get_dashboard_stats()
    
    return render_template(
        'index.html',
        version=APP_VERSION,
        blog=Config.UPDATE_BLOG,
        luck_result=get_luck(user_ip),
        visit_count=get_visit_count(),
        # 【新增】传入这三个变量
        info_count=stats['info_count'],
        song_count=stats['song_count'],
        artist_count=stats['artist_count']
    )