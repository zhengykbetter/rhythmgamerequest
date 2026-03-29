from flask import Blueprint, render_template, request
from server.services.visit_service import get_visit_count, get_luck
from server.config import Config
from config.version import APP_VERSION  # 现在可以直接导入了

main_bp = Blueprint('main', __name__)

@main_bp.route('/')
def index():
    user_ip = request.remote_addr
    return render_template(
        'index.html',
        version=APP_VERSION,
        blog=Config.UPDATE_BLOG,
        luck_result=get_luck(user_ip),
        visit_count=get_visit_count()
    )