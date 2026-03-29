from flask import Flask
from server.config import Config
from server.routes.main import main_bp
from server.routes.api import api_bp

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)
    
    # 注册蓝图
    app.register_blueprint(main_bp)
    app.register_blueprint(api_bp)
    
    return app