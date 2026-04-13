from flask import Flask
from servee.config import Config
from servee.routes.main import main_bp
from servee.routes.api import api_bp
from servee.routes.pages import pages_bp  # <-- 新增

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)
    app.register_blueprint(main_bp)
    app.register_blueprint(api_bp)
    app.register_blueprint(pages_bp)  # <-- 新增
    return app