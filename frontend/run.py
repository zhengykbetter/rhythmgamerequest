import os
import sys
from pathlib import Path

# ===================== 核心路径配置（无冗余、绝对精准） =====================
# 获取 run.py 绝对路径
FILE = Path(__file__).resolve()
# frontend 根目录
FRONTEND_ROOT = FILE.parent
# 项目主根目录（frontend 的上级）
MAIN_PROJECT_ROOT = FRONTEND_ROOT.parent

# 强制将两个核心目录加入Python搜索路径（解决所有模块导入问题）
sys.path.insert(0, str(FRONTEND_ROOT))
sys.path.insert(0, str(MAIN_PROJECT_ROOT))
# ==========================================================================

# 导入应用（完全兼容你原有的导入方式）
from servee.app import create_app

app = create_app()

# 启动配置（保持你原有的参数）
if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000)