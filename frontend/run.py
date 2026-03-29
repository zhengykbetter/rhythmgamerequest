import sys
from pathlib import Path

# ===================== 核心修复：路径处理 =====================
# 获取当前文件 (run.py) 的绝对路径
FILE = Path(__file__).resolve()
# 定位到 frontend 目录
FRONTEND_ROOT = FILE.parent
# 定位到主项目根目录 (main_project)，因为 config 在那里
MAIN_PROJECT_ROOT = FRONTEND_ROOT.parent

# 将这两个目录都加入 sys.path，确保能找到所有模块
if str(FRONTEND_ROOT) not in sys.path:
    sys.path.insert(0, str(FRONTEND_ROOT))
if str(MAIN_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(MAIN_PROJECT_ROOT))
# ===============================================================

from server.app import create_app

app = create_app()

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000)