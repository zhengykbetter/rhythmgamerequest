from flask import Flask, render_template_string, request
import random
import hashlib
from datetime import date
import os
from pathlib import Path

# 导入配置
import sys
sys.path.append(str(Path(__file__).parent.parent.parent))
from config.version import APP_VERSION

app = Flask(__name__)

# ===================== 配置路径 =====================
BASE_DIR = Path(__file__).parent.parent
LOG_PATH = BASE_DIR / "blogshow" / "update.log"
COUNT_PATH = BASE_DIR / "blogshow" / "visit_count.txt"

# 加载日志
with open(LOG_PATH, "r", encoding="utf-8") as f:
    UPDATE_BLOG = f.read()

# 初始化访问计数文件
if not os.path.exists(COUNT_PATH):
    with open(COUNT_PATH, "w") as f:
        f.write("0")

# ===================== 访问计数函数 =====================
def get_visit_count():
    with open(COUNT_PATH, "r") as f:
        count = int(f.read().strip())
    # 访问自增
    count += 1
    with open(COUNT_PATH, "w") as f:
        f.write(str(count))
    return count

# ===================== 固定运势算法（同日同用户不变） =====================
def get_luck(ip):
    # 组合：用户IP + 当天日期 → 生成唯一标识
    today = str(date.today())
    unique_key = f"{ip}_{today}"
    # 哈希转数字
    hash_obj = hashlib.md5(unique_key.encode())
    hash_num = int(hash_obj.hexdigest(), 16)
    # 80% 100分，20% 10分，结果永久固定
    if hash_num % 10 < 8:
        return "恭喜你，你的运势是100分"
    else:
        return "恭喜你，你的运势是10分"

# ===================== 页面路由 =====================
@app.route('/')
def index():
    # 获取用户IP
    user_ip = request.remote_addr
    # 固定运势
    luck_result = get_luck(user_ip)
    # 访问人次
    visit_count = get_visit_count()
    
    html = '''
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>RhythmGameQuery</title>
        <style>
            body {
                text-align: center;
                margin-top: 50px;
                font-size: 20px;
                background: #f5f5f5;
            }
            button {
                padding: 10px 20px;
                font-size: 18px;
                cursor: pointer;
                margin: 20px;
                border-radius: 8px;
                border: none;
                background: #4285F4;
                color: white;
            }
            .info-box {
                margin: 20px auto;
                font-size: 16px;
                color: #555;
                line-height: 1.8;
            }
            /* 右下角悬浮版本号 */
            .version-box {
                position: fixed;
                right: 30px;
                bottom: 30px;
                text-align: center;
                background: white;
                padding: 15px 25px;
                border-radius: 12px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                min-width: 180px;
            }
            .version-text {
                font-size: 16px;
                color: #666;
                margin-bottom: 15px;
            }
            /* 弹窗 */
            .modal {
                display: none;
                position: fixed;
                top: 0;
                left: 0;
                width: 100%;
                height: 100%;
                background: rgba(0,0,0,0.7);
                z-index: 999;
            }
            .modal-content {
                position: absolute;
                top: 50%;
                left: 50%;
                transform: translate(-50%, -50%);
                background: white;
                padding: 30px;
                border-radius: 12px;
                width: 80%;
                max-width: 600px;
                max-height: 70vh;
                overflow-y: auto;
                text-align: left;
                white-space: pre-line;
            }
            .close-btn {
                position: absolute;
                right: 15px;
                top: 15px;
                font-size: 20px;
                cursor: pointer;
                background: none;
                color: #333;
            }
        </style>
    </head>
    <body>
        <h1>✅ 少女正在施工中，thinking...</h1>
        
        <button onclick="showLuck()">今日运势</button>
        <p id="result"></p>

        <!-- 新增：本地时间 + 访问人次 -->
        <div class="info-box">
            <div>🖥️ 本地时间：<span id="local-time"></span></div>
            <div>👀 累计访问：{{ visit_count }} 人次</div>
        </div>

        <!-- 右下角版本号 -->
        <div class="version-box">
            <div class="version-text">版本：{{ version }}</div>
            <button onclick="showBlog()">更新日志</button>
        </div>

        <!-- 日志弹窗 -->
        <div id="blogModal" class="modal">
            <div class="modal-content">
                <button class="close-btn" onclick="closeBlog()">×</button>
                <h3>📝 更新日志</h3>
                <div>{{ blog }}</div>
            </div>
        </div>

        <script>
            // 后端固定好的运势结果
            const LUCK_RESULT = "{{ luck_result }}";
            
            // 显示运势（结果固定）
            function showLuck() {
                document.getElementById("result").innerText = LUCK_RESULT;
            }

            // 实时本地时间
            function updateTime() {
                const now = new Date();
                document.getElementById("local-time").innerText = now.toLocaleString();
            }
            setInterval(updateTime, 1000);
            updateTime();

            // 弹窗控制
            function showBlog() {
                document.getElementById("blogModal").style.display = "block";
            }
            function closeBlog() {
                document.getElementById("blogModal").style.display = "none";
            }
            window.onclick = function(event) {
                let modal = document.getElementById("blogModal");
                if (event.target == modal) modal.style.display = "none";
            }
        </script>
    </body>
    </html>
    '''
    return render_template_string(
        html, 
        version=APP_VERSION, 
        blog=UPDATE_BLOG,
        luck_result=luck_result,
        visit_count=visit_count
    )

if __name__ == '__main__':
    app.run()