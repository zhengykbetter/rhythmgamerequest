from flask import Flask, render_template_string
import random
import os
from pathlib import Path

# 导入配置
import sys
sys.path.append(str(Path(__file__).parent.parent.parent))
from config.version import APP_VERSION

app = Flask(__name__)

# 读取更新日志
LOG_PATH = Path(__file__).parent.parent / "blogshow" / "update.log"
with open(LOG_PATH, "r", encoding="utf-8") as f:
    UPDATE_BLOG = f.read()

@app.route('/')
def index():
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
            /* 右下角悬浮版本号区域 */
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
            /* 弹窗样式 */
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
        <h1>✅ 我的网站跑通啦！</h1>
        <p>如果你是Potassium，CSNCSN！</p>
        <button onclick="checkLuck()">今日运势</button>
        <p id="result"></p>

        <!-- 右下角版本号 + 日志按钮 -->
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
            // 运势功能
            function checkLuck() {
                let rand = Math.random();
                let res = rand < 0.8 ? "恭喜你，你的运势是100分" : "恭喜你，你的运势是10分";
                document.getElementById("result").innerText = res;
            }
            // 弹窗控制
            function showBlog() {
                document.getElementById("blogModal").style.display = "block";
            }
            function closeBlog() {
                document.getElementById("blogModal").style.display = "none";
            }
            // 点击空白关闭弹窗
            window.onclick = function(event) {
                let modal = document.getElementById("blogModal");
                if (event.target == modal) modal.style.display = "none";
            }
        </script>
    </body>
    </html>
    '''
    return render_template_string(html, version=APP_VERSION, blog=UPDATE_BLOG)

if __name__ == '__main__':
    app.run()