from flask import Flask, render_template_string, request, jsonify
import random
import hashlib
from datetime import date
import os
from pathlib import Path

# 导入项目配置
import sys
sys.path.append(str(Path(__file__).parent.parent.parent))
from config.version import APP_VERSION
from server.llm_service import llm_query

app = Flask(__name__)

# ===================== 配置路径 =====================
BASE_DIR = Path(__file__).parent.parent
LOG_PATH = BASE_DIR / "blogshow" / "update.log"
COUNT_PATH = BASE_DIR / "blogshow" / "visit_count.txt"

# 加载日志
with open(LOG_PATH, "r", encoding="utf-8") as f:
    UPDATE_BLOG = f.read()

# 初始化访问计数
if not os.path.exists(COUNT_PATH):
    with open(COUNT_PATH, "w") as f:
        f.write("0")

# ===================== 访问计数 =====================
def get_visit_count():
    with open(COUNT_PATH, "r") as f:
        count = int(f.read().strip())
    count += 1
    with open(COUNT_PATH, "w") as f:
        f.write(str(count))
    return count

# ===================== 固定运势算法 =====================
def get_luck(ip):
    today = str(date.today())
    unique_key = f"{ip}_{today}"
    hash_obj = hashlib.md5(unique_key.encode())
    hash_num = int(hash_obj.hexdigest(), 16)
    if hash_num % 10 < 8:
        return "恭喜你，你的运势是100分"
    else:
        return "恭喜你，你的运势是10分"

# ===================== 页面路由 =====================
@app.route('/')
def index():
    user_ip = request.remote_addr
    luck_result = get_luck(user_ip)
    visit_count = get_visit_count()
    
    html = '''
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>RhythmGameQuery - 音游数据智能查询</title>
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body {
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
                padding: 20px;
                color: #333;
            }
            .container {
                max-width: 1200px;
                margin: 0 auto;
            }
            .header {
                text-align: center;
                color: white;
                margin-bottom: 30px;
            }
            .header h1 {
                font-size: 2.5rem;
                margin-bottom: 10px;
                text-shadow: 0 2px 10px rgba(0,0,0,0.2);
            }
            .header p {
                font-size: 1.1rem;
                opacity: 0.9;
            }
            .main-card {
                background: white;
                border-radius: 20px;
                box-shadow: 0 20px 60px rgba(0,0,0,0.3);
                padding: 40px;
                margin-bottom: 20px;
            }
            .section-title {
                font-size: 1.5rem;
                font-weight: 600;
                margin-bottom: 20px;
                color: #667eea;
                border-left: 4px solid #667eea;
                padding-left: 15px;
            }
            .query-box {
                margin-bottom: 30px;
            }
            .query-input {
                width: 100%;
                padding: 15px 20px;
                font-size: 1.1rem;
                border: 2px solid #e0e0e0;
                border-radius: 12px;
                outline: none;
                transition: all 0.3s;
                margin-bottom: 15px;
            }
            .query-input:focus {
                border-color: #667eea;
                box-shadow: 0 0 0 4px rgba(102, 126, 234, 0.1);
            }
            .query-btn {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                border: none;
                padding: 15px 40px;
                font-size: 1.1rem;
                border-radius: 12px;
                cursor: pointer;
                transition: all 0.3s;
                font-weight: 600;
            }
            .query-btn:hover {
                transform: translateY(-2px);
                box-shadow: 0 10px 20px rgba(102, 126, 234, 0.4);
            }
            .query-btn:disabled {
                opacity: 0.6;
                cursor: not-allowed;
                transform: none;
            }
            .result-box {
                margin-top: 30px;
                display: none;
            }
            .result-table {
                width: 100%;
                border-collapse: collapse;
                margin-top: 15px;
            }
            .result-table th, .result-table td {
                padding: 12px 15px;
                text-align: left;
                border-bottom: 1px solid #e0e0e0;
            }
            .result-table th {
                background: #f5f5f5;
                font-weight: 600;
                color: #667eea;
            }
            .result-table tr:hover {
                background: #f9f9f9;
            }
            .loading {
                text-align: center;
                padding: 30px;
                color: #667eea;
                font-size: 1.1rem;
            }
            .error {
                background: #fff0f0;
                color: #d32f2f;
                padding: 15px;
                border-radius: 8px;
                margin-top: 15px;
            }
            .info-row {
                display: flex;
                justify-content: space-between;
                flex-wrap: wrap;
                gap: 20px;
                margin-top: 30px;
                padding-top: 30px;
                border-top: 1px solid #e0e0e0;
            }
            .info-card {
                flex: 1;
                min-width: 200px;
                background: #f5f5f5;
                padding: 20px;
                border-radius: 12px;
                text-align: center;
            }
            .info-card h3 {
                font-size: 1.2rem;
                margin-bottom: 10px;
                color: #667eea;
            }
            .info-card p {
                font-size: 1.5rem;
                font-weight: 600;
                color: #333;
            }
            .version-box {
                position: fixed;
                right: 30px;
                bottom: 30px;
                background: white;
                padding: 20px 30px;
                border-radius: 15px;
                box-shadow: 0 10px 30px rgba(0,0,0,0.2);
                text-align: center;
            }
            .version-text {
                font-size: 1rem;
                color: #666;
                margin-bottom: 10px;
            }
            .blog-btn {
                background: #667eea;
                color: white;
                border: none;
                padding: 8px 20px;
                border-radius: 8px;
                cursor: pointer;
                font-size: 0.9rem;
            }
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
                padding: 40px;
                border-radius: 20px;
                width: 90%;
                max-width: 700px;
                max-height: 80vh;
                overflow-y: auto;
            }
            .close-btn {
                position: absolute;
                right: 20px;
                top: 20px;
                font-size: 2rem;
                cursor: pointer;
                color: #999;
            }
            .close-btn:hover {
                color: #333;
            }
            @media (max-width: 768px) {
                .header h1 { font-size: 1.8rem; }
                .main-card { padding: 20px; }
                .version-box { position: static; margin-top: 20px; }
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🎮 RhythmGameQuery</h1>
                <p>少女正在施工中，thinking...</p>
            </div>

            <div class="main-card">
                <div class="section-title">🔍 智能数据查询</div>
                <div class="query-box">
                    <input type="text" id="queryInput" class="query-input" placeholder="请输入自然语言查询，例如：查询所有游戏名称 / 查询Phigros的歌曲">
                    <button id="queryBtn" class="query-btn" onclick="executeQuery()">开始查询</button>
                </div>

                <div id="loading" class="loading" style="display:none;">
                    正在查询中，请稍候...
                </div>

                <div id="error" class="error" style="display:none;"></div>

                <div id="resultBox" class="result-box">
                    <div class="section-title">📊 查询结果</div>
                    <table id="resultTable" class="result-table"></table>
                </div>

                <div class="info-row">
                    <div class="info-card">
                        <h3>今日运势</h3>
                        <button onclick="showLuck()" style="background:#667eea;color:white;border:none;padding:10px 20px;border-radius:8px;cursor:pointer;">查看运势</button>
                        <p id="luckResult" style="font-size:1rem;margin-top:10px;"></p>
                    </div>
                    <div class="info-card">
                        <h3>本地时间</h3>
                        <p id="localTime" style="font-size:1.2rem;"></p>
                    </div>
                    <div class="info-card">
                        <h3>累计访问</h3>
                        <p>{{ visit_count }} 人次</p>
                    </div>
                </div>
            </div>
        </div>

        <div class="version-box">
            <div class="version-text">版本：{{ version }}</div>
            <button class="blog-btn" onclick="showBlog()">更新日志</button>
        </div>

        <div id="blogModal" class="modal">
            <div class="modal-content">
                <span class="close-btn" onclick="closeBlog()">×</span>
                <h2 style="color:#667eea;margin-bottom:20px;">📝 更新日志</h2>
                <pre style="white-space:pre-line;line-height:1.8;font-size:1rem;">{{ blog }}</pre>
            </div>
        </div>

        <script>
            const LUCK_RESULT = "{{ luck_result }}";
            
            function showLuck() {
                document.getElementById("luckResult").innerText = LUCK_RESULT;
            }

            function updateTime() {
                const now = new Date();
                document.getElementById("localTime").innerText = now.toLocaleString();
            }
            setInterval(updateTime, 1000);
            updateTime();

            async function executeQuery() {
                const query = document.getElementById("queryInput").value.trim();
                if (!query) {
                    alert("请输入查询内容！");
                    return;
                }

                document.getElementById("loading").style.display = "block";
                document.getElementById("resultBox").style.display = "none";
                document.getElementById("error").style.display = "none";
                document.getElementById("queryBtn").disabled = true;

                try {
                    const response = await fetch("/api/query", {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({ query: query })
                    });
                    const data = await response.json();

                    if (data.error) {
                        document.getElementById("error").innerText = data.error;
                        document.getElementById("error").style.display = "block";
                    } else if (data.data && data.data.length > 0) {
                        renderTable(data.data);
                        document.getElementById("resultBox").style.display = "block";
                    } else {
                        document.getElementById("error").innerText = "未查询到数据";
                        document.getElementById("error").style.display = "block";
                    }
                } catch (e) {
                    document.getElementById("error").innerText = "查询失败：" + e.message;
                    document.getElementById("error").style.display = "block";
                } finally {
                    document.getElementById("loading").style.display = "none";
                    document.getElementById("queryBtn").disabled = false;
                }
            }

            function renderTable(data) {
                const table = document.getElementById("resultTable");
                const headers = Object.keys(data[0]);
                let html = "<thead><tr>";
                headers.forEach(h => html += `<th>${h}</th>`);
                html += "</tr></thead><tbody>";
                data.forEach(row => {
                    html += "<tr>";
                    headers.forEach(h => html += `<td>${row[h] || ""}</td>`);
                    html += "</tr>";
                });
                html += "</tbody>";
                table.innerHTML = html;
            }

            function showBlog() {
                document.getElementById("blogModal").style.display = "block";
            }
            function closeBlog() {
                document.getElementById("blogModal").style.display = "none";
            }
            window.onclick = function(event) {
                if (event.target == document.getElementById("blogModal")) closeBlog();
            }

            document.getElementById("queryInput").addEventListener("keypress", function(e) {
                if (e.key === "Enter") executeQuery();
            });
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

# ===================== 🔥 新增：LLM查询API接口 =====================
@app.route('/api/query', methods=['POST'])
def api_query():
    try:
        data = request.get_json()
        user_query = data.get("query", "")
        
        if not user_query:
            return jsonify({"error": "请输入查询内容"})
        
        # 调用LLM查询服务
        result = llm_query(user_query)
        
        return jsonify({"data": result})
    
    except Exception as e:
        return jsonify({"error": str(e)})

if __name__ == '__main__':
    app.run()