from flask import Flask, render_template_string, request, jsonify
import random
import hashlib
from datetime import date, datetime
import os
import json
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
ISSUES_PATH = BASE_DIR.parent / "data_issues" / "issues.json"

# 确保Issues目录存在
os.makedirs(os.path.dirname(ISSUES_PATH), exist_ok=True)
if not os.path.exists(ISSUES_PATH):
    with open(ISSUES_PATH, "w", encoding="utf-8") as f:
        json.dump([], f)

# 加载日志
with open(LOG_PATH, "r", encoding="utf-8") as f:
    UPDATE_BLOG = f.read()

# 初始化访问计数
if not os.path.exists(COUNT_PATH):
    with open(COUNT_PATH, "w") as f:
        f.write("0")

# ===================== Issues 数据操作 =====================
def load_issues():
    with open(ISSUES_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def save_issues(issues):
    with open(ISSUES_PATH, "w", encoding="utf-8") as f:
        json.dump(issues, f, ensure_ascii=False, indent=2)

def add_issue(name, contact, content):
    issues = load_issues()
    new_issue = {
        "id": len(issues) + 1,
        "name": name,
        "contact": contact,
        "content": content,
        "status": "待处理",
        "create_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    issues.append(new_issue)
    save_issues(issues)
    return new_issue

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
                padding-bottom: 100px;
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
            .table-wrapper {
                overflow-x: auto;
                border-radius: 12px;
                border: 1px solid #e0e0e0;
                margin-top: 15px;
            }
            .result-table {
                width: 100%;
                border-collapse: collapse;
                min-width: 800px;
            }
            .result-table th, .result-table td {
                padding: 12px 15px;
                text-align: left;
                border-bottom: 1px solid #e0e0e0;
                white-space: nowrap;
            }
            .result-table th {
                background: #f5f5f5;
                font-weight: 600;
                color: #667eea;
                position: sticky;
                top: 0;
            }
            .result-table tr:hover {
                background: #f9f9f9;
            }
            .table-wrapper::-webkit-scrollbar {
                height: 10px;
            }
            .table-wrapper::-webkit-scrollbar-track {
                background: #f1f1f1;
                border-radius: 5px;
            }
            .table-wrapper::-webkit-scrollbar-thumb {
                background: #667eea;
                border-radius: 5px;
            }
            .table-wrapper::-webkit-scrollbar-thumb:hover {
                background: #5a6fd6;
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
            .success {
                background: #f0fff4;
                color: #2e7d32;
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
            /* 🔥 新增：Issues 底部入口 */
            .issues-bar {
                position: fixed;
                bottom: 0;
                left: 0;
                right: 0;
                background: white;
                padding: 15px 30px;
                box-shadow: 0 -10px 30px rgba(0,0,0,0.1);
                display: flex;
                justify-content: center;
                gap: 20px;
                z-index: 100;
            }
            .issues-btn {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                border: none;
                padding: 12px 30px;
                border-radius: 10px;
                cursor: pointer;
                font-size: 1rem;
                font-weight: 600;
                transition: all 0.3s;
            }
            .issues-btn:hover {
                transform: translateY(-2px);
                box-shadow: 0 5px 15px rgba(102, 126, 234, 0.4);
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
                max-width: 600px;
                max-height: 80vh;
                overflow-y: auto;
            }
            .modal-content.large {
                max-width: 900px;
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
            .form-group {
                margin-bottom: 20px;
            }
            .form-group label {
                display: block;
                margin-bottom: 8px;
                font-weight: 600;
                color: #667eea;
            }
            .form-group input, .form-group textarea {
                width: 100%;
                padding: 12px 15px;
                border: 2px solid #e0e0e0;
                border-radius: 10px;
                font-size: 1rem;
                outline: none;
                transition: all 0.3s;
            }
            .form-group input:focus, .form-group textarea:focus {
                border-color: #667eea;
                box-shadow: 0 0 0 4px rgba(102, 126, 234, 0.1);
            }
            .form-group textarea {
                min-height: 120px;
                resize: vertical;
            }
            .submit-btn {
                width: 100%;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                border: none;
                padding: 15px;
                border-radius: 10px;
                font-size: 1.1rem;
                font-weight: 600;
                cursor: pointer;
                transition: all 0.3s;
            }
            .submit-btn:hover {
                transform: translateY(-2px);
                box-shadow: 0 10px 20px rgba(102, 126, 234, 0.4);
            }
            .issue-card {
                background: #f9f9f9;
                border-radius: 12px;
                padding: 20px;
                margin-bottom: 15px;
                border-left: 4px solid #667eea;
            }
            .issue-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 10px;
            }
            .issue-name {
                font-weight: 600;
                color: #667eea;
                font-size: 1.1rem;
            }
            .issue-status {
                padding: 4px 12px;
                border-radius: 20px;
                font-size: 0.85rem;
                font-weight: 600;
            }
            .status-pending { background: #fff3cd; color: #856404; }
            .status-processing { background: #cce5ff; color: #004085; }
            .status-done { background: #d4edda; color: #155724; }
            .issue-content {
                margin: 10px 0;
                line-height: 1.6;
            }
            .issue-meta {
                color: #666;
                font-size: 0.9rem;
                margin-top: 10px;
            }
            @media (max-width: 768px) {
                .header h1 { font-size: 1.8rem; }
                .main-card { padding: 20px; }
                .version-box { position: static; margin-top: 20px; }
                .issues-bar { padding: 10px; flex-direction: column; gap: 10px; }
                .issues-btn { width: 100%; }
                body { padding-bottom: 180px; }
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
                    <div class="table-wrapper">
                        <table id="resultTable" class="result-table"></table>
                    </div>
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

        <!-- 🔥 新增：Issues 底部入口 -->
        <div class="issues-bar">
            <button class="issues-btn" onclick="showSubmitModal()">📝 提交 Issue</button>
            <button class="issues-btn" onclick="showViewModal()">👀 查看 Issues</button>
        </div>

        <!-- 提交 Issue 弹窗 -->
        <div id="submitModal" class="modal">
            <div class="modal-content">
                <span class="close-btn" onclick="closeSubmitModal()">×</span>
                <h2 style="color:#667eea;margin-bottom:25px;">📝 提交 Issue</h2>
                <div id="submitSuccess" class="success" style="display:none;">提交成功！感谢你的反馈！</div>
                <div id="submitError" class="error" style="display:none;"></div>
                <div class="form-group">
                    <label>称呼</label>
                    <input type="text" id="issueName" placeholder="请输入你的称呼">
                </div>
                <div class="form-group">
                    <label>联系方式</label>
                    <input type="text" id="issueContact" placeholder="邮箱/QQ/微信等（选填）">
                </div>
                <div class="form-group">
                    <label>Issue 内容</label>
                    <textarea id="issueContent" placeholder="请详细描述你的问题或建议..."></textarea>
                </div>
                <button class="submit-btn" onclick="submitIssue()">提交</button>
            </div>
        </div>

        <!-- 查看 Issues 弹窗 -->
        <div id="viewModal" class="modal">
            <div class="modal-content large">
                <span class="close-btn" onclick="closeViewModal()">×</span>
                <h2 style="color:#667eea;margin-bottom:25px;">👀 所有 Issues</h2>
                <div id="issuesList"></div>
            </div>
        </div>

        <!-- 更新日志弹窗 -->
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

            // Issues 功能
            function showSubmitModal() {
                document.getElementById("submitModal").style.display = "block";
                document.getElementById("submitSuccess").style.display = "none";
                document.getElementById("submitError").style.display = "none";
                document.getElementById("issueName").value = "";
                document.getElementById("issueContact").value = "";
                document.getElementById("issueContent").value = "";
            }
            function closeSubmitModal() {
                document.getElementById("submitModal").style.display = "none";
            }

            async function submitIssue() {
                const name = document.getElementById("issueName").value.trim();
                const contact = document.getElementById("issueContact").value.trim();
                const content = document.getElementById("issueContent").value.trim();

                if (!name || !content) {
                    document.getElementById("submitError").innerText = "请填写称呼和内容！";
                    document.getElementById("submitError").style.display = "block";
                    return;
                }

                try {
                    const response = await fetch("/api/issues/submit", {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({ name, contact, content })
                    });
                    const data = await response.json();

                    if (data.success) {
                        document.getElementById("submitSuccess").style.display = "block";
                        document.getElementById("submitError").style.display = "none";
                        setTimeout(closeSubmitModal, 2000);
                    } else {
                        document.getElementById("submitError").innerText = data.error;
                        document.getElementById("submitError").style.display = "block";
                    }
                } catch (e) {
                    document.getElementById("submitError").innerText = "提交失败：" + e.message;
                    document.getElementById("submitError").style.display = "block";
                }
            }

            async function showViewModal() {
                document.getElementById("viewModal").style.display = "block";
                try {
                    const response = await fetch("/api/issues/list");
                    const data = await response.json();
                    renderIssues(data.data);
                } catch (e) {
                    document.getElementById("issuesList").innerHTML = "<div class='error'>加载失败：" + e.message + "</div>";
                }
            }
            function closeViewModal() {
                document.getElementById("viewModal").style.display = "none";
            }

            function renderIssues(issues) {
                const container = document.getElementById("issuesList");
                if (!issues || issues.length === 0) {
                    container.innerHTML = "<p style='text-align:center;color:#666;'>暂无 Issues</p>";
                    return;
                }

                let html = "";
                issues.reverse().forEach(issue => {
                    let statusClass = "status-pending";
                    if (issue.status === "处理中") statusClass = "status-processing";
                    if (issue.status === "已解决") statusClass = "status-done";

                    html += `
                        <div class="issue-card">
                            <div class="issue-header">
                                <span class="issue-name">${issue.name}</span>
                                <span class="issue-status ${statusClass}">${issue.status}</span>
                            </div>
                            <div class="issue-content">${issue.content}</div>
                            <div class="issue-meta">
                                📅 ${issue.create_time}
                                ${issue.contact ? ` | 📞 ${issue.contact}` : ""}
                            </div>
                        </div>
                    `;
                });
                container.innerHTML = html;
            }

            function showBlog() {
                document.getElementById("blogModal").style.display = "block";
            }
            function closeBlog() {
                document.getElementById("blogModal").style.display = "none";
            }
            window.onclick = function(event) {
                if (event.target == document.getElementById("blogModal")) closeBlog();
                if (event.target == document.getElementById("submitModal")) closeSubmitModal();
                if (event.target == document.getElementById("viewModal")) closeViewModal();
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

# ===================== LLM查询API接口 =====================
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

# ===================== Issues API接口 =====================
@app.route('/api/issues/submit', methods=['POST'])
def submit_issue_api():
    try:
        data = request.get_json()
        name = data.get("name", "")
        contact = data.get("contact", "")
        content = data.get("content", "")
        
        if not name or not content:
            return jsonify({"success": False, "error": "请填写称呼和内容"})
        
        add_issue(name, contact, content)
        return jsonify({"success": True})
    
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/api/issues/list', methods=['GET'])
def list_issues_api():
    try:
        issues = load_issues()
        return jsonify({"data": issues})
    
    except Exception as e:
        return jsonify({"data": [], "error": str(e)})

if __name__ == '__main__':
    app.run()