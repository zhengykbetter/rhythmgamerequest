from flask import Blueprint, request, jsonify
from server.services.issue_service import add_issue, load_issues
from server.services.query_log_service import log_query_simple
from server.llm_service import llm_query  # 确保这个也能找到，如果报错也需要检查路径

api_bp = Blueprint('api', __name__, url_prefix='/api')

# ... 下面的代码保持不变 ...
@api_bp.route('/query', methods=['POST'])
def api_query():
    try:
        data = request.get_json()
        user_query = data.get("query", "")
        
        if not user_query:
            return jsonify({"error": "请输入查询内容"})
        
        # 【新增】记录问题
        log_query_simple(user_query)
        
        # 调用LLM查询服务
        result = llm_query(user_query)
        return jsonify({"data": result})
    
    except Exception as e:
        return jsonify({"error": str(e)})
@api_bp.route('/issues/submit', methods=['POST'])
def submit_issue():
    try:
        data = request.get_json()
        name, content = data.get("name", ""), data.get("content", "")
        if not name or not content: return jsonify({"success": False, "error": "请填写称呼和内容"})
        add_issue(name, data.get("contact", ""), content)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@api_bp.route('/issues/list', methods=['GET'])
def list_issues():
    try:
        return jsonify({"data": load_issues()})
    except Exception as e:
        return jsonify({"data": [], "error": str(e)})