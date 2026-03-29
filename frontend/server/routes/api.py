from flask import Blueprint, request, jsonify
from server.services.issue_service import add_issue, load_issues
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))
from server.llm_service import llm_query

api_bp = Blueprint('api', __name__, url_prefix='/api')

@api_bp.route('/query', methods=['POST'])
def api_query():
    try:
        data = request.get_json()
        user_query = data.get("query", "")
        if not user_query: return jsonify({"error": "请输入查询内容"})
        return jsonify({"data": llm_query(user_query)})
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