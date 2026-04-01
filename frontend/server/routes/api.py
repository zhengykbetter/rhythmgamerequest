from flask import Blueprint, request, jsonify
import time
from sqlalchemy import text

# 🔥 修复为你真实的导入路径（完全匹配你的原始代码）
from server.llm_service import llm_query, generate_sql
from server.services.db_service import get_mysql_engine
from server.services.issue_service import add_issue, load_issues

api_bp = Blueprint('api', __name__, url_prefix='/api')

@api_bp.route('/query', methods=['POST'])
def query():
    """
    改造后：智能查询接口，返回完整调试信息
    兼容你的原始 llm_query + 新增SQL/耗时/错误详情
    """
    start_time = time.time()
    user_query = request.json.get('query', '').strip()

    # 1. 基础参数校验
    if not user_query:
        return jsonify({
            "success": False,
            "user_query": user_query,
            "generated_sql": "",
            "execution_time": round(time.time() - start_time, 3),
            "result_count": 0,
            "data": [],
            "error_stage": "PARAM_CHECK",
            "error_msg": "请输入查询内容"
        }), 400

    generated_sql = ""
    try:
        # 2. 生成SQL（兼容你的llm_service）
        generated_sql = generate_sql(user_query)
        if not generated_sql:
            return jsonify({
                "success": False,
                "user_query": user_query,
                "generated_sql": generated_sql,
                "execution_time": round(time.time() - start_time, 3),
                "result_count": 0,
                "data": [],
                "error_stage": "LLM_GENERATE",
                "error_msg": "AI生成SQL失败，请更换提问方式"
            })

        # 3. 执行查询（兼容你的原始逻辑）
        data = llm_query(user_query)
        result_count = len(data)
        execution_time = round(time.time() - start_time, 3)

        # 4. 无数据场景
        if result_count == 0:
            return jsonify({
                "success": True,
                "user_query": user_query,
                "generated_sql": generated_sql,
                "execution_time": execution_time,
                "result_count": 0,
                "data": [],
                "error_stage": "NO_DATA",
                "error_msg": "SQL执行成功，数据库中未匹配到数据"
            })

        # 5. 查询成功
        return jsonify({
            "success": True,
            "user_query": user_query,
            "generated_sql": generated_sql,
            "execution_time": execution_time,
            "result_count": result_count,
            "data": data,
            "error_stage": "",
            "error_msg": ""
        })

    except Exception as e:
        # 全局异常捕获
        execution_time = round(time.time() - start_time, 3)
        return jsonify({
            "success": False,
            "user_query": user_query,
            "generated_sql": generated_sql,
            "execution_time": execution_time,
            "result_count": 0,
            "data": [],
            "error_stage": "SYSTEM_ERROR",
            "error_msg": f"查询失败：{str(e)}"
        })

# ===================== 你的原始Issue接口（无改动） =====================
@api_bp.route('/issues/submit', methods=['POST'])
def submit_issue():
    try:
        data = request.get_json()
        name, content = data.get("name", ""), data.get("content", "")
        if not name or not content: 
            return jsonify({"success": False, "error": "请填写称呼和内容"})
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