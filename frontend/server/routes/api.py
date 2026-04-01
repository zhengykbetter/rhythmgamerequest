from flask import Blueprint, request, jsonify
import time
from sqlalchemy import text
from server.services.llm_service import generate_sql
from server.services.db_service import get_mysql_engine
from server.services.issue_service import add_issue

api_bp = Blueprint('api', __name__)

@api_bp.route('/query', methods=['POST'])
def query():
    """
    改造后：智能查询接口，返回完整调试信息
    统一返回结构：success/user_query/generated_sql/execution_time/result_count/data/error_stage/error_msg
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
        # 2. LLM生成SQL阶段
        generated_sql = generate_sql(user_query)
        if not generated_sql or not generated_sql.strip().upper().startswith("SELECT"):
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

        # 3. SQL执行阶段
        engine = get_mysql_engine()
        if not engine:
            return jsonify({
                "success": False,
                "user_query": user_query,
                "generated_sql": generated_sql,
                "execution_time": round(time.time() - start_time, 3),
                "result_count": 0,
                "data": [],
                "error_stage": "DB_CONNECT",
                "error_msg": "数据库连接失败"
            })

        with engine.connect() as conn:
            result = conn.execute(text(generated_sql))
            data = [dict(row) for row in result.mappings()]
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

# ===================== 原有Issue接口保持不变 =====================
@api_bp.route('/issues/submit', methods=['POST'])
def submit_issue():
    data = request.json
    name = data.get('name', '').strip()
    contact = data.get('contact', '').strip()
    content = data.get('content', '').strip()
    if not name or not content:
        return jsonify({"success": False, "error": "请填写称呼和内容"})
    add_issue(name, contact, content)
    return jsonify({"success": True})

@api_bp.route('/issues/list', methods=['GET'])
def list_issues():
    from server.services.issue_service import load_issues
    return jsonify({"success": True, "data": load_issues()})