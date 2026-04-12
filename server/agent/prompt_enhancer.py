from server.encoding_utils import safe_print
from agent.config import TABLE_RULES
from datetime import datetime

def get_current_time_info():
    now = datetime.now()
    return {
        "current_date": now.strftime("%Y-%m-%d"),
        "current_datetime": now.strftime("%Y-%m-%d %H:%M:%S"),
        "sql_date_format": "%Y-%m-%d",
        "weekday_mapping": "1=周一, 2=周二, 3=周三, 4=周四, 5=周五, 6=周六, 7=周日"
    }

def get_table_schema_prompt():
    safe_print(f"      📋 [prompt_enhancer] 生成系统提示词...")
    
    time_info = get_current_time_info()
    safe_print(f"      ⏰ [prompt_enhancer] 注入时间: {time_info['current_date']}")
    
    # 基础表结构
    schema_prompt = "数据库表结构如下（仅允许查询这些表）：\n"
    for table in TABLE_RULES["create_order"]:
        rule = TABLE_RULES[table]
        fields = list(rule["field_types"].keys())
        schema_prompt += f"表名：{table} | 字段：{', '.join(fields)}\n"

    # 注入 datetime 时间信息
    schema_prompt += f"\n【时间规则（来自datetime模块）】\n"
    schema_prompt += f"1. 当前日期：{time_info['current_date']}\n"
    schema_prompt += f"2. 标准时间格式：{time_info['sql_date_format']}\n"
    schema_prompt += f"3. 星期对应规则：{time_info['weekday_mapping']}\n"

    # 原有SQL规则
    schema_prompt += "\n【SQL生成规则】\n"
    schema_prompt += "1. 仅生成SELECT查询语句，禁止任何增删改\n"
    schema_prompt += "2. 仅使用上述表和字段\n"
    schema_prompt += "3. 返回纯SQL，不要任何解释\n"
    schema_prompt += "4. 字段名必须严格匹配\n"
    schema_prompt += "5. 时间查询使用标准SQL日期函数"
    
    safe_print(f"      ✅ [prompt_enhancer] 提示词生成完成，长度: {len(schema_prompt)}")
    return schema_prompt