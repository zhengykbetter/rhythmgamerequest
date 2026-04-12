import re
from openai import OpenAI
from agent.config import LLM_CONFIG
# 导入：从 prompt_enhancer 获取表结构提示词
from agent.prompt_enhancer import get_table_schema_prompt
# 导入校验器
from agent.validator import validate_sql_safety

client = OpenAI(api_key=LLM_CONFIG["api_key"], base_url=LLM_CONFIG["base_url"])

def generate_sql(query: str) -> str:
    try:
        resp = client.chat.completions.create(
            model=LLM_CONFIG["model"],
            temperature=LLM_CONFIG["temperature"],
            messages=[
                # 调用迁移后的函数
                {"role": "system", "content": get_table_schema_prompt()},
                {"role": "user", "content": query}
            ]
        )
        sql = resp.choices[0].message.content.strip()
        sql = re.sub(r"```sql|```", "", sql).strip()
        
        return sql if validate_sql_safety(sql) else ""
        
    except Exception as e:
        from server.encoding_utils import safe_print
        safe_print(f"[❌ LLM生成SQL失败：{str(e)}]")
        return ""