import re
from server.encoding_utils import safe_print
from openai import OpenAI
from agent.config import LLM_CONFIG
from agent.prompt_enhancer import get_table_schema_prompt
from agent.validator import validate_sql_safety

client = OpenAI(api_key=LLM_CONFIG["api_key"], base_url=LLM_CONFIG["base_url"])

def generate_sql(query: str) -> str:
    safe_print(f"   📥 [sql_builder] 输入: {query}")
    
    try:
        system_prompt = get_table_schema_prompt()
        safe_print(f"   📋 [sql_builder] 系统提示词长度: {len(system_prompt)} 字符")
        safe_print(f"   🔤 [sql_builder] 调用 LLM (SQL生成)...")
        
        resp = client.chat.completions.create(
            model=LLM_CONFIG["model"],
            temperature=LLM_CONFIG["temperature"],
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": query}
            ]
        )
        
        raw_sql = resp.choices[0].message.content.strip()
        token_used = resp.usage.total_tokens if hasattr(resp, 'usage') else 0
        
        safe_print(f"   ✅ [sql_builder] LLM 调用成功")
        safe_print(f"   🔢 [sql_builder] Token 消耗: {token_used}")
        safe_print(f"   📄 [sql_builder] 原始输出: {raw_sql[:100]}...")
        
        # 清洗 SQL
        sql = re.sub(r"```sql|```", "", raw_sql).strip()
        safe_print(f"   🧹 [sql_builder] 清洗后 SQL: {sql[:100]}...")
        
        # 安全校验
        safe_print(f"   🔍 [sql_builder] 调用 validator 校验...")
        is_valid = validate_sql_safety(sql)
        
        if is_valid:
            safe_print(f"   ✅ [sql_builder] 校验通过")
            return sql
        else:
            safe_print(f"   ❌ [sql_builder] 校验失败")
            return ""
        
    except Exception as e:
        safe_print(f"   ❌ [sql_builder] 出错: {str(e)}")
        return ""