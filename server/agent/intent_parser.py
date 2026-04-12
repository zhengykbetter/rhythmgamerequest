from openai import OpenAI
from agent.config import LLM_CONFIG

client = OpenAI(api_key=LLM_CONFIG["api_key"], base_url=LLM_CONFIG["base_url"])

def parse_intent(query: str) -> dict:
    print(f"   📥 [intent_parser] 输入: {query}")
    
    if not query:
        print(f"   ⚠️  [intent_parser] 输入为空")
        return {"original": "", "entities": [], "is_complex": False}

    try:
        prompt = f"""
        分析用户查询，输出JSON，无其他内容：
        1. entities：提取歌曲/作者/游戏实体
        2. is_complex：是否包含统计/排名/计算/排除（True/False）
        查询：{query}
        """
        
        print(f"   🔤 [intent_parser] 调用 LLM (意图解析)...")
        print(f"   📝 [intent_parser] LLM 输入长度: {len(prompt)} 字符")
        
        resp = client.chat.completions.create(
            model=LLM_CONFIG["model"],
            temperature=0.0,
            messages=[{"role": "user", "content": prompt}]
        )
        
        raw_result = resp.choices[0].message.content.strip()
        token_used = resp.usage.total_tokens if hasattr(resp, 'usage') else 0
        
        print(f"   ✅ [intent_parser] LLM 调用成功")
        print(f"   🔢 [intent_parser] Token 消耗: {token_used}")
        print(f"   📤 [intent_parser] LLM 输出: {raw_result[:100]}...")
        
        return {
            "original": query,
            "entities": [],
            "is_complex": False,
            "raw_result": raw_result
        }
    except Exception as e:
        print(f"   ❌ [intent_parser] 出错: {str(e)}")
        return {"original": query, "entities": [], "is_complex": False}