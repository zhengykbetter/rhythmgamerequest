import re
import json
from openai import OpenAI
from agent.config import LLM_CONFIG

client = OpenAI(api_key=LLM_CONFIG["api_key"], base_url=LLM_CONFIG["base_url"])

def parse_intent(query: str) -> dict:
    print(f"   📥 [intent_parser] 输入: {query}")
    
    if not query:
        print(f"   ⚠️  [intent_parser] 输入为空")
        return {"original": query, "entities": [], "is_complex": False}

    try:
        prompt = f"""
分析用户查询，严格按以下最高优先级规则输出JSON，无其他内容：
【最高优先级语义规则】
1. 只要查询出现「X的作者」句式，X必须归类到【歌曲】实体，绝对不能归类到【作者】实体
2. 只要查询出现「X的歌曲」句式，X必须归类到【作者】实体，绝对不能归类到【歌曲】实体

【输出要求】
输出严格JSON格式，包含两个字段：
1. entities：对象，key固定为「歌曲」「作者」「游戏」，value为对应提取到的实体数组
2. is_complex：布尔值，查询包含统计/排名/计算/排除则为True，否则为False

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
        print(f"   📤 [intent_parser] LLM 原始输出: {raw_result[:150]}...")
        
        # ===================== 【核心修复】解析 JSON =====================
        # 1. 去掉 ```json 和 ``` 标记
        json_str = re.sub(r"```json|```", "", raw_result).strip()
        
        # 2. 解析成 Python 字典
        parsed_data = json.loads(json_str)
        
        # 3. 提取 entities 和 is_complex
        entities = parsed_data.get("entities", [])
        is_complex = parsed_data.get("is_complex", False)
        
        print(f"   ✅ [intent_parser] JSON 解析成功")
        print(f"   📌 [intent_parser] 识别到的歌曲: {entities.get('歌曲', [])}")
        print(f"   📌 [intent_parser] 识别到的作者: {entities.get('作者', [])}")
        print(f"   📌 [intent_parser] 识别到的游戏: {entities.get('游戏', [])}")
        print(f"   📌 [intent_parser] 是否复杂: {is_complex}")
        
        return {
            "original": query,
            "entities": entities,
            "is_complex": is_complex,
            "raw_result": raw_result
        }
    except Exception as e:
        print(f"   ❌ [intent_parser] 出错: {str(e)}")
        # 兜底：解析失败也不崩溃
        return {"original": query, "entities": [], "is_complex": False}