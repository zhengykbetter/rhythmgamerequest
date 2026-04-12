from openai import OpenAI
from agent.config import LLM_CONFIG

client = OpenAI(api_key=LLM_CONFIG["api_key"], base_url=LLM_CONFIG["base_url"])

def parse_intent(query: str) -> dict:
    """
    输出固定结构化数据：最小可用版
    """
    if not query:
        return {"original": "", "entities": [], "is_complex": False}

    try:
        # 极简 Prompt：只做实体识别+复杂度判断
        prompt = f"""
        分析用户查询，输出JSON，无其他内容：
        1. entities：提取歌曲/作者/游戏实体
        2. is_complex：是否包含统计/排名/计算/排除（True/False）
        查询：{query}
        """
        
        resp = client.chat.completions.create(
            model=LLM_CONFIG["model"],
            temperature=0.0,
            messages=[{"role": "user", "content": prompt}]
        )
        
        return {
            "original": query,
            "entities": [],
            "is_complex": False,
            "raw_result": resp.choices[0].message.content.strip()
        }
    except:
        # 兜底：保证流程不中断
        return {"original": query, "entities": [], "is_complex": False}