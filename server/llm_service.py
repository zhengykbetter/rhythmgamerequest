from agent.intent_parser import parse_intent
from agent.sql_builder import generate_sql

def generate_sql_service(natural_query: str) -> str:
    """
    对外唯一入口函数
    兼容你原始 api.py 的调用方式
    """
    if not natural_query.strip():
        return ""

    # 1. 意图解析（核心步骤）
    intent_result = parse_intent(natural_query)
    
    # 2. 预留扩展位：lookup/domain/display/prompt_enhancer 可后续接入
    enhanced_query = intent_result["original"]

    # 3. 生成 SQL（核心步骤）
    return generate_sql(enhanced_query)

# 本地测试入口
if __name__ == "__main__":
    print("=== Agent NL2SQL 服务 ===")
    while True:
        q = input("\n请输入查询：")
        if q in ["exit", "退出"]:
            break
        sql = generate_sql_service(q)
        print(f"生成SQL：{sql if sql else '失败'}")