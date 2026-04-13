from agent.intent_parser import parse_intent
from agent.sql_builder import generate_sql
from agent.prompt_enhancer import build_structured_enhanced_prompt

def generate_sql_service(natural_query: str) -> str:
    print("\n" + "="*60)
    print(f"🤖 [总流程] 开始处理查询")
    print(f"📝 原始查询: {natural_query}")
    print("="*60)
    
    if not natural_query.strip():
        print("❌ [总流程] 查询为空，直接返回")
        return ""

    # 1. 意图解析
    print("\n🔄 [步骤1] 调用 intent_parser...")
    intent_result = parse_intent(natural_query)
    print(f"✅ [步骤1] intent_parser 完成")
    
    # 2. 【纯组装】结构化Prompt注入（无任何业务逻辑）
    print("\n🔄 [步骤2] 调用 prompt_enhancer 结构化注入...")
    enhanced_query = build_structured_enhanced_prompt(intent_result)
    print(f"📌 [增强后] 查询: {enhanced_query}")

    # 3. 生成SQL
    print("\n🔄 [步骤3] 调用 sql_builder...")
    sql = generate_sql(enhanced_query)
    
    # 最终结果
    print("\n" + "="*60)
    if sql:
        print(f"✅ [总流程] 成功生成 SQL")
        print(f"📄 最终 SQL: {sql}")
    else:
        print(f"❌ [总流程] 未生成有效 SQL")
    print("="*60 + "\n")
    
    return sql

# 本地测试入口
if __name__ == "__main__":
    print("=== Agent NL2SQL 服务 (调试版) ===")
    while True:
        q = input("\n请输入查询：")
        if q in ["exit", "退出"]:
            print("👋 再见！")
            break
        generate_sql_service(q)