from server.encoding_utils import safe_print
from agent.intent_parser import parse_intent
from agent.sql_builder import generate_sql

def generate_sql_service(natural_query: str) -> str:
    safe_print("\n" + "="*60)
    safe_print(f"🤖 [总流程] 开始处理查询")
    safe_print(f"📝 原始查询: {natural_query}")
    safe_print("="*60)
    
    if not natural_query.strip():
        safe_print("❌ [总流程] 查询为空，直接返回")
        return ""

    # 1. 意图解析
    safe_print("\n🔄 [步骤1] 调用 intent_parser...")
    intent_result = parse_intent(natural_query)
    safe_print(f"✅ [步骤1] intent_parser 完成")
    
    # 2. 预留扩展位
    enhanced_query = intent_result["original"]
    safe_print(f"📌 [透传] 最终查询: {enhanced_query}")

    # 3. 生成 SQL
    safe_print("\n🔄 [步骤2] 调用 sql_builder...")
    sql = generate_sql(enhanced_query)
    
    # 4. 最终结果
    safe_print("\n" + "="*60)
    if sql:
        safe_print(f"✅ [总流程] 成功生成 SQL")
        safe_print(f"📄 最终 SQL: {sql}")
    else:
        safe_print(f"❌ [总流程] 未生成有效 SQL")
    safe_print("="*60 + "\n")
    
    return sql

# 本地测试入口
if __name__ == "__main__":
    safe_print("=== Agent NL2SQL 服务 (调试版) ===")
    while True:
        q = input("\n请输入查询：")
        if q in ["exit", "退出"]:
            safe_print("👋 再见！")
            break
        generate_sql_service(q)