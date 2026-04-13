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
    print(f"      📋 [prompt_enhancer] 生成系统提示词...")
    time_info = get_current_time_info()
    print(f"      ⏰ [prompt_enhancer] 注入时间: {time_info['current_date']}")
    
    schema_prompt = "数据库表结构如下（仅允许查询这些表）：\n"
    for table in TABLE_RULES["create_order"]:
        rule = TABLE_RULES[table]
        fields = list(rule["field_types"].keys())
        schema_prompt += f"表名：{table} | 字段：{', '.join(fields)}\n"

    schema_prompt += f"\n【时间规则（来自datetime模块）】\n"
    schema_prompt += f"1. 当前日期：{time_info['current_date']}\n"
    schema_prompt += f"2. 标准时间格式：{time_info['sql_date_format']}\n"
    schema_prompt += f"3. 星期对应规则：{time_info['weekday_mapping']}\n"

    schema_prompt += "\n【SQL生成规则】\n"
    schema_prompt += "1. 仅生成SELECT查询语句，禁止任何增删改\n"
    schema_prompt += "2. 仅使用上述表和字段\n"
    schema_prompt += "3. 返回纯SQL，不要任何解释\n"
    schema_prompt += "4. 字段名必须严格匹配\n"
    schema_prompt += "5. 时间查询使用标准SQL日期函数"
    
    print(f"      ✅ [prompt_enhancer] 提示词生成完成，长度: {len(schema_prompt)}")
    return schema_prompt

def build_structured_enhanced_prompt(intent_result: dict) -> str:
    """
    【最强健壮性】兼容任何 entities 格式，零报错
    """
    original_query = intent_result.get("original", intent_result.get("original_query", ""))
    entities = intent_result.get("entities", [])
    is_complex = intent_result.get("is_complex", False)

    # 健壮性处理：不管 entities 是 dict 还是 list，都能正常提取
    songs = []
    authors = []
    games = []
    
    if isinstance(entities, dict):
        # 格式1：dict 格式 {"歌曲": [], "作者": [], "游戏": []}
        songs = entities.get("歌曲", entities.get("song", []))
        authors = entities.get("作者", entities.get("author", []))
        games = entities.get("游戏", entities.get("game", []))
    elif isinstance(entities, list):
        # 格式2：list 格式，直接透传
        pass

    # 【核心新增】解决空格/大小写/排除失效问题
    structured_prompt = f"""
用户查询：{original_query}
已识别结构化信息：
- 识别到的歌曲：{songs}
- 识别到的作者：{authors}
- 识别到的游戏：{games}
- 是否为复杂查询：{is_complex}

====================
【曲库核心设定】
1. 曲库中大部分曲名为英文/日文，**中文名称极高概率是别名**
2. 搜索歌曲时，**必须同时并列筛选 歌名、别名 两个字段**
3. 输入的中文歌曲名，优先按别名匹配，同时兼容歌名匹配
4. 原创/独占歌曲规则
   - 判定：歌曲的 来源 = 本家 = 某游戏 → 即为该游戏的原创/独占歌曲
   - 本质：原创和独占含义完全一致，无区别
   - 联动：原创/独占歌曲，可被其他游戏通过联动方式收录
5. 联合原创/联合独占歌曲规则
   - 判定：歌曲的 本家字段（字符串）包含 来源字段（字符串）
6. 收录定义
   - 游戏新增添加歌曲，即为该游戏对歌曲的「收录」
7. 更新定义
   - 歌曲的收录时间 = 游戏的更新时间
   - 游戏每一次收录新歌曲，都对应一次游戏更新，一次更新会收录多首歌曲

【SQL生成强制规则】
0. 【最高优先级·逻辑简化规则】
   - 绝对禁止多余嵌套！
   - 找「歌曲的作者的其他歌曲」时：
     1. 直接通过 `song_author_rel` 从歌曲找 `author_id`
     2. 绝对不要查 `author_info` 表！
     3. 不要绕弯子，逻辑越简单越好
0.1 【最高优先级·多实体同时满足】
   🔴 禁止使用 INTERSECT（运行极慢）！
   ✅ 所有「A和B」「同时包含」场景，固定写法：
   1. JOIN 关联表（song_author_rel/game_song_rel）
   2. WHERE 筛选目标名称
   3. GROUP BY 主键+展示字段
   4. HAVING COUNT(DISTINCT 关联ID) = 目标数量
   5. 禁止乱用 DISTINCT（GROUP BY 已去重）
0.2 【排除类查询规则】
   1. 「非某游戏的曲子」：
      - 先查目标作者/歌曲的 song_id
      - 再查目标游戏的 song_id
      - 用 NOT IN 排除
   2. 必须通过关联表查ID，禁止直接用名称匹配
   3. song_author_rel 只有 song_id/author_id，没有曲名/作者名！

1. 优先用 song_id / author_id 数字ID关联
2. 仅查询目标数据，禁止冗余联表
3. 严格按实体类型匹配，禁止歌曲名=作者名
4. 匹配名称时：TRIM去空格 + LOWER转小写
5. MySQL语法规范：GROUP BY 包含所有SELECT非聚合字段
6. 逻辑极简，不输出无关数据
7. 子查询可能返回多行数据，必须用 IN 而不是 =
8. 查询要求匹配「完整英文单词」时：必须用通用正则：REGEXP '(^|[^a-z])单词($|[^a-z])'


====================

请生成合规SQL。
"""
    return structured_prompt.strip()