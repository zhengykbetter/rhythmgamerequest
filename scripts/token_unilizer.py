import csv
import sys
from pathlib import Path
from collections import defaultdict
from typing import List, Dict, Tuple, Optional

# ===================== 配置层 =====================
class Config:
    SCRIPT_DIR = Path(__file__).parent
    PROJECT_DIR = SCRIPT_DIR.parent
    DATA_CSV_DIR = PROJECT_DIR / "data_csv"
    
    AUTHOR_TOKEN_MAP = DATA_CSV_DIR / "author_token_map.csv"
    ALIAS_TABLE = DATA_CSV_DIR / "alias_table.csv"
    UNI_TOKEN_OUTPUT = DATA_CSV_DIR / "unitoken.csv"

# ===================== 工具层 =====================
def read_csv_delimited(file_path: Path, delimiter: str = ",") -> List[Dict]:
    """通用CSV读取：支持逗号/制表符分隔"""
    if not file_path.exists():
        print(f"⚠️  警告：文件不存在 {file_path.name}，跳过该规则")
        return []
    try:
        with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            data = list(reader)
            print(f"✅ 成功读取 {file_path.name}：{len(data)} 条记录")
            return data
    except Exception as e:
        print(f"❌ 读取 {file_path.name} 失败：{str(e)}")
        return []

def write_csv(file_path: Path, data: List[Dict], headers: List[str]) -> None:
    """通用CSV写入"""
    file_path.parent.mkdir(exist_ok=True)
    with open(file_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data)

def filter_changed_only(data: List[Dict]) -> List[Dict]:
    """核心过滤：仅保留【原始Token ≠ 统一Token】的记录"""
    return [
        item for item in data
        if item.get("原始Token") and item.get("统一Token") 
        and item["原始Token"] != item["统一Token"]
    ]

def build_name_to_token_map(author_data: List[Dict]) -> Dict[str, str]:
    """构建【作者原名 → Token】快速查询字典"""
    token_map = {}
    for row in author_data:
        name = row.get("作者原名", "").strip()
        token = row.get("作者Token", "").strip()
        if name and token:
            token_map[name] = token
    print(f"✅ 作者Token字典构建完成：共 {len(token_map)} 个有效作者名")
    return token_map

# ===================== 业务解析层 =====================
def parse_author_name(full_name: str) -> Tuple[str, Optional[str]]:
    """分离基础名 + 末尾括号厂牌名"""
    full_name = full_name.strip()
    if not full_name:
        return "", None
    
    if full_name.endswith(")"):
        last_open = full_name.rfind("(")
        if last_open > 0:
            base = full_name[:last_open].strip()
            label = full_name[last_open+1 : -1].strip()
            return base, label
    return full_name, None

# ===================== 核心业务1：厂牌合并 =====================
def group_by_basename(author_data: List[Dict]) -> Dict[str, List[Dict]]:
    """按基础作者名分组"""
    groups = defaultdict(list)
    for row in author_data:
        name = row.get("作者原名", "").strip()
        token = row.get("作者Token", "").strip()
        if not name or not token:
            continue
        
        base_name, label = parse_author_name(name)
        groups[base_name].append({
            "原始作者名": name,
            "原始Token": token,
            "label": label
        })
    return groups

def generate_label_unify_map(groups: Dict[str, List[Dict]]) -> List[Dict]:
    """厂牌合并：无厂牌 → 字典序最小带厂牌作者"""
    result = []
    for base, members in groups.items():
        labeled = [m for m in members if m["label"]]
        target = sorted(labeled, key=lambda x: x["原始作者名"])[0] if labeled else members[0]
        
        for m in members:
            result.append({
                "原始作者名": m["原始作者名"],
                "原始Token": m["原始Token"],
                "统一作者名": target["原始作者名"],
                "统一Token": target["原始Token"]
            })
    return result

# ===================== 核心业务2：别名合并（分隔符已修正！） =====================
def generate_alias_unify_map(author_data: List[Dict], alias_data: List[Dict]) -> List[Dict]:
    """
    别名合并：作者名2 → 指向 → 作者名1
    示例：Tetrajectory → 1N6Fs | DJ Mashiro → 3R2
    """
    result = []
    name2token = build_name_to_token_map(author_data)

    print(f"\n🔍 开始匹配别名表（共 {len(alias_data)} 条）：")
    print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    for idx, row in enumerate(alias_data, 1):
        target_standard_name = row.get("作者名1", "").strip()
        source_alias_name = row.get("作者名2", "").strip()

        print(f"[{idx}] 处理别名：{source_alias_name} → {target_standard_name}")

        if not source_alias_name or not target_standard_name:
            print(f"   ⚠️  跳过：存在空值")
            continue

        src_token = name2token.get(source_alias_name)
        target_token = name2token.get(target_standard_name)

        if not src_token:
            print(f"   ❌ 匹配失败：【{source_alias_name}】不在作者Token表中")
            continue
        if not target_token:
            print(f"   ❌ 匹配失败：【{target_standard_name}】不在作者Token表中")
            continue

        result.append({
            "原始作者名": source_alias_name,
            "原始Token": src_token,
            "统一作者名": target_standard_name,
            "统一Token": target_token
        })
        print(f"   ✅ 匹配成功：Token {src_token} → {target_token}")

    print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f"✅ 别名合并最终生效：{len(result)} 条")
    return result

# ===================== 合并层 =====================
def merge_all_rules(label_map: List[Dict], alias_map: List[Dict]) -> List[Dict]:
    """合并规则：别名优先级 > 厂牌"""
    final_map = {item["原始作者名"]: item for item in label_map}
    override_count = 0
    
    for item in alias_map:
        final_map[item["原始作者名"]] = item
        override_count += 1

    print(f"\n⚙️  规则合并：别名覆盖 {override_count} 条厂牌合并结果")
    return list(final_map.values())

# ===================== 主流程 =====================
def main():
    print("=" * 70)
    print("🎯 作者Token统一工具（厂牌+别名合并 · 分隔符修正版）")
    print("=" * 70)

    try:
        # 1. 读取数据
        print(f"\n📥 步骤1：读取作者Token表")
        author_data = read_csv_delimited(Config.AUTHOR_TOKEN_MAP, delimiter=",")
        if not author_data:
            print("❌ 无作者数据，退出")
            return

        print(f"\n📥 步骤2：读取别名对照表")
        # 🔥 核心修正：别名表用逗号分隔读取
        alias_data = read_csv_delimited(Config.ALIAS_TABLE, delimiter=",")

        # 2. 厂牌合并
        print(f"\n🔍 步骤3：执行厂牌合并")
        groups = group_by_basename(author_data)
        label_result = generate_label_unify_map(groups)
        label_changed = filter_changed_only(label_result)

        # 3. 别名合并
        print(f"\n🔍 步骤4：执行别名合并")
        alias_result = generate_alias_unify_map(author_data, alias_data)
        alias_changed = filter_changed_only(alias_result)

        # 4. 合并规则
        print(f"\n⚙️ 步骤5：合并最终映射")
        full_result = merge_all_rules(label_result, alias_result)
        final_result = filter_changed_only(full_result)

        # 5. 最终统计
        print(f"\n📊 最终合并统计")
        print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        print(f"🏷️  厂牌合并贡献：{len(label_changed)} 条")
        print(f"🔤 别名合并贡献：{len(alias_changed)} 条")
        print(f"✅ 总有效合并记录：{len(final_result)} 条")
        print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

        # 6. 输出文件
        headers = ["原始作者名", "原始Token", "统一作者名", "统一Token"]
        write_csv(Config.UNI_TOKEN_OUTPUT, final_result, headers)

        print(f"\n🎉 处理完成！文件已保存至：{Config.UNI_TOKEN_OUTPUT.name}")

    except Exception as e:
        print(f"\n❌ 程序报错：{str(e)}")
        sys.exit(1)
    
    print("=" * 70)

if __name__ == "__main__":
    main()