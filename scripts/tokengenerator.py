import re
import uuid
import csv
from pathlib import Path
from collections import defaultdict
from tqdm import tqdm

# ===================== 路径配置 =====================
SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
DATA_CSV_DIR = PROJECT_DIR / "data_csv"

INPUT_CSV = DATA_CSV_DIR / "songraw_info.csv"
OUTPUT_TOKEN_CSV = DATA_CSV_DIR / "songtoken.csv"
OUTPUT_FUZZY_SUMMARY = DATA_CSV_DIR / "fuzzy_match_pairs.csv"

# ===================== 【修复】标准化函数（强化版，无例外） =====================
def normalize_string(s: str) -> str:
    """
    强制标准化：所有字符串必须执行，无例外
    1. 移除所有空格
    2. 统一小写
    3. 全角转半角（所有符号/字母）
    """
    if not s or not isinstance(s, str):
        return ""
    
    # 1. 移除所有空白字符（空格、制表符等）
    s = re.sub(r'\s+', '', s)
    # 2. 强制小写
    s = s.lower()
    # 3. 全角转半角（核心修复：全覆盖）
    normalized = []
    for c in s:
        code = ord(c)
        # 全角空格 → 半角
        if code == 0x3000:
            normalized.append(' ')
        # 全角字符(！-～) → 半角
        elif 0xFF01 <= code <= 0xFF5E:
            normalized.append(chr(code - 0xFEE0))
        else:
            normalized.append(c)
    final = ''.join(normalized)
    # 最后再清一次空格
    return final.replace(' ', '')

# ===================== 【修复】编辑距离=1（严格增减1字符） =====================
def is_edit_distance_one(s1: str, s2: str) -> bool:
    if s1 == s2:
        return False
    len1, len2 = len(s1), len(s2)
    if abs(len1 - len2) > 1:
        return False
    # 保证s1为长字符串
    if len1 < len2:
        s1, s2 = s2, s1
    # 单次删除匹配
    for i in range(len(s1)):
        if s1[:i] + s1[i+1:] == s2:
            return True
    return False

# ===================== 并查集（无改动） =====================
class UnionFind:
    def __init__(self):
        self.parent = {}
    def find(self, x):
        if self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
        return self.parent[x]
    def union(self, x, y):
        if x not in self.parent:
            self.parent[x] = x
        if y not in self.parent:
            self.parent[y] = y
        rx, ry = self.find(x), self.find(y)
        if rx != ry:
            self.parent[ry] = rx

# ===================== 【核心重构】正确的模糊匹配器 =====================
class FuzzyMatcher:
    def __init__(self):
        self.items = []  # (标准化字符串, song_id, 原始值)
        self.uf = UnionFind()
        self.token_map = {}
        self.fuzzy_pairs = []
        # 【关键新增】标准化字符串 -> 绑定的song_id列表（完全相同强制合并）
        self.norm_to_ids = defaultdict(list)

    def add_item(self, original: str, song_id: str):
        if not original or not song_id:
            return
        # 【修复】强制标准化，所有字符串无例外
        norm = normalize_string(original)
        if not norm:
            return
        self.items.append((norm, song_id, original))
        # 【核心修复】记录：相同标准化字符串 → 绑定所有song_id
        self.norm_to_ids[norm].append(song_id)

    def build_groups(self):
        """
        正确执行顺序（最高优先级→最低）：
        1. 标准化完全相同 → 强制合并（必同Token）
        2. 长度≤2 → 禁止模糊匹配
        3. 编辑距离=1 → 模糊合并
        """
        # ========== 步骤1：完全相同标准化字符串 → 强制合并（修复核心BUG） ==========
        for norm_str, sid_list in self.norm_to_ids.items():
            if len(sid_list) < 2:
                continue
            # 同组所有ID合并
            main_sid = sid_list[0]
            for sid in sid_list[1:]:
                self.uf.union(main_sid, sid)

        # ========== 步骤2：分桶 + 模糊匹配（仅长度>2） ==========
        len_buckets = defaultdict(list)
        for idx, (norm, sid, orig) in enumerate(self.items):
            len_buckets[len(norm)].append((idx, norm, sid, orig))

        total = len(self.items)
        with tqdm(total=total, desc="模糊匹配进度", unit="条") as pbar:
            for i in range(total):
                norm1, id1, orig1 = self.items[i]
                self.uf.union(id1, id1)
                cur_len = len(norm1)

                # 【规则】长度≤2 → 禁止模糊匹配
                if cur_len <= 2:
                    pbar.update(1)
                    continue

                # 仅比对长度±1的字符串
                for target_len in [cur_len, cur_len - 1]:
                    if target_len not in len_buckets:
                        continue
                    for j, norm2, id2, orig2 in len_buckets[target_len]:
                        if j <= i:
                            continue
                        if len(norm2) <= 2:
                            continue

                        # 【核心】仅编辑距离=1 判定模糊匹配
                        if is_edit_distance_one(norm1, norm2):
                            self.uf.union(id1, id2)
                            self.fuzzy_pairs.append({
                                "id1": id1, "内容1": orig1,
                                "id2": id2, "内容2": orig2
                            })
                pbar.update(1)

    def get_token(self, song_id: str) -> str:
        if song_id not in self.uf.parent:
            return ""
        root = self.uf.find(song_id)
        if root not in self.token_map:
            self.token_map[root] = uuid.uuid4().hex[:8]
        return self.token_map[root]

# ===================== 作者解析（无改动） =====================
def parse_author_list(real_author_str: str) -> list[str]:
    if not real_author_str:
        return []
    s = real_author_str.strip()
    if not (s.startswith("[") and s.endswith("]")):
        return [s] if s else []
    try:
        content = s[1:-1].replace("\\'", "'")
        authors = [a.strip("' ") for a in content.split(",") if a.strip("' ")]
        return authors
    except:
        return [s]

def get_final_authors(row: dict) -> list[str]:
    real = row.get("真实作者", "").strip()
    if real:
        return parse_author_list(real)
    author = row.get("作者", "").strip()
    return [author] if author else []

# ===================== 主流程（无改动） =====================
def main():
    print("=" * 60)
    print("🎵 【最终修复版】Token生成器（标准化完全匹配+模糊匹配）")
    print("=" * 60)

    # 1. 读取数据
    print(f"\n1. 读取数据：{INPUT_CSV.name}")
    if not INPUT_CSV.exists():
        print("❌ 文件不存在")
        return
    with open(INPUT_CSV, "r", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    print(f"✅ 读取完成：{len(rows)} 条记录")

    # 2. 初始化匹配器
    song_matcher = FuzzyMatcher()
    author_matcher = FuzzyMatcher()
    token_output = []

    # 3. 加载数据
    print("\n2. 加载数据中...")
    for row in rows:
        sid = row.get("song_id", "").strip()
        if not sid:
            continue
        # 歌名
        song_matcher.add_item(row.get("歌名", ""), sid)
        # 作者
        for a in get_final_authors(row):
            author_matcher.add_item(a, sid)

    # 4. 执行匹配（核心修复）
    print("\n3. 执行匹配（完全相同优先 → 模糊匹配）...")
    song_matcher.build_groups()
    author_matcher.build_groups()

    # 5. 生成Token
    print("\n4. 生成Token中...")
    for row in tqdm(rows, desc="生成Token进度", unit="条"):
        sid = row.get("song_id", "").strip()
        if not sid:
            continue
        song_token = song_matcher.get_token(sid)
        author_tokens = [author_matcher.get_token(sid) for _ in get_final_authors(row)]
        token_output.append({
            "song_id": sid,
            "歌名token": song_token,
            "作者token": str(author_tokens)
        })

    # 6. 生成模糊匹配表
    all_pairs = []
    for p in song_matcher.fuzzy_pairs:
        p["匹配类型"] = "歌名"
        p["token"] = song_matcher.get_token(p["id1"])
        all_pairs.append(p)
    for p in author_matcher.fuzzy_pairs:
        p["匹配类型"] = "作者"
        p["token"] = author_matcher.get_token(p["id1"])
        all_pairs.append(p)

    # 7. 保存文件
    DATA_CSV_DIR.mkdir(exist_ok=True)
    # 保存Token表
    token_headers = ["song_id", "歌名token", "作者token"]
    with open(OUTPUT_TOKEN_CSV, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=token_headers)
        writer.writeheader()
        writer.writerows(token_output)
    # 保存模糊匹配对
    fuzzy_headers = ["匹配类型", "token", "id1", "内容1", "id2", "内容2"]
    with open(OUTPUT_FUZZY_SUMMARY, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fuzzy_headers)
        writer.writeheader()
        writer.writerows(all_pairs)

    print(f"\n🎉 全部完成！")
    print(f"✅ 歌曲Token表：{OUTPUT_TOKEN_CSV.name}")
    print(f"✅ 模糊匹配记录表：{OUTPUT_FUZZY_SUMMARY.name}（{len(all_pairs)} 条）")
    print("=" * 60)

if __name__ == "__main__":
    main()