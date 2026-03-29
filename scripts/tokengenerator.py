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

# ===================== 标准化函数 =====================
def normalize_string(s: str) -> str:
    if not s:
        return ""
    s = s.replace(" ", "").lower()
    normalized = []
    for c in s:
        code = ord(c)
        if 0xFF01 <= code <= 0xFF5E:
            normalized.append(chr(code - 0xFEE0))
        else:
            normalized.append(c)
    return "".join(normalized)

# ===================== 核心：编辑距离=1 =====================
def is_edit_distance_one(s1: str, s2: str) -> bool:
    len1, len2 = len(s1), len(s2)
    if abs(len1 - len2) > 1:
        return False
    if len1 < len2:
        s1, s2 = s2, s1
    for i in range(len(s1)):
        if s1[:i] + s1[i+1:] == s2:
            return True
    return False

# ===================== 轻量并查集 =====================
class UnionFind:
    def __init__(self):
        self.parent = {}
    def find(self, x):
        if self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
        return self.parent[x]
    def union(self, x, y):
        if x not in self.parent: self.parent[x] = x
        if y not in self.parent: self.parent[y] = y
        rx, ry = self.find(x), self.find(y)
        if rx != ry:
            self.parent[ry] = rx

# ===================== 极速模糊匹配器 =====================
class FuzzyMatcher:
    def __init__(self):
        self.items = []
        self.uf = UnionFind()
        self.token_map = {}
        self.fuzzy_pairs = []

    def add_item(self, original: str, song_id: str):
        if not original or not song_id:
            return
        norm = normalize_string(original)
        if len(norm) <= 2:
            norm = original
        self.items.append((norm, song_id, original))

    def build_groups_fast(self):
        len_buckets = defaultdict(list)
        for idx, (norm, sid, orig) in enumerate(self.items):
            len_buckets[len(norm)].append( (idx, norm, sid, orig) )

        total = len(self.items)
        with tqdm(total=total, desc="模糊匹配进度", unit="条") as pbar:
            for i in range(total):
                norm1, id1, orig1 = self.items[i]
                self.uf.union(id1, id1)
                cur_len = len(norm1)
                
                for target_len in [cur_len, cur_len-1]:
                    if target_len not in len_buckets:
                        continue
                    for j, norm2, id2, orig2 in len_buckets[target_len]:
                        if j <= i:
                            continue
                        if len(norm1) <=2 or len(norm2) <=2:
                            continue
                        if is_edit_distance_one(norm1, norm2):
                            self.uf.union(id1, id2)
                            self.fuzzy_pairs.append({
                                "id1":id1,"内容1":orig1,"id2":id2,"内容2":orig2
                            })
                pbar.update(1)

    def get_token(self, song_id: str) -> str:
        if song_id not in self.uf.parent:
            return ""
        root = self.uf.find(song_id)
        if root not in self.token_map:
            self.token_map[root] = uuid.uuid4().hex[:8]
        return self.token_map[root]

# ===================== 作者解析 =====================
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

# ===================== 主流程 =====================
def main():
    print("=" * 60)
    print("🎵 极速版模糊Token生成器（修复报错版）")
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
        song_matcher.add_item(row.get("歌名", ""), sid)
        for a in get_final_authors(row):
            author_matcher.add_item(a, sid)

    # 4. 模糊匹配
    print("\n3. 执行模糊匹配：")
    song_matcher.build_groups_fast()
    author_matcher.build_groups_fast()

    # 5. 生成Token
    print("\n4. 生成Token中...")
    for row in tqdm(rows, desc="生成Token进度", unit="条"):
        sid = row.get("song_id", "").strip()
        if not sid:
            continue
        st = song_matcher.get_token(sid)
        ats = [author_matcher.get_token(sid) for _ in get_final_authors(row)]
        token_output.append({
            "song_id": sid, 
            "歌名token": st, 
            "作者token": str(ats)
        })

    # 6. 合并匹配记录
    all_pairs = []
    for p in song_matcher.fuzzy_pairs:
        p.update({"匹配类型":"歌名","token":song_matcher.get_token(p["id1"])})
        all_pairs.append(p)
    for p in author_matcher.fuzzy_pairs:
        p.update({"匹配类型":"作者","token":author_matcher.get_token(p["id1"])})
        all_pairs.append(p)

    # 7. 保存文件（修复字段名BUG）
    DATA_CSV_DIR.mkdir(exist_ok=True)
    
    # 保存songtoken.csv（正确字段名）
    token_headers = ["song_id", "歌名token", "作者token"]
    with open(OUTPUT_TOKEN_CSV, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=token_headers)
        writer.writeheader()
        writer.writerows(token_output)
    
    # 保存模糊匹配表
    fuzzy_headers = ["匹配类型","token","id1","内容1","id2","内容2"]
    with open(OUTPUT_FUZZY_SUMMARY, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fuzzy_headers)
        writer.writeheader()
        writer.writerows(all_pairs)

    print(f"\n🎉 全部完成！")
    print(f"✅ 歌曲Token：{OUTPUT_TOKEN_CSV.name}")
    print(f"✅ 模糊匹配表：{OUTPUT_FUZZY_SUMMARY.name}（{len(all_pairs)} 条）")
    print("=" * 60)

if __name__ == "__main__":
    main()