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
    if not s or not isinstance(s, str):
        return ""
    s = re.sub(r'\s+', '', s)
    s = s.lower()
    normalized = []
    for c in s:
        code = ord(c)
        if code == 0x3000:
            normalized.append(' ')
        elif 0xFF01 <= code <= 0xFF5E:
            normalized.append(chr(code - 0xFEE0))
        else:
            normalized.append(c)
    final = ''.join(normalized)
    return final.replace(' ', '')

# ===================== 【最终规则】纯替换1次 或 纯删除/插入≤2次（禁止组合） =====================
def is_edit_distance_one(s1: str, s2: str) -> bool:
    if s1 == s2:
        return False
    len1, len2 = len(s1), len(s2)
    if abs(len1 - len2) > 2:
        return False

    i = j = replace = 0
    while i < len1 and j < len2:
        if s1[i] != s2[j]:
            replace += 1
            if replace > 1:
                return False
            i += 1
            j += 1
        else:
            i += 1
            j += 1

    delete_insert = (len1 - i) + (len2 - j)
    return (replace == 1 and delete_insert == 0) or (replace == 0 and delete_insert <= 2)

# ===================== 并查集（无改动） =====================
class UnionFind:
    def __init__(self):
        self.parent = {}
    def find(self, x):
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]
    def union(self, x, y):
        if x not in self.parent:
            self.parent[x] = x
        if y not in self.parent:
            self.parent[y] = y
        rx, ry = self.find(x), self.find(y)
        if rx != ry:
            self.parent[ry] = rx

# ===================== 【保留】原歌名匹配器（按song_id合并） =====================
class FuzzyMatcher:
    def __init__(self):
        self.items = []
        self.uf = UnionFind()
        self.token_map = {}
        self.fuzzy_pairs = []
        self.norm_to_ids = defaultdict(list)

    def add_item(self, original: str, song_id: str):
        if not original or not song_id:
            return
        norm = normalize_string(original)
        if not norm:
            return
        self.items.append((norm, song_id, original))
        self.norm_to_ids[norm].append(song_id)

    def build_groups(self):
        for norm_str, sid_list in self.norm_to_ids.items():
            if len(sid_list) < 2:
                continue
            main_sid = sid_list[0]
            for sid in sid_list[1:]:
                self.uf.union(main_sid, sid)

        len_buckets = defaultdict(list)
        for idx, (norm, sid, orig) in enumerate(self.items):
            len_buckets[len(norm)].append((idx, norm, sid, orig))

        total = len(self.items)
        with tqdm(total=total, desc="歌名模糊匹配", unit="条") as pbar:
            for i in range(total):
                norm1, id1, orig1 = self.items[i]
                self.uf.union(id1, id1)
                cur_len = len(norm1)

                if cur_len <= 2:
                    pbar.update(1)
                    continue

                for target_len in [cur_len, cur_len - 1]:
                    if target_len not in len_buckets:
                        continue
                    for j, norm2, id2, orig2 in len_buckets[target_len]:
                        if j <= i:
                            continue
                        if len(norm2) <= 2:
                            continue
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

# ===================== 【新增】专门的作者匹配器（按作者名分组） =====================
class AuthorMatcher:
    def __init__(self):
        self.author_items = []
        self.norm_to_original = {}
        self.uf = UnionFind()
        self.token_map = {}
        self.fuzzy_pairs = []
        self.norm_map = defaultdict(list)

    def add_author(self, original: str):
        if not original:
            return
        norm = normalize_string(original)
        if not norm:
            return
        self.author_items.append((norm, original))
        self.norm_map[norm].append(original)
        if norm not in self.norm_to_original:
            self.norm_to_original[norm] = original

    def build_groups(self):
        for norm_str, orig_list in self.norm_map.items():
            if len(orig_list) < 2:
                continue
            main_orig = orig_list[0]
            for orig in orig_list[1:]:
                self.uf.union(main_orig, orig)

        len_buckets = defaultdict(list)
        for idx, (norm, orig) in enumerate(self.author_items):
            len_buckets[len(norm)].append((idx, norm, orig))

        total = len(self.author_items)
        for i in range(total):
            norm1, orig1 = self.author_items[i]
            self.uf.union(orig1, orig1)
            cur_len = len(norm1)

            if cur_len <= 2:
                continue

            for target_len in [cur_len, cur_len - 1]:
                if target_len not in len_buckets:
                    continue
                for j, norm2, orig2 in len_buckets[target_len]:
                    if j <= i:
                        continue
                    if len(norm2) <= 2:
                        continue
                    if is_edit_distance_one(norm1, norm2):
                        self.uf.union(orig1, orig2)
                        self.fuzzy_pairs.append({
                            "内容1": orig1, "内容2": orig2
                        })

    def get_token(self, original: str) -> str:
        if not original:
            return ""
        if original not in self.uf.parent:
            return ""
        root = self.uf.find(original)
        if root not in self.token_map:
            self.token_map[root] = uuid.uuid4().hex[:8]
        return self.token_map[root]

# ===================== 作者解析（保留原样） =====================
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
    print("🎵 【修复作者多Token】Token生成器")
    print("=" * 60)

    print(f"\n1. 读取数据：{INPUT_CSV.name}")
    if not INPUT_CSV.exists():
        print("❌ 文件不存在")
        return
    with open(INPUT_CSV, "r", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    print(f"✅ 读取完成：{len(rows)} 条记录")

    song_matcher = FuzzyMatcher()
    author_matcher = AuthorMatcher()
    token_output = []

    print("\n2. 加载数据中...")
    all_authors_set = set()
    for row in rows:
        sid = row.get("song_id", "").strip()
        if not sid:
            continue
        song_matcher.add_item(row.get("歌名", ""), sid)
        for a in get_final_authors(row):
            all_authors_set.add(a)

    for a in all_authors_set:
        author_matcher.add_author(a)

    print("\n3. 执行匹配...")
    song_matcher.build_groups()
    author_matcher.build_groups()

    print("\n4. 生成Token中...")
    for row in tqdm(rows, desc="生成Token进度", unit="条"):
        sid = row.get("song_id", "").strip()
        if not sid:
            continue
        song_token = song_matcher.get_token(sid)
        
        author_list = get_final_authors(row)
        author_tokens = [author_matcher.get_token(a) for a in author_list]
        
        token_output.append({
            "song_id": sid,
            "歌名token": song_token,
            "作者token": str(author_tokens)
        })

    all_pairs = []
    for p in song_matcher.fuzzy_pairs:
        p["匹配类型"] = "歌名"
        p["token"] = song_matcher.get_token(p["id1"])
        all_pairs.append(p)
    for p in author_matcher.fuzzy_pairs:
        p["匹配类型"] = "作者"
        p["token"] = author_matcher.get_token(p["内容1"])
        p["id1"] = ""
        p["id2"] = ""
        all_pairs.append(p)

    DATA_CSV_DIR.mkdir(exist_ok=True)
    token_headers = ["song_id", "歌名token", "作者token"]
    with open(OUTPUT_TOKEN_CSV, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=token_headers)
        writer.writeheader()
        writer.writerows(token_output)
    
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