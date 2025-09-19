# batch_noise_injection.py
import re, shlex, random
import numpy as np
import pandas as pd
from typing import List, Dict, Set

# ============ configs ============
datasets = {
     "amazon": {
        "csv": r"datasets/Amazon-sale-report-clean.csv",
        "fds": r"bench_results/DFD_amazon_classic.txt",
     },
}

base_outdir = r""
noise_rates = [0.01, 0.1, 0.20, 0.30]
seed = 42
# =======================================
def norm(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower())

def load_schema_columns(path: str) -> List[str]:
    return pd.read_csv(path, nrows=0, dtype=object).columns.tolist()

def build_norm_map(cols: List[str]) -> Dict[str, str]:
    return {norm(c): c for c in cols}

def max_token_len(cols: List[str]) -> int:
    return max((len(norm(c).split()) for c in cols), default=1)

def rough_split(side: str) -> List[str]:
    s = side.strip()
    if not s: return []
    if '"' in s or "'" in s:
        return [t for t in shlex.split(s) if t.strip()]
    if any(d in s for d in [",",";","\t"]) or re.search(r"\s{2,}", s):
        return [p.strip() for p in re.split(r"[;,]|\t|\s{2,}", s) if p.strip()]
    return s.split()

def greedy_match(chunks: List[str], schema_map: Dict[str, str], max_words: int) -> List[str]:
    words: List[str] = []
    for ch in chunks: words += [w for w in ch.split() if w]
    i, matched = 0, []
    while i < len(words):
        best, step = None, 1
        for k in range(1, max_words+1):
            j = min(len(words), i+k)
            cand = " ".join(words[i:j])
            n = norm(cand)
            if n in schema_map:
                best, step = schema_map[n], j-i
        if best: matched.append(best); i += step
        else: i += 1
    return matched

def parse_fd_attrs_with_schema(fd_lines: List[str], schema_cols: List[str]) -> List[str]:
    schema_map = build_norm_map(schema_cols)
    max_words = max_token_len(schema_cols)
    seen: Set[str] = set()
    attrs: List[str] = []
    for raw in fd_lines:
        line = raw.strip()
        if not line or line.startswith("#"): continue
        parts = re.split(r"\s*->\s*", line)
        if len(parts) != 2: continue
        lhs = greedy_match(rough_split(parts[0]), schema_map, max_words)
        rhs = greedy_match(rough_split(parts[1]), schema_map, max_words)
        for a in lhs + rhs:
            if a not in seen:
                seen.add(a); attrs.append(a)
    return attrs

def gen_oob_numeric(series: pd.Series, rng: random.Random) -> float:
    vs = pd.to_numeric(series, errors="coerce").dropna()
    if vs.empty: return 9.99e15
    vmin, vmax = float(vs.min()), float(vs.max())
    span = max(10.0, abs(vmax - vmin) * 0.5 + 10.0)
    return (vmax + 1.0 + rng.random() * span) if rng.random() < 0.5 else (vmin - 1.0 - rng.random() * span)

def gen_oob_text(colname: str, existing: Set[str], rng: random.Random) -> str:
    for _ in range(1000):
        cand = f"__NOISE__{colname}__{rng.randrange(10**12)}"
        if cand not in existing: return cand
    return f"__NOISE__{colname}__FALLBACK__{rng.randrange(10**12)}"

def is_mostly_numeric(series: pd.Series, threshold: float = 0.9) -> bool:
    vals = [v for v in series.dropna().tolist()]
    if not vals: return False
    def _floatable(x):
        try: float(x); return True
        except: return False
    return sum(_floatable(v) for v in vals) / len(vals) >= threshold


import math, os
import pandas as pd
import numpy as np
import random

def inject_noise(csv_path, fds_path, out_csv, out_log, gamma, seed=42):
    rng = random.Random(seed)
    np.random.seed(seed)

    schema_cols = load_schema_columns(csv_path)
    with open(fds_path, "r", encoding="utf-8") as f:
        fd_lines = f.readlines()

    # Select attributes that appear in the FD list and also exist in the schema header
    all_fd_attrs = [c for c in parse_fd_attrs_with_schema(fd_lines, schema_cols) if c in schema_cols]
    if not all_fd_attrs:
        print(f"[WARN] {csv_path} 的 FD 未与表头匹配到任何列，跳过。")
        return

    # Choose target columns: 50% of matched FD attributes (at least 1)
    k = max(1, math.ceil(len(all_fd_attrs) * 0.5))
    rng.shuffle(all_fd_attrs)
    target_attrs = sorted(all_fd_attrs[:k])

    # Read data as object to handle both strings and numbers
    df = pd.read_csv(csv_path, dtype=object)
    rows, _ = df.shape

    # Candidate cells = non-null cells in target columns
    candidates = []
    for c in target_attrs:
        idxs = df.index[df[c].notna()].tolist()
        candidates.extend((i, c) for i in idxs)

    total_candidates = len(candidates)
    if total_candidates == 0:
        print(f"[WARN] {csv_path} 目标列中无可翻转的非空单元格，跳过。")
        return

    # Number of cells to flip ≈ gamma * total_candidates
    n_flip = max(0, min(total_candidates, int(round(gamma * total_candidates))))
    rng.shuffle(candidates)
    to_flip = candidates[:n_flip]

    # ===== Build a fixed “out-of-domain” noise pool for each column (size=20) =====  # >>> CHANGED
    NOISE_POOL_SIZE = 20
    noise_pool: dict[str, list[str]] = {}
    for c in target_attrs:
        col = df[c]
        existing = set(col.dropna().astype(str).unique().tolist())
        pool = []

        # Decide if this column is mostly numeric
        numeric_like = is_mostly_numeric(col, threshold=0.9)

        # Generate until the pool reaches 20 unique out-of-domain values
        tries = 0
        while len(pool) < NOISE_POOL_SIZE and tries < NOISE_POOL_SIZE * 200:
            tries += 1
            if numeric_like:
                # Create an out-of-bound numeric (write back as string)
                val = gen_oob_numeric(col, rng)
                sval = str(val)
            else:
                # Create a novel text token
                sval = gen_oob_text(c, existing | set(pool), rng)
            if sval not in existing and sval not in pool:
                pool.append(sval)

        # Fallback: ensure at least one token
        if not pool:
            pool = [gen_oob_text(c, existing, rng)]

        noise_pool[c] = pool

    # ===== Apply replacements using the fixed noise pool (per column) =====  # >>> CHANGED
    logs = []
    effective_flips = 0
    for (row_i, col) in to_flip:
        old_val = df.at[row_i, col]
        old_str = None if pd.isna(old_val) else str(old_val)
        pool = noise_pool[col]

        # Pick from the column's pool, try not to equal the old value
        if len(pool) == 1:
            new_val = pool[0]
        else:
            choices = [v for v in pool if v != old_str] or pool
            new_val = rng.choice(choices)

        if new_val != old_str:
            df.at[row_i, col] = new_val
            effective_flips += 1

        logs.append({
            "row_idx0": row_i,
            "row_idx1": row_i + 1,
            "column": col,
            "old_value": old_val,
            "new_value": new_val
        })

    # Save outputs
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    os.makedirs(os.path.dirname(out_log), exist_ok=True)
    df.to_csv(out_csv, index=False)
    pd.DataFrame(logs).to_csv(out_log, index=False)

    affected_rows = len({row for (row, _) in to_flip})

    print(f"[DONE] {csv_path}")
    print(f"  FD列总数: {len(all_fd_attrs)} | 选中目标列: {len(target_attrs)} -> {target_attrs}")
    print(f"  候选cell(目标列非空): {total_candidates} | gamma={gamma:.3f} | 计划翻转: {n_flip}")
    print(f"  实际成功替换(与原值不同): {effective_flips} | 受影响行数: {affected_rows}")
    print(f"  模式: 域外固定池替换(out-of-domain, pool_size={NOISE_POOL_SIZE})")  # >>> CHANGED


# Batch runner
if __name__ == "__main__":
    os.makedirs(base_outdir, exist_ok=True)

    for name, paths in datasets.items():
        for rate in noise_rates:
            # Either {rate:.2f} or raw {rate} is fine
            out_csv = os.path.join(base_outdir, f"{name}_noised_{rate}.csv")
            out_log = os.path.join(base_outdir, f"{name}_log_{rate}.csv")
            inject_noise(paths["csv"], paths["fds"], out_csv, out_log, rate, seed)
