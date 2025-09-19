# -*- coding: utf-8 -*-
import os
import time
import json
import sys
import traceback
import pandas as pd
import numpy as np
from pgmpy.estimators import BaseEstimator

# Allow importing from the project root
notebook_path = os.getcwd()
project_root = os.path.abspath(os.path.join(notebook_path, ".."))
if project_root not in sys.path:
    sys.path.append(project_root)

from profiler.core import *  # Profiler, FILE, NUMERIC, CATEGORICAL, TEXT, DATE, etc.
import profiler.data.transformer as T

# ---------------------------
# Patch: robust version of normalized_sim
# ---------------------------
def _normalized_sim_patch(diff):
    if isinstance(diff, (pd.Series, pd.DataFrame)):
        np_diff = diff.values
    else:
        np_diff = np.asarray(diff)
    if np_diff.size == 0:
        return np.zeros_like(np_diff, dtype=float)
    np_diff = np_diff.astype(float)
    finite = np.isfinite(np_diff)
    if not finite.any():
        return np.zeros_like(np_diff, dtype=float)
    max_abs = np.nanmax(np.abs(np_diff[finite]))
    if max_abs <= 0:
        return np.ones_like(np_diff, dtype=float)
    sim = 1.0 - (np.abs(np_diff) / max_abs)
    sim = np.where(np.isfinite(sim), sim, 0.0)
    return sim

T.normalized_sim = _normalized_sim_patch

# ---------------------------
# Utilities: compute support / confidence
# ---------------------------
def compute_support(estimator, variable, parents, data_len=None):
    """
    estimator: BaseEstimator
    variable:  RHS attribute
    parents:   LHS attribute list
    data_len:  dataset size (optional)
    """
    state_counts = estimator.state_counts(variable, parents, reindex=False)
    counts = np.asarray(state_counts)

    sample_size = int(data_len) if data_len is not None else (
        len(estimator.data) if hasattr(estimator, 'data') else counts.sum()
    )

    part_sup = counts * (counts - 1)
    all_sup = float(np.sum(part_sup))
    denom_sup = float(sample_size * (sample_size - 1))
    support = all_sup / denom_sup if denom_sup > 0 else 0.0

    pervalue = np.sum(counts, axis=0, dtype=float)
    denom_conf = float(np.sum(pervalue * (pervalue - 1)))
    confidence = all_sup / denom_conf if denom_conf > 0 else 0.0

    return float(support), float(confidence)


def fd_scores_all_and_filtered(
    parent_sets,
    estimator,
    supp_min=0.10,
    conf_min=0.90,
    data_len=None,
    file_all_path=None,
    file_keep_path=None,
    print_all=True,
    print_keep=True,
):
    """
    Args:
      parent_sets: {rhs_attr: parents_iterable}
      estimator:   BaseEstimator
      supp_min, conf_min: thresholds for filtering
      data_len:    optional, number of rows
      file_all_path:  optional path for writing ALL FDs with metrics
      file_keep_path: optional path for writing FILTERED FDs with metrics
    Returns:
      (all_results, keep_results)
        all_results  = [(lhs_tuple, rhs_str, supp_float, conf_float), ...]
        keep_results = [(lhs_tuple, rhs_str, supp_float, conf_float), ...]
    """
    all_results = []
    for rhs, parents in parent_sets.items():
        lhs = tuple(str(p) for p in parents)
        if not lhs:
            continue
        supp, conf = compute_support(estimator, rhs, list(lhs), data_len=data_len)
        all_results.append((lhs, str(rhs), supp, conf))

    # Filter by thresholds
    keep_results = [r for r in all_results if r[2] >= supp_min and r[3] >= conf_min]

    if print_all:
        print("=== All FDs (unsorted) ===")
        for lhs, rhs, supp, conf in all_results:
            print(f"{','.join(lhs)} -> {rhs} | supp={supp:.6f}, conf={conf:.6f}")

    if print_keep:
        print("\n=== Filtered FDs (unsorted) ===")
        print(f"(thresholds: supp >= {supp_min}, conf >= {conf_min})")
        for lhs, rhs, supp, conf in keep_results:
            print(f"{','.join(lhs)} -> {rhs} | supp={supp:.6f}, conf={conf:.6f}")

    # File writer
    def _write(path, results, header):
        if not path:
            return
        dirn = os.path.dirname(path)
        if dirn:
            os.makedirs(dirn, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            if header:
                f.write(header + "\n")
            for lhs, rhs, supp, conf in results:
                f.write(f"{','.join(lhs)} -> {rhs} | supp={supp:.6f}, conf={conf:.6f}\n")

    _write(file_all_path,  all_results,  "=== All FDs (unsorted) ===")
    _write(file_keep_path, keep_results, f"=== Filtered FDs (unsorted) ===\n(thresholds: supp >= {supp_min}, conf >= {conf_min})")

    return all_results, keep_results


# ---------------------------
# Predefined schemas (use fixed types if matched; otherwise auto inference)
# ---------------------------
SCHEMAS = {
    "amazon": {
        "cols": [
            'index','Order ID','Date','Status','Fulfilment','Sales Channel ',
            'ship-service-level','Style','SKU','Category','Size','ASIN',
            'Courier Status','Qty','currency','Amount','ship-city','ship-state',
            'ship-postal-code','ship-country','promotion-ids','B2B',
            'fulfilled-by','Unnamed: 22'
        ],
        "types": [
            NUMERIC,CATEGORICAL,DATE,CATEGORICAL,CATEGORICAL,CATEGORICAL,
            CATEGORICAL,TEXT,TEXT,CATEGORICAL,TEXT,TEXT,
            CATEGORICAL,NUMERIC,CATEGORICAL,NUMERIC,TEXT,TEXT,
            NUMERIC,CATEGORICAL,TEXT,CATEGORICAL,CATEGORICAL,CATEGORICAL
        ],
        "extract": [None]*24,
    },
    "flight": {
        "cols": [
            'Year','Quarter','Month','DayofMonth','DayOfWeek','FlightDate',
            'UniqueCarrier','AirlineID','Carrier','TailNum','FlightNum',
            'OriginAirportID','OriginAirportSeqID','OriginCityMarketID',
            'Origin','OriginCityName','OriginState','OriginStateFips',
            'OriginStateName','OriginWac'
        ],
        "types": [
            NUMERIC,NUMERIC,NUMERIC,NUMERIC,NUMERIC,TEXT,
            CATEGORICAL,CATEGORICAL,CATEGORICAL,CATEGORICAL,NUMERIC,
            CATEGORICAL,CATEGORICAL,CATEGORICAL,CATEGORICAL,TEXT,
            CATEGORICAL,CATEGORICAL,TEXT,CATEGORICAL
        ],
        "extract": [None]*20,
    },
    "student": {
        "cols": list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")[:26],
        "types": [
            CATEGORICAL,CATEGORICAL,NUMERIC,CATEGORICAL,CATEGORICAL,CATEGORICAL,
            NUMERIC,NUMERIC,CATEGORICAL,CATEGORICAL,CATEGORICAL,CATEGORICAL,
            NUMERIC,NUMERIC,NUMERIC,CATEGORICAL,CATEGORICAL,CATEGORICAL,
            CATEGORICAL,CATEGORICAL,CATEGORICAL,CATEGORICAL,CATEGORICAL,
            NUMERIC,NUMERIC,NUMERIC
        ],
        "extract": [None]*26,
    },
    "hospital": {
        "cols": ['A','H','J','G','M','Q','R','K','L','N'],
        "types": [CATEGORICAL,NUMERIC,CATEGORICAL,TEXT,TEXT,NUMERIC,NUMERIC,TEXT,TEXT,TEXT],
        "extract": [None,None,None,None,None,r'(\d+)%',r'(\d+)\spatients',None,None,None],
    },
    # More datasets can be added; unmatched ones use auto type inference.
}

def infer_schema_name(file_path: str) -> str | None:
    p = file_path.lower()
    if "amazon" in p:      return "amazon"
    if "flight_20" in p or "flights_20" in p or "flight" in p: return "flight"
    if "student" in p:     return "student"
    if "hosp" in p or "hospital" in p: return "hospital"
    return None


# ---------------------------
# Single-dataset execution
# ---------------------------
def run_profile(
    file_path: str,
    schema_name: str | None,       # None = auto infer dtypes (no change_dtypes)
    out_dir: str = "./out",
    *,
    workers: int = 2,
    tol: float = 1e-6,
    eps: float = 0.05,
    embedtxt: bool = True,
    na_values: str = "empty",
    multiplier=None,
    difference: bool = True,
    sparsity: float = 0,
    infer_order: bool = True,
    supp_min: float = 0.10,
    conf_min: float = 0.90,
    save_embedding: bool = True,
    embedding_path: str = "data/",
    print_all: bool = False,
    print_keep: bool = True,
):
    pf = Profiler(workers=workers, tol=tol, eps=eps, embedtxt=embedtxt)

    pf.session.load_data(
        name=schema_name or "auto",
        src=FILE,
        fpath=file_path,
        check_param=True,
        na_values=na_values
    )

    # Only enforce dtypes when a schema is provided; otherwise, rely on auto inference
    if schema_name and schema_name in SCHEMAS:
        schema = SCHEMAS[schema_name]
        pf.session.change_dtypes(schema["cols"], schema["types"], schema["extract"])
    else:
        print(f"[INFO] No schema provided (or not found). Using automatic type inference: {file_path}")

    # Embeddings
    pf.session.load_embedding(save=save_embedding, path=embedding_path, load=not save_embedding)

    # Train data & learn structure
    t0 = time.time()
    pf.session.load_training_data(multiplier=multiplier, difference=difference)
    pf.session.learn_structure(sparsity=sparsity, infer_order=infer_order)
    parent_sets = pf.session.get_dependencies(score="fit_error")
    elapsed = time.time() - t0

    # Scoring
    data = pd.read_csv(file_path)
    estimator = BaseEstimator(data)

    os.makedirs(out_dir, exist_ok=True)
    base = os.path.splitext(os.path.basename(file_path))[0]
    tag   = f"s{supp_min:.2f}_c{conf_min:.2f}".replace(".", "p")
    file_all  = os.path.join(out_dir, f"{base}_{tag}_all.txt")
    file_keep = os.path.join(out_dir, f"{base}_{tag}_filtered.txt")

    all_results, keep_results = fd_scores_all_and_filtered(
        parent_sets,
        estimator,
        supp_min=supp_min,
        conf_min=conf_min,
        data_len=len(data),
        file_all_path=file_all,
        file_keep_path=file_keep,
        print_all=print_all,
        print_keep=print_keep
    )

    summary = {
        "file_path": file_path,
        "schema_name": schema_name or "auto",
        "out_dir": out_dir,
        "num_candidates": len(all_results),
        "num_kept": len(keep_results),
        "elapsed_seconds": elapsed,
        "output_all": file_all,
        "output_filtered": file_keep,
        "supp_min": supp_min,
        "conf_min": conf_min,
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return summary, all_results, keep_results


# ---------------------------
# Path bases (Windows absolute -> relative to CWD)
# ---------------------------

# Dataset base (if you still need it elsewhere)
WIN_DATA_BASE_ABS = r"D:\CodeWork\python\data_dependency\code-for-paper\datasets"
DATA_BASE_REL = os.path.relpath(WIN_DATA_BASE_ABS, start=os.getcwd())

def data_path(*parts: str) -> str:
    """Build a path under the dataset base, using a path relative to CWD."""
    return os.path.normpath(os.path.join(DATA_BASE_REL, *parts))

# Noise data base (new requirement)
WIN_NOISE_BASE_ABS = r"D:\CodeWork\python\data_dependency\code-for-paper\exps\robustness\noisedata"
NOISE_BASE_REL = os.path.relpath(WIN_NOISE_BASE_ABS, start=os.getcwd())

def noise_path(*parts: str) -> str:
    """Build a path under the noise-data base, using a path relative to CWD."""
    return os.path.normpath(os.path.join(NOISE_BASE_REL, *parts))


# ---------------------------
# Noise-only batch: iterate all *_noised_*.csv in noise base
# ---------------------------
def run_jobs_noise_only(out_root: str):
    # Input directory for noisy CSVs (relative to CWD)
    in_dir = noise_path()  # equals exps/robustness/noisedata relative to CWD

    def dataset_key_from_filename(fname: str) -> str:
        """Extract dataset key from filename prefix (e.g., flight_noised_0.1.csv -> flight)."""
        base = os.path.splitext(os.path.basename(fname))[0]
        parts = base.split("_")
        return parts[0].lower() if parts else base.lower()

    # Collect noisy CSV files; keep the original "statlog" filter if needed.
    # To process ALL noise CSVs, drop the 'and "statlog" in f' clause.
    all_files = [
        os.path.join(in_dir, f)
        for f in os.listdir(in_dir)
        if f.endswith(".csv") and "_noised_" in f and "statlog" in f
    ]
    all_files.sort()

    print(f"[INFO] {len(all_files)} noisy datasets to process:")
    for p in all_files:
        print("   -", os.path.basename(p))

    os.makedirs(out_root, exist_ok=True)

    summaries = []
    errors = []

    summary_path = os.path.join(out_root, "summary_noise_jobs.jsonl")
    with open(summary_path, "w", encoding="utf-8") as sf:
        for path in all_files:
            sname = infer_schema_name(path)  # None -> auto
            print(f"\n=== RUN JOB (noise) ===\npath={path}\nschema={sname or 'auto'}")
            try:
                summary, *_ = run_profile(
                    file_path=path,
                    schema_name=sname,
                    out_dir=out_root,
                    print_all=False,
                    print_keep=True,
                )
                summaries.append(summary)
                sf.write(json.dumps({"ok": True, **summary}, ensure_ascii=False) + "\n")
            except Exception as e:
                err = {
                    "ok": False,
                    "file_path": path,
                    "schema_name": sname or "auto",
                    "error": repr(e),
                    "traceback": traceback.format_exc(limit=3),
                }
                errors.append(err)
                sf.write(json.dumps(err, ensure_ascii=False) + "\n")
                print(f"[ERROR] Failed to process: {path}\n{err['error']}")

    # Terminal summary
    print("\n====== SUMMARY ======")
    print(f"Total files: {len(all_files)}")
    print(f"Succeeded: {len(summaries)}, Failed: {len(errors)}")
    if summaries:
        kept_total = sum(s["num_kept"] for s in summaries)
        cand_total = sum(s["num_candidates"] for s in summaries)
        print(f"Total candidate FDs: {cand_total}, total kept FDs: {kept_total}")
        print(f"Log file: {summary_path}")
    if errors:
        print("\nExamples of failures (up to 3):")
        for e in errors[:3]:
            print(f"- {os.path.basename(e['file_path'])}: {e['error']}")

    return summaries, errors


if __name__ == "__main__":
    # Unified output directory (relative)
    OUT = os.path.join(".", "out_batch")
    run_jobs_noise_only(OUT)
