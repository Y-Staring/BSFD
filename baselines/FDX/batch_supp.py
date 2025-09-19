# -*- coding: utf-8 -*-
import os
import time
import json
import sys
import csv
import pandas as pd
import numpy as np
from pathlib import Path
from pgmpy.estimators import BaseEstimator

# place under the tutorial directory in https://github.com/sis-ethz/Profiler-Public
notebook_path = os.getcwd()
project_root = os.path.abspath(os.path.join(notebook_path, ".."))
if project_root not in sys.path:
    sys.path.append(project_root)

from profiler.core import *
import profiler.data.transformer as T


def _normalized_sim_patch(diff):
    """Patch for normalized similarity to be robust to NaNs/Infs and zero ranges."""
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
    variable:  RHS
    parents:   LHS
    data_len:  row_count
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
    print_all=False,
    print_keep=True,
):
    """Compute support/confidence for all (lhs -> rhs) and write two files:
       - *_all.txt with metrics
       - *_filtered.txt with plain rules (kept by thresholds)
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

    # Writer utility
    def _write_rules(path, results, *, header=None, plain=True):
        """
        plain=False: write full line 'A,B -> C | supp=.., conf=..' (for all.txt)
        plain=True : only write 'A,B -> C' (for filtered.txt)
        """
        if not path:
            return
        dirn = os.path.dirname(path)
        if dirn:
            os.makedirs(dirn, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            if (header is not None) and (not plain) and header:
                f.write(header + "\n")
            for lhs, rhs, supp, conf in results:
                if plain:
                    f.write(f"{','.join(lhs)} -> {rhs}\n")
                else:
                    f.write(f"{','.join(lhs)} -> {rhs} | supp={supp:.6f}, conf={conf:.6f}\n")

    # Write all candidates (keep metrics, optionally with header)
    _write_rules(
        file_all_path,
        all_results,
        header="=== All FDs (unsorted) ===",
        plain=False
    )
    # Write filtered rules only as 'A,B -> C'
    _write_rules(
        file_keep_path,
        keep_results,
        header=None,
        plain=True
    )

    return all_results, keep_results


SCHEMAS = {
    "amazon": {
        "cols": [
            'index', 'Order ID', 'Date', 'Status', 'Fulfilment', 'Sales Channel ',
            'ship-service-level', 'Style', 'SKU', 'Category', 'Size', 'ASIN',
            'Courier Status', 'Qty', 'currency', 'Amount', 'ship-city', 'ship-state',
            'ship-postal-code', 'ship-country', 'promotion-ids', 'B2B',
            'fulfilled-by', 'Unnamed: 22'
        ],
        "types": [
            NUMERIC, CATEGORICAL, DATE, CATEGORICAL, CATEGORICAL, CATEGORICAL,
            CATEGORICAL, TEXT, TEXT, CATEGORICAL, TEXT, TEXT,
            CATEGORICAL, NUMERIC, CATEGORICAL, NUMERIC, TEXT, TEXT,
            NUMERIC, CATEGORICAL, TEXT, CATEGORICAL, CATEGORICAL, CATEGORICAL
        ],
        "extract": [None] * 24,
    },
    "flight": {
        "cols": [
            'Year', 'Quarter', 'Month', 'DayofMonth', 'DayOfWeek', 'FlightDate',
            'UniqueCarrier', 'AirlineID', 'Carrier', 'TailNum', 'FlightNum',
            'OriginAirportID', 'OriginAirportSeqID', 'OriginCityMarketID',
            'Origin', 'OriginCityName', 'OriginState', 'OriginStateFips',
            'OriginStateName', 'OriginWac'
        ],
        "types": [
            NUMERIC, NUMERIC, NUMERIC, NUMERIC, NUMERIC, TEXT,
            CATEGORICAL, CATEGORICAL, CATEGORICAL, CATEGORICAL, NUMERIC,
            CATEGORICAL, CATEGORICAL, CATEGORICAL, CATEGORICAL, TEXT,
            CATEGORICAL, CATEGORICAL, TEXT, CATEGORICAL
        ],
        "extract": [None] * 20,
    },
    "student": {
        "cols": list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")[:26],
        "types": [
            CATEGORICAL, CATEGORICAL, NUMERIC, CATEGORICAL, CATEGORICAL, CATEGORICAL,
            NUMERIC, NUMERIC, CATEGORICAL, CATEGORICAL, CATEGORICAL, CATEGORICAL,
            NUMERIC, NUMERIC, NUMERIC, CATEGORICAL, CATEGORICAL, CATEGORICAL,
            CATEGORICAL, CATEGORICAL, CATEGORICAL, CATEGORICAL, CATEGORICAL,
            NUMERIC, NUMERIC, NUMERIC
        ],
        "extract": [None] * 26,
    },
    "hospital": {
        "cols": ['A', 'H', 'J', 'G', 'M', 'Q', 'R', 'K', 'L', 'N'],
        "types": [CATEGORICAL, NUMERIC, CATEGORICAL, TEXT, TEXT, NUMERIC, NUMERIC, TEXT, TEXT, TEXT],
        "extract": [None, None, None, None, None, r'(\d+)%', r'(\d+)\spatients', None, None, None],
    },
    # Add other datasets later; default path uses automatic type inference
}


def infer_schema_name(file_path: str) -> str | None:
    p = file_path.lower()
    if "amazon" in p:
        return "amazon"
    if "flights_20" in p or "flight_20" in p or "flight" in p:
        return "flight"
    if "student" in p:
        return "student"
    if "hosp" in p or "hospital" in p:
        return "hospital"
    return None


# ---------------------------
# Single dataset run
# ---------------------------
def run_profile(
    file_path: str,
    schema_name: str | None,       # None = auto infer dtypes (no change_dtypes)
    out_dir: str,
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

    # Only override dtypes when schema is provided; otherwise rely on auto inference
    if schema_name and schema_name in SCHEMAS:
        schema = SCHEMAS[schema_name]
        pf.session.change_dtypes(schema["cols"], schema["types"], schema["extract"])
    else:
        print(f"[INFO] No schema provided (or not found), using automatic type inference: {file_path}")

    # Embedding
    pf.session.load_embedding(save=save_embedding, path=embedding_path, load=not save_embedding)

    # Training data & structure learning
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
    tag = f"s{supp_min:.2f}_c{conf_min:.2f}".replace(".", "p")
    file_all = os.path.join(out_dir, f"{base}_{tag}_all.txt")
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
        "dataset": base,
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
    # One summary.json per job
    with open(os.path.join(out_dir, f"{base}_{tag}_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    # Console brief
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return summary, all_results, keep_results


# ---------------------------
# Job generation & batch execution
# ---------------------------
def generate_jobs(paths, conf_fixed=0.90):
    """Generate [(supp, conf=0.9)] jobs for given dataset paths."""
    supp_grid = [round(x, 2) for x in np.arange(0.2, 1.0, 0.1)]
    jobs = []
    for p in paths:
        for s in supp_grid:
            jobs.append({"path": p, "supp": float(s), "conf": float(conf_fixed)})
    return jobs


def run_jobs(jobs: list[dict], out_root: str):
    """Run all jobs and write batch summaries (CSV + JSON)."""
    run_ts = time.strftime("%Y%m%d-%H%M%S")
    batch_dir = os.path.join(out_root, f"support_sweep_c0p90_{run_ts}")
    os.makedirs(batch_dir, exist_ok=True)

    # Persist config of this batch
    with open(os.path.join(batch_dir, "batch_config.json"), "w", encoding="utf-8") as f:
        json.dump(
            {
                "conf_fixed": 0.90,
                "supp_grid": [round(x, 2) for x in np.arange(0.2, 1.0, 0.1)],
                "num_jobs": len(jobs),
            },
            f,
            indent=2,
            ensure_ascii=False,
        )

    summaries = []
    for j in jobs:
        path = j["path"]
        supp = j["supp"]
        conf = j["conf"]
        sname = infer_schema_name(path)  # None -> auto

        base = os.path.splitext(os.path.basename(path))[0]
        tag = f"s{supp:.2f}_c{conf:.2f}".replace(".", "p")
        out_dir = os.path.join(batch_dir, base, tag)

        print(f"\n=== RUN JOB ===\npath={path}\nsupp={supp}, conf={conf}\nschema={sname or 'auto'}\nout_dir={out_dir}")
        summary, *_ = run_profile(
            file_path=path,
            schema_name=sname,        # None = auto
            out_dir=out_dir,
            supp_min=supp,
            conf_min=conf,
            print_all=False,
            print_keep=True,
        )
        summaries.append(summary)

    # Write CSV summary
    csv_path = os.path.join(batch_dir, "summaries.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "dataset", "file_path", "schema_name", "supp_min", "conf_min",
            "num_candidates", "num_kept", "elapsed_seconds",
            "output_all", "output_filtered", "out_dir"
        ])
        for s in summaries:
            writer.writerow([
                s["dataset"], s["file_path"], s["schema_name"], s["supp_min"], s["conf_min"],
                s["num_candidates"], s["num_kept"], f'{s["elapsed_seconds"]:.3f}',
                s["output_all"], s["output_filtered"], s["out_dir"]
            ])

    # Write JSON summary
    with open(os.path.join(batch_dir, "summaries.json"), "w", encoding="utf-8") as f:
        json.dump({"batch_dir": batch_dir, "summaries": summaries}, f, indent=2, ensure_ascii=False)

    print(f"\n=== BATCH DONE ===\nBatch dir: {batch_dir}\nCSV: {csv_path}\nTotal jobs: {len(jobs)}")
    return summaries


# ===========================
# Dataset paths (relative to the Windows base folder)
# ===========================
# Absolute Windows base; we convert it to a path *relative to current working directory*.
WIN_DATA_BASE_ABS = r"D:\CodeWork\python\data_dependency\code-for-paper\datasets"
DATA_BASE_REL = os.path.relpath(WIN_DATA_BASE_ABS, start=os.getcwd())

def data_path(filename: str) -> str:
    """Build a dataset path using the Windows base converted to a relative path."""
    return os.path.normpath(os.path.join(DATA_BASE_REL, filename))


DATASETS = [
    # data_path("adult_clean.csv"),
    # data_path("statlog_german_credit_data.csv"),
    # data_path("hospital_clean.csv"),
    data_path("flights_20_500k_clean.csv"),
    # data_path("Amazon-sale-report-clean.csv"),
    # data_path("studentfull_26.csv"),
]

JOBS = generate_jobs(DATASETS, conf_fixed=0.90)

if __name__ == "__main__":
    # Output root (relative folder under current working directory)
    OUT = os.path.join(".", "out_support_sweep")
    run_jobs(JOBS, OUT)
