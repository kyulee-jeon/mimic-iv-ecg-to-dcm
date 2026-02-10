# -*- coding: utf-8 -*-
"""
Batch convert WFDB ECG (MIMIC-IV-ECG) -> DICOM ECG Waveform Storage
Features:
- resume (skip already-successful rows)
- ProcessPoolExecutor parallel
- hard per-task timeout (kill stuck conversions safely)
- quick DICOM integrity validation
- df_info indexed by study_id for speed
- checkpoint writes

Usage (Windows example):
python run_convert.py ^
  --input_csv "C:\Projects\ecg_data\mimic-iv-ecg\study_ids.csv" ^
  --wfdb_dir  "C:\Projects\ecg_data\mimic-iv-ecg\wfdb" ^
  --dcm_dir   "C:\Projects\ecg_data\mimic-iv-ecg\dicom_out" ^
  --df_info_pkl "C:\Projects\ecg_data\mimic-iv-ecg\sample\machine_measurements_info.pkl" ^
  --output_csv "C:\Projects\ecg_data\mimic-iv-ecg\study_ids.with_dcm.csv" ^
  --study_id_col "study_id" ^
  --workers 12 ^
  --timeout 60 ^
  --checkpoint 2000
"""

from __future__ import annotations

import os
import sys
import time
import argparse
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp

import pandas as pd
import pydicom
from tqdm import tqdm
from Transform_WFDB_to_DICOM import wfdb_to_dicom_ecg_phase1


# ============================================================
# Globals per worker process
# ============================================================
_G = {
    "df_info_idx": None,     # pd.DataFrame indexed by normalized study_id
    "wfdb_dir": None,
    "dcm_dir": None,
    "overwrite": False,
}


def _normalize_study_id(x) -> str:
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s


def _init_worker(df_info_pkl: str, wfdb_dir: str, dcm_dir: str, overwrite: bool):
    """Initializer runs once per worker process."""
    global _G
    df_info = pd.read_pickle(df_info_pkl)

    # study_id normalize + index
    if "study_id" not in df_info.columns:
        raise KeyError("df_info pickle must contain 'study_id' column")

    df_info = df_info.copy()
    df_info["_sid_norm"] = df_info["study_id"].map(_normalize_study_id)
    df_info = df_info.set_index("_sid_norm", drop=False)

    _G["df_info_idx"] = df_info
    _G["wfdb_dir"] = str(Path(wfdb_dir))
    _G["dcm_dir"] = str(Path(dcm_dir))
    _G["overwrite"] = bool(overwrite)


# ============================================================
# Quick integrity validation
# ============================================================
def validate_dicom_ecg_quick(dcm_path: str) -> tuple[bool, str]:
    """
    Checks:
    - WaveformSequence exists
    - NumberOfWaveformChannels/Samples/SamplingFrequency valid
    - WaveformData length matches n_samp*n_ch*2 (SS + 16-bit)
    """
    try:
        ds = pydicom.dcmread(dcm_path, force=True)

        if "WaveformSequence" not in ds or not ds.WaveformSequence:
            return False, "No WaveformSequence"

        wf = ds.WaveformSequence[0]
        n_ch = int(getattr(wf, "NumberOfWaveformChannels", 0))
        n_samp = int(getattr(wf, "NumberOfWaveformSamples", 0))
        fs = float(getattr(wf, "SamplingFrequency", 0.0))

        if n_ch <= 0 or n_samp <= 0 or fs <= 0:
            return False, f"Invalid n_ch/n_samp/fs: {n_ch}/{n_samp}/{fs}"

        interp = str(getattr(wf, "WaveformSampleInterpretation", ""))
        bits = int(getattr(wf, "WaveformBitsAllocated", 0))

        if interp != "SS":
            return False, f"Unexpected interpretation: {interp}"
        if bits != 16:
            return False, f"Unexpected bits allocated: {bits}"

        if "WaveformData" not in wf:
            return False, "No WaveformData"

        expected_len = n_ch * n_samp * 2
        if len(wf.WaveformData) != expected_len:
            return False, f"WaveformData length mismatch: {len(wf.WaveformData)} != {expected_len}"

        return True, "OK"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


# ============================================================
# Hard-timeout execution helper (per study_id)
# - Runs conversion in a child process, join(timeout), terminate if exceeded.
# - This prevents stuck conversions from clogging the pool.
# ============================================================
def _convert_target(q: mp.Queue, study_id: str, wfdb_path_no_ext: str, dcm_path: str, df_info_row_dict: dict):
    """
    This function runs in a *child process* spawned by the worker process.
    """
    try:
        df_info_one = pd.DataFrame([df_info_row_dict])

        saved = wfdb_to_dicom_ecg_phase1(
            sample_path_no_ext=wfdb_path_no_ext,
            out_dcm_path=dcm_path,
            df_info=df_info_one,
        )
        ok, msg = validate_dicom_ecg_quick(saved)
        if not ok:
            q.put((study_id, None, f"ValidationFailed: {msg}"))
        else:
            q.put((study_id, saved, None))
    except Exception as e:
        q.put((study_id, None, f"{type(e).__name__}: {e}"))


def _convert_one_with_timeout(study_id: str, task_timeout_sec: int) -> dict:
    """
    Worker entry:
    - Resolve paths
    - Fast skip if already good
    - Look up df_info row
    - Run hard-timeout conversion (child process)
    """
    global _G

    sid = _normalize_study_id(study_id)
    wfdb_path_no_ext = os.path.join(_G["wfdb_dir"], sid)
    dcm_path = os.path.join(_G["dcm_dir"], f"{sid}.dcm")

    # overwrite=False and already exists & valid => success (skip)
    if (not _G["overwrite"]) and Path(dcm_path).exists():
        ok, _ = validate_dicom_ecg_quick(dcm_path)
        if ok:
            return {"study_id": sid, "dcm_path": dcm_path, "dcm_error": None}

    # df_info row lookup (O(1))
    df_info_idx = _G["df_info_idx"]
    if sid not in df_info_idx.index:
        return {"study_id": sid, "dcm_path": None, "dcm_error": "Missing df_info row for study_id"}

    row_dict = df_info_idx.loc[sid].to_dict()

    # Hard timeout child process
    q: mp.Queue = mp.Queue()
    p = mp.Process(
        target=_convert_target,
        args=(q, sid, wfdb_path_no_ext, dcm_path, row_dict),
        daemon=True,
    )
    p.start()
    p.join(timeout=task_timeout_sec)

    if p.is_alive():
        p.terminate()
        p.join(timeout=5)
        return {"study_id": sid, "dcm_path": None, "dcm_error": f"Timeout>{task_timeout_sec}s"}

    # Collect result
    if not q.empty():
        sid2, out_path, err = q.get()
        return {"study_id": sid2, "dcm_path": out_path, "dcm_error": err}

    return {"study_id": sid, "dcm_path": None, "dcm_error": "No result returned (unexpected)"}


# ============================================================
# Resume logic
# ============================================================
def _row_is_success(row, dcm_path_col: str) -> bool:
    p = row.get(dcm_path_col, None)
    if p is None or pd.isna(p):
        return False
    p = str(p)
    if not p:
        return False
    if not Path(p).exists():
        return False
    ok, _ = validate_dicom_ecg_quick(p)
    return ok


# ============================================================
# Main batch runner
# ============================================================
def run_batch(
    input_csv: str,
    output_csv: str,
    wfdb_dir: str,
    dcm_dir: str,
    df_info_pkl: str,
    study_id_col: str,
    dcm_path_col: str,
    error_col: str,
    workers: int,
    task_timeout_sec: int,
    checkpoint_every: int,
    overwrite: bool,
    error_log: str,
):
    Path(dcm_dir).mkdir(parents=True, exist_ok=True)

    # Resume:
    if Path(output_csv).exists():
        df = pd.read_csv(output_csv)
    else:
        df = pd.read_csv(input_csv)

    if study_id_col not in df.columns:
        raise KeyError(f"'{study_id_col}' column not found in CSV")

    if dcm_path_col not in df.columns:
        df[dcm_path_col] = pd.NA
    if error_col not in df.columns:
        df[error_col] = pd.NA

    # Determine what to process: only not-success
    mask_success = df.apply(lambda r: _row_is_success(r, dcm_path_col), axis=1)
    todo_ids = df.loc[~mask_success, study_id_col].tolist()

    with open(error_log, "a", encoding="utf-8") as f:
        f.write(f"\n=== RUN {pd.Timestamp.now()} | todo={len(todo_ids)} ===\n")

    if len(todo_ids) == 0:
        print("All rows already successful. Nothing to do.")
        df.to_csv(output_csv, index=False)
        return

    # Parallel executor
    results_buffer = []
    done = 0

    with ProcessPoolExecutor(
        max_workers=workers,
        initializer=_init_worker,
        initargs=(df_info_pkl, wfdb_dir, dcm_dir, overwrite),
    ) as ex:
        futs = {ex.submit(_convert_one_with_timeout, sid, task_timeout_sec): sid for sid in todo_ids}

        for fut in tqdm(as_completed(futs), total=len(futs), desc="Converting"):
            sid = futs[fut]
            try:
                res = fut.result()
            except Exception as e:
                res = {"study_id": _normalize_study_id(sid), "dcm_path": None, "dcm_error": f"WorkerCrash: {type(e).__name__}: {e}"}

            results_buffer.append(res)
            done += 1

            if checkpoint_every > 0 and (done % checkpoint_every == 0):
                _apply_results(df, results_buffer, study_id_col, dcm_path_col, error_col, error_log)
                results_buffer.clear()
                df.to_csv(output_csv, index=False)

    # final flush
    if results_buffer:
        _apply_results(df, results_buffer, study_id_col, dcm_path_col, error_col, error_log)
        results_buffer.clear()

    df.to_csv(output_csv, index=False)


def _apply_results(df: pd.DataFrame, results: list[dict], study_id_col: str, dcm_path_col: str, error_col: str, error_log: str):
    if not results:
        return
    r = pd.DataFrame(results)
    r["_sid_norm"] = r["study_id"].map(_normalize_study_id)

    df["_sid_norm"] = df[study_id_col].astype(str).map(_normalize_study_id)

    r = r.set_index("_sid_norm")

    # Update rows
    for i, sid_norm in enumerate(df["_sid_norm"].tolist()):
        if sid_norm in r.index:
            df.at[df.index[i], dcm_path_col] = r.at[sid_norm, "dcm_path"]
            df.at[df.index[i], error_col] = r.at[sid_norm, "dcm_error"]

    # Log errors
    err = r[r["dcm_error"].notna() & (r["dcm_error"].astype(str) != "None")]
    if not err.empty:
        with open(error_log, "a", encoding="utf-8") as f:
            for sid_norm, row in err.iterrows():
                f.write(f"{row['study_id']}\t{row['dcm_error']}\n")

    df.drop(columns=["_sid_norm"], inplace=True, errors="ignore")


# ============================================================
# CLI
# ============================================================
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input_csv", required=True)
    ap.add_argument("--output_csv", required=True)
    ap.add_argument("--wfdb_dir", required=True)
    ap.add_argument("--dcm_dir", required=True)
    ap.add_argument("--df_info_pkl", required=True)

    ap.add_argument("--study_id_col", default="study_id")
    ap.add_argument("--dcm_path_col", default="dcm_path")
    ap.add_argument("--error_col", default="dcm_error")

    ap.add_argument("--workers", type=int, default=max(1, os.cpu_count() or 4))
    ap.add_argument("--timeout", type=int, default=60, help="Per-study hard timeout in seconds")
    ap.add_argument("--checkpoint", type=int, default=2000)
    ap.add_argument("--overwrite", action="store_true")

    ap.add_argument("--error_log", default=None)
    return ap.parse_args()


def main():
    args = parse_args()

    error_log = args.error_log
    if error_log is None:
        error_log = str(Path(args.output_csv).with_suffix(".errors.log"))

    run_batch(
        input_csv=args.input_csv,
        output_csv=args.output_csv,
        wfdb_dir=args.wfdb_dir,
        dcm_dir=args.dcm_dir,
        df_info_pkl=args.df_info_pkl,
        study_id_col=args.study_id_col,
        dcm_path_col=args.dcm_path_col,
        error_col=args.error_col,
        workers=args.workers,
        task_timeout_sec=args.timeout,
        checkpoint_every=args.checkpoint,
        overwrite=args.overwrite,
        error_log=error_log,
    )
    print("DONE. Output:", args.output_csv)
    print("Error log:", error_log)


if __name__ == "__main__":
    mp.freeze_support()
    main()
