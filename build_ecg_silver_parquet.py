#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
DICOM header -> "Silver layer" Parquet builder (tag-exploded)

Output schema (per row):
  file_path, study_uid, series_uid, instance_uid, sop_class_uid, modality,
  tag, vr, vm, path, value

- Reads DICOM with stop_before_pixels=True (fast; avoids pixel data)
- Explodes all tags, including nested SQ, and multi-valued elements (VM>1)
- Writes sharded parquet files (part-00000.parquet, ...) to out_dir
"""

import os
import sys
import json
import math
import argparse
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pydicom
from pydicom.dataset import Dataset
from pydicom.dataelem import DataElement
from pydicom.tag import Tag

import pyarrow as pa
import pyarrow.parquet as pq


# ----------- config / utilities -----------

DEFAULT_SKIP_TAGS = {
    # waveform data is enormous; you usually don't want it in "header-only" silver
    "54001010",  # (5400,1010) Waveform Data
    # PixelData won't be present when stop_before_pixels=True, but keep for safety
    "7FE00010",  # (7FE0,0010) Pixel Data
}

# For safety: truncate very long string representations
MAX_VALUE_CHARS = 4096


def iter_dicom_files(root: str) -> Iterable[str]:
    """Yield .dcm files under root (recursive)."""
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            if fn.lower().endswith(".dcm"):
                yield os.path.join(dirpath, fn)


def safe_str(x: Any) -> str:
    """Convert value to a bounded string for parquet storage."""
    if x is None:
        return ""
    try:
        s = str(x)
    except Exception:
        s = repr(x)
    if len(s) > MAX_VALUE_CHARS:
        s = s[:MAX_VALUE_CHARS] + "...(truncated)"
    return s


def tag_hex(elem_tag: Tag) -> str:
    """Return tag as 8-hex uppercase, e.g. 00080016."""
    return f"{int(elem_tag):08X}"


def vm_to_str(elem: DataElement) -> str:
    """Return VM as string (pydicom may provide int or '1-n')."""
    try:
        return str(elem.VM)
    except Exception:
        return ""


def get_top_ids(ds: Dataset) -> Dict[str, str]:
    """Extract common IDs (best-effort)."""
    def g(name: str) -> str:
        return safe_str(getattr(ds, name, ""))

    out = {
        "study_uid": g("StudyInstanceUID"),
        "series_uid": g("SeriesInstanceUID"),
        "instance_uid": g("SOPInstanceUID"),
        "sop_class_uid": g("SOPClassUID"),
        "modality": g("Modality"),
    }
    return out


# ----------- core: explode dataset -----------

def explode_dataset(
    ds: Dataset,
    file_path: str,
    ids: Dict[str, str],
    skip_tags: set,
    include_waveform_data: bool,
) -> List[Dict[str, str]]:
    """
    Explode a pydicom Dataset into row dicts. Includes nested SQ elements.
    Produces (tag, vr, vm, path, value) per scalar item.
    """
    rows: List[Dict[str, str]] = []

    def walk(current: Dataset, base_path: str) -> None:
        for elem in current.iterall():
            # elem here includes nested sequence elements too; but iterall() flattens.
            # We will instead iterate only immediate elements to manage path precisely.
            pass

    def walk_immediate(current: Dataset, base_path: str) -> None:
        for elem in current:
            th = tag_hex(elem.tag)

            if (not include_waveform_data) and th == "54001010":
                continue
            if th in skip_tags:
                continue

            vr = safe_str(elem.VR)
            vm = vm_to_str(elem)

            # Sequence (SQ): recurse into items
            if vr == "SQ":
                # Some sequences can be empty
                try:
                    seq = elem.value
                except Exception:
                    seq = []
                if seq is None:
                    seq = []

                # store an optional row for the sequence container itself (often useful)
                rows.append({
                    "file_path": file_path,
                    **ids,
                    "tag": th,
                    "vr": vr,
                    "vm": vm,
                    "path": f"{base_path}{th}/",
                    "value": f"SQ[{len(seq)}]",
                })

                for i, item in enumerate(seq):
                    if isinstance(item, Dataset):
                        walk_immediate(item, f"{base_path}{th}[{i}]/")
                    else:
                        # rare: non-dataset item in SQ
                        rows.append({
                            "file_path": file_path,
                            **ids,
                            "tag": th,
                            "vr": vr,
                            "vm": vm,
                            "path": f"{base_path}{th}[{i}]/",
                            "value": safe_str(item),
                        })
                continue

            # Non-sequence: handle multi-valued
            val = elem.value

            # bytes/bytearray -> store length + first bytes hex (bounded)
            if isinstance(val, (bytes, bytearray)):
                # waveform data won't happen if excluded; still, be safe
                preview = val[:32].hex().upper()
                rows.append({
                    "file_path": file_path,
                    **ids,
                    "tag": th,
                    "vr": vr,
                    "vm": vm,
                    "path": f"{base_path}{th}[0]/",
                    "value": f"bytes(len={len(val)};hex32={preview})",
                })
                continue

            # pydicom MultiValue behaves like list/tuple
            if isinstance(val, (list, tuple)):
                for i, v in enumerate(val):
                    rows.append({
                        "file_path": file_path,
                        **ids,
                        "tag": th,
                        "vr": vr,
                        "vm": vm,
                        "path": f"{base_path}{th}[{i}]/",
                        "value": safe_str(v),
                    })
            else:
                rows.append({
                    "file_path": file_path,
                    **ids,
                    "tag": th,
                    "vr": vr,
                    "vm": vm,
                    "path": f"{base_path}{th}[0]/",
                    "value": safe_str(val),
                })

    walk_immediate(ds, base_path="")
    return rows


# ----------- parquet writing (sharded) -----------

ARROW_SCHEMA = pa.schema([
    pa.field("file_path", pa.string()),
    pa.field("study_uid", pa.string()),
    pa.field("series_uid", pa.string()),
    pa.field("instance_uid", pa.string()),
    pa.field("sop_class_uid", pa.string()),
    pa.field("modality", pa.string()),
    pa.field("tag", pa.string()),
    pa.field("vr", pa.string()),
    pa.field("vm", pa.string()),
    pa.field("path", pa.string()),
    pa.field("value", pa.string()),
])


def write_shard(out_dir: str, shard_idx: int, rows: List[Dict[str, str]]) -> str:
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"part-{shard_idx:05d}.parquet")
    table = pa.Table.from_pylist(rows, schema=ARROW_SCHEMA)
    pq.write_table(table, out_path, compression="zstd")
    return out_path


# ----------- main pipeline -----------

def build_parquet(
    dicom_root: str,
    out_dir: str,
    max_files: Optional[int],
    shard_rows: int,
    skip_waveform_data: bool,
    extra_skip_tags: List[str],
    verbose_every: int,
) -> None:
    dicom_root = os.path.abspath(dicom_root)
    out_dir = os.path.abspath(out_dir)

    skip_tags = set(DEFAULT_SKIP_TAGS)
    for t in extra_skip_tags:
        t2 = t.replace("(", "").replace(")", "").replace(",", "").replace(" ", "")
        # allow "5400,1010" or "54001010"
        if len(t2) == 8 and all(c in "0123456789ABCDEFabcdef" for c in t2):
            skip_tags.add(t2.upper())

    include_waveform_data = not skip_waveform_data

    shard_idx = 0
    buffer: List[Dict[str, str]] = []
    n_files = 0
    n_bad = 0
    n_rows = 0

    for fp in iter_dicom_files(dicom_root):
        n_files += 1
        if max_files is not None and n_files > max_files:
            break

        try:
            ds = pydicom.dcmread(fp, stop_before_pixels=True, force=True)
            ids = get_top_ids(ds)
            rows = explode_dataset(
                ds=ds,
                file_path=fp,
                ids=ids,
                skip_tags=skip_tags,
                include_waveform_data=include_waveform_data,
            )
            buffer.extend(rows)
            n_rows += len(rows)
        except Exception as e:
            n_bad += 1
            # keep going; record minimal info
            if (n_bad <= 20) or (n_bad % 1000 == 0):
                print(f"[WARN] failed: {fp} :: {e}", file=sys.stderr)

        if verbose_every > 0 and (n_files % verbose_every == 0):
            print(f"[INFO] files={n_files:,} bad={n_bad:,} rows_buffer={len(buffer):,} rows_total={n_rows:,}")

        if len(buffer) >= shard_rows:
            out_path = write_shard(out_dir, shard_idx, buffer)
            print(f"[WRITE] {out_path}  (rows={len(buffer):,})")
            shard_idx += 1
            buffer = []

    # flush remaining
    if buffer:
        out_path = write_shard(out_dir, shard_idx, buffer)
        print(f"[WRITE] {out_path}  (rows={len(buffer):,})")

    # write a tiny run summary
    summary = {
        "dicom_root": dicom_root,
        "out_dir": out_dir,
        "files_processed": n_files,
        "files_failed": n_bad,
        "rows_total": n_rows,
        "shards_written": shard_idx + (1 if buffer else 0),
        "skip_waveform_data": skip_waveform_data,
        "skip_tags": sorted(list(skip_tags)),
        "compression": "zstd",
        "schema": [f"{f.name}:{f.type}" for f in ARROW_SCHEMA],
    }
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "_build_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("[DONE]")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def parse_args():
    ap = argparse.ArgumentParser(
        description="Build DICOM header Silver Layer Parquet from a root directory of .dcm files."
    )
    ap.add_argument("--dicom_root", required=True, help="Top-level folder containing .dcm files (recursive).")
    ap.add_argument("--out_dir", required=True, help="Output directory to write parquet shards.")
    ap.add_argument("--max_files", type=int, default=None, help="Process only N files (debug). Default: all.")
    ap.add_argument("--shard_rows", type=int, default=2_000_000,
                    help="Rows per parquet shard file. Default: 2,000,000 (tune for RAM).")
    ap.add_argument("--skip_waveform_data", action="store_true",
                    help="Exclude (5400,1010) Waveform Data (recommended).")
    ap.add_argument("--extra_skip_tags", nargs="*", default=[],
                    help="Additional tags to skip (e.g., 5400100A or '5400,100A').")
    ap.add_argument("--verbose_every", type=int, default=5000,
                    help="Print progress every N files. Default: 5000.")
    return ap.parse_args()


if __name__ == "__main__":
    args = parse_args()
    build_parquet(
        dicom_root=args.dicom_root,
        out_dir=args.out_dir,
        max_files=args.max_files,
        shard_rows=args.shard_rows,
        skip_waveform_data=args.skip_waveform_data,
        extra_skip_tags=args.extra_skip_tags,
        verbose_every=args.verbose_every,
    )
