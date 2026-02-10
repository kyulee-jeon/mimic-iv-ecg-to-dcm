# pip install wfdb pydicom numpy pandas

from __future__ import annotations
import datetime as dt
from typing import Optional, List
from pathlib import Path

import numpy as np
import pandas as pd
import wfdb
from wfdb.io.record import Record

from pydicom.dataset import Dataset, FileDataset, FileMetaDataset
from pydicom.uid import generate_uid, ExplicitVRLittleEndian


# ===============================
# Coding helpers
# ===============================

def _ucum_unit(code: str, meaning: str) -> Dataset:
    ds = Dataset()
    ds.CodingSchemeDesignator = "UCUM"
    ds.CodeValue = code
    ds.CodeMeaning = meaning
    return ds

# https://dicom.nema.org/medical/dicom/current/output/chtml/part16/sect_CID_3001.html
LEAD_MDC_CID3001 = {
    "I":   ("2:1",  "Lead I"),
    "II":  ("2:2",  "Lead II"),
    "III": ("2:61",  "Lead III"),
    "aVR": ("2:62",  "aVR, augmented voltage, right"),
    "aVL": ("2:63",  "aVL, augmented voltage, left"),
    "aVF": ("2:64",  "aVF, augmented voltage, foot"),
    "V1":  ("2:3",  "Lead V1"),
    "V2":  ("2:4",  "Lead V2"),
    "V3":  ("2:5", "Lead V3"),
    "V4":  ("2:6", "Lead V4"),
    "V5":  ("2:7", "Lead V5"),
    "V6":  ("2:8", "Lead V6"),
}


def make_channel_source_sequence(lead_label: str) -> list[Dataset]:
    src = Dataset()
    if lead_label in LEAD_MDC_CID3001:
        code_value, code_meaning = LEAD_MDC_CID3001[lead_label]
        src.CodingSchemeDesignator = "MDC"
        src.CodeValue = code_value
        src.CodeMeaning = code_meaning
    else:
        src.CodingSchemeDesignator = "99LOCAL"
        src.CodeValue = lead_label
        src.CodeMeaning = f"ECG Lead {lead_label}"
    return [src]


# ===============================
# WFDB helpers
# ===============================

def _parse_subject_id_from_comments(comments: Optional[List[str]]) -> Optional[str]:
    if not comments:
        return None
    for line in comments:
        s = line.lstrip("#").strip()
        if s.lower().startswith("<subject_id>"):
            return s.split(":", 1)[1].strip()
    return None


def _parse_wfdb_header_datetime(sample_path_no_ext: str) -> Optional[dt.datetime]:
    hea = Path(sample_path_no_ext).with_suffix(".hea")
    if not hea.exists():
        return None
    with open(hea, "r", encoding="utf-8") as f:
        first = f.readline().strip()
    parts = first.split()
    if len(parts) >= 6:
        try:
            return dt.datetime.strptime(
                f"{parts[5]} {parts[4]}",
                "%d/%m/%Y %H:%M:%S"
            )
        except ValueError:
            return None
    return None


# ===============================
# Main converter
# ===============================

def wfdb_to_dicom_ecg_phase1(
    sample_path_no_ext: str,
    out_dcm_path: str,
    df_info: pd.DataFrame,
    study_uid: Optional[str] = None,
    series_uid: Optional[str] = None,
) -> str:

    record = wfdb.rdrecord(sample_path_no_ext, physical=False)

    # ---------------------------
    # waveform
    # ---------------------------
    if record.d_signal is None:
        raise ValueError("WFDB record has no d_signal")

    sig = np.asarray(record.d_signal, dtype=np.int16)
    n_samples, n_ch = sig.shape
    waveform_bytes = sig.astype("<i2").reshape(-1, order="C").tobytes()

    # ---------------------------
    # header values
    # ---------------------------
    fs = float(record.fs)
    sig_names = record.sig_name
    adc_gain = record.adc_gain
    baseline = record.baseline
    units = record.units

    # ---------------------------
    # IDs
    # ---------------------------
    study_id = record.record_name
    subject_id = _parse_subject_id_from_comments(record.comments) or "UNKNOWN"
    
    file_info = df_info[df_info["study_id"].astype(str) == str(study_id)]
    if not file_info.empty:
        file_info = file_info.iloc[0]
    else:
        file_info = None

    # ---------------------------
    # datetime
    # ---------------------------
    acq_dt = _parse_wfdb_header_datetime(sample_path_no_ext)
    if acq_dt is None and file_info is not None:
        acq_dt = pd.to_datetime(file_info["ecg_time"]).to_pydatetime()
    if acq_dt is None:
        acq_dt = dt.datetime.now()

    # ---------------------------
    # DICOM meta
    # ---------------------------
    fm = FileMetaDataset()
    fm.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.9.1.1"
    fm.MediaStorageSOPInstanceUID = generate_uid()
    fm.TransferSyntaxUID = ExplicitVRLittleEndian

    ds = FileDataset(out_dcm_path, {}, file_meta=fm, preamble=b"\0" * 128)
    ds.is_little_endian = True
    ds.is_implicit_VR = False

    ds.SOPClassUID = fm.MediaStorageSOPClassUID
    ds.SOPInstanceUID = fm.MediaStorageSOPInstanceUID

    ds.PatientID = subject_id
    ds.StudyID = study_id

    if file_info is not None:
        ds.StationName = str(file_info["cart_id"])

    ds.StudyInstanceUID = study_uid or generate_uid()
    ds.SeriesInstanceUID = series_uid or generate_uid()

    ds.Modality = "ECG"
    ds.SeriesNumber = 1
    ds.InstanceNumber = 1

    ds.StudyDate = acq_dt.strftime("%Y%m%d")
    ds.StudyTime = acq_dt.strftime("%H%M%S")
    ds.AcquisitionDateTime = acq_dt.strftime("%Y%m%d%H%M%S")

    # ---------------------------
    # Waveform Sequence
    # ---------------------------
    wf = Dataset()
    wf.WaveformOriginality = "ORIGINAL"
    wf.NumberOfWaveformChannels = n_ch
    wf.NumberOfWaveformSamples = n_samples
    wf.SamplingFrequency = fs
    wf.MultiplexGroupLabel = "ECG"

    if file_info is not None:
        wf.FilterLowFrequency = float(file_info["lowpassfilter"])
        wf.FilterHighFrequency = float(file_info["highpassfilter"])

    wf.WaveformBitsAllocated = 16
    wf.WaveformBitsStored = 16
    wf.WaveformSampleInterpretation = "SS"
    wf.WaveformData = waveform_bytes

    # ---------------------------
    # Channel Definition Sequence
    # ---------------------------
    ch_seq = []
    for i in range(n_ch):
        ch = Dataset()
        label = sig_names[i]
        ch.ChannelLabel = label
        ch.ChannelSourceSequence = make_channel_source_sequence(label)

        gain = adc_gain[i]
        ch.ChannelSensitivity = float(1.0 / gain)
        ch.ChannelSensitivityUnitsSequence = [_ucum_unit("mV", "millivolt")]
        ch.ChannelBaseline = int(baseline[i])

        ch.WaveformBitsStored = 16
        ch_seq.append(ch)

    wf.ChannelDefinitionSequence = ch_seq
    ds.WaveformSequence = [wf]

    ds.save_as(out_dcm_path, write_like_original=False)
    return out_dcm_path
