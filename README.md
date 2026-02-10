# WFDB-to-DICOM ECG Conversion (Phase 1)

This document describes how ECG waveform data and associated metadata from **MIMIC-IV-ECG (WFDB format)** are converted into **DICOM 12-lead ECG Waveform Storage** using the function:

```python
wfdb_to_dicom_ecg_phase1(
    sample_path_no_ext: str,
    out_dcm_path: str,
    df_info: pd.DataFrame
)
```

The goal of *Phase 1* is to generate a **standards-compliant DICOM ECG waveform object** using:

* raw digital ECG samples from WFDB (`.hea` / `.dat`)
* machine- and acquisition-level metadata from `machine_measurements.csv`

No derived clinical measurements (e.g., RR interval, axes) are embedded in the DICOM at this stage; those are handled separately in the CDM layer.

---

## 1. Source Data

### 1.1 WFDB ECG Records (MIMIC-IV-ECG)

For each ECG study:

* WFDB files consist of:

  * `.hea`: header (metadata)
  * `.dat`: waveform samples
* The WFDB record is read using:

  ```python
  wfdb.rdrecord(sample_path_no_ext, physical=False)
  ```

  ensuring access to **raw digital samples (`d_signal`)** rather than scaled physical values.

Key information extracted from the WFDB header:

* Number of signals (e.g., 12)
* Number of samples per signal (e.g., 5000)
* Sampling frequency (e.g., 500 Hz)
* Signal names (I, II, III, aVR, …, V6)
* ADC gain and baseline
* Base date and base time (if available)

---

### 1.2 Machine Measurements Metadata (`df_info`)

Additional metadata are derived from **MIMIC-IV-ECG `machine_measurements.csv`** and preprocessed as follows:

```python
df_csv = pd.read_csv("machine_measurements.csv")

df_info = df_csv[
    ["subject_id", "study_id", "cart_id", "ecg_time", "bandwidth"]
]

df_info["lowpassfilter"] = (
    df_info["bandwidth"]
    .str.split("-").str[0]
    .str.replace("Hz", "").str.strip()
)

df_info["highpassfilter"] = (
    df_info["bandwidth"]
    .str.split("-").str[1]
    .str.replace("Hz", "").str.strip()
)
```

The resulting `df_info` DataFrame contains, for each `study_id`:

* `subject_id`
* `cart_id`
* `ecg_time`
* `lowpassfilter` (Hz)
* `highpassfilter` (Hz)

In the conversion function:

```python
file_info = df_info[df_info["study_id"] == study_id]
```

is used to retrieve the row corresponding to the current ECG record.

---

## 2. DICOM Object Overview

The function generates a **DICOM 12-lead ECG Waveform Storage SOP Instance**:

* **SOP Class UID**:
  `1.2.840.10008.5.1.4.1.1.9.1.1`

* **Transfer Syntax**:
  Explicit VR Little Endian

* **Waveform Data Encoding**:

  * Signed 16-bit integers (`SS`)
  * Channel-interleaved, sample-major order
  * Directly derived from WFDB `d_signal` (no rescaling applied)

---

## 3. Patient, Study, and Equipment Mapping

| Source                                        | DICOM Attribute      | Tag         | Description                |
| --------------------------------------------- | -------------------- | ----------- | -------------------------- |
| `subject_id`                                  | Patient ID           | (0010,0020) | MIMIC subject identifier   |
| `study_id` (WFDB record name)                 | Study ID             | (0020,0010) | Used for cross-table joins |
| `cart_id`                                     | Station Name         | (0008,1010) | ECG acquisition cart       |
| WFDB base date/time (preferred) or `ecg_time` | Acquisition DateTime | (0008,002A) | ECG acquisition timestamp  |

**AcquisitionDateTime logic**:

1. If `base_date` and `base_time` exist in the WFDB header, they are used.
2. Otherwise, `ecg_time` from `df_info` is used.

---

## 4. Waveform Sequence (5400,0100)

Each ECG record contains a single **Waveform Sequence Item**, representing one multiplexed ECG waveform group.

### 4.1 Waveform-Level Attributes

| DICOM Attribute             | Tag         | Value / Source             |
| --------------------------- | ----------- | -------------------------- |
| Waveform Originality        | (003A,0004) | `ORIGINAL`                 |
| Number of Waveform Channels | (003A,0005) | From `.hea` (e.g., 12)     |
| Number of Waveform Samples  | (003A,0010) | From `.hea` (e.g., 5000)   |
| Sampling Frequency          | (003A,001A) | From `.hea` (e.g., 500 Hz) |
| Multiplex Group Label       | (003A,0020) | `"ECG"`                    |

---

### 4.2 Channel Definition Sequence (003A,0200)

The **Channel Definition Sequence** contains one item per ECG lead (typically 12 items).

#### 4.2.1 Channel Label

| Attribute     | Tag         | Source                                              |
| ------------- | ----------- | --------------------------------------------------- |
| Channel Label | (003A,0203) | Signal description from `.hea` (e.g., I, II, V1, …) |

---

#### 4.2.2 Channel Source Sequence (003A,0208)

Each channel is coded using **CID 3001 (ECG Lead)** from IEEE 11073 MDC when possible.
https://dicom.nema.org/medical/dicom/current/output/chtml/part16/sect_CID_3001.html

```python
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
```

Example for Lead II:

| Attribute                | Tag         | Value     |
| ------------------------ | ----------- | --------- |
| Coding Scheme Designator | (0008,0102) | `MDC`     |
| Code Value               | (0008,0100) | `2:3`     |
| Code Meaning             | (0008,0104) | `Lead II` |

If a channel label does not correspond to a standard 12-lead ECG, a local coding scheme (`99LOCAL`) is used as a fallback.

---

#### 4.2.3 Channel Sensitivity and Units

| Attribute                          | Tag         | Value / Source            |
| ---------------------------------- | ----------- | ------------------------- |
| Channel Sensitivity                | (003A,0210) | `1 / adc_gain` (mV/count) |
| Channel Sensitivity Units Sequence | (003A,0211) | UCUM `mV`                 |

Example:

* WFDB header: `200.0(0)/mV`
* ADC gain = 200 counts/mV
  → Channel Sensitivity = **0.005 mV/count**

---

#### 4.2.4 Channel Baseline

| Attribute        | Tag         | Source                                                          |
| ---------------- | ----------- | --------------------------------------------------------------- |
| Channel Baseline | (003A,0213) | WFDB baseline value (from `( )` in `.hea` or `record.baseline`) |

---

#### 4.2.5 Bit Depth

| Attribute                      | Tag         | Value               |
| ------------------------------ | ----------- | ------------------- |
| Waveform Bits Stored           | (003A,021A) | `16`                |
| Waveform Bits Allocated        | (5400,1004) | `16`                |
| Waveform Sample Interpretation | (5400,1006) | `SS` (Signed Short) |

---

### 4.3 Waveform Data (5400,1010)

* Source: WFDB `d_signal`
* Type: raw **int16 digital samples**
* Layout:

  * Sample-major
  * Channel-interleaved
    `(s1c1, s1c2, …, s1cN, s2c1, …)`
* No additional scaling is applied during storage; physical units are defined via **Channel Sensitivity**.

---

## 5. Design Rationale

* **Transparency**: Raw ADC counts are preserved without re-quantization.
* **Standards compliance**: DICOM ECG IOD and CID 3001 are used wherever applicable.
* **Reproducibility**: Acquisition and machine parameters are explicitly encoded.
* **Separation of concerns**:

  * Signal storage → DICOM Waveform
  * Clinical measurements → OMOP CDM (`Measurement`, `Observation`)

