# MIMIC-IV-ECG to DICOM Converter

A Python toolset for converting WFDB-format ECG waveform data from **MIMIC-IV-ECG** into **DICOM 12-lead ECG Waveform Storage** format. This project provides both single-file conversion and batch processing capabilities with parallel execution, resume support, and comprehensive error handling.

## Overview

This project converts ECG waveform data from the MIMIC-IV-ECG dataset (stored in WFDB format) to standards-compliant DICOM 12-lead ECG Waveform Storage objects. The conversion preserves raw digital samples without re-quantization and embeds acquisition metadata from the MIMIC-IV-ECG dataset.

### Key Features

* **Standards-compliant DICOM conversion**: Generates DICOM ECG Waveform Storage (SOP Class UID: `1.2.840.10008.5.1.4.1.1.9.1.1`)
* **Raw data preservation**: Maintains original ADC counts without rescaling
* **Batch processing**: Parallel conversion with `ProcessPoolExecutor`
* **Resume capability**: Automatically skips already-successful conversions
* **Timeout protection**: Prevents stuck conversions from blocking the pipeline
* **Checkpointing**: Periodic saves to prevent data loss
* **DICOM validation**: Quick integrity checks after each conversion

## Installation

### Prerequisites

* Python 3.7+
* Access to MIMIC-IV-ECG dataset (WFDB files)

### Required Packages

```bash
pip install pandas pydicom wfdb tqdm numpy
```

## Quick Start

### Single File Conversion

```python
from Transform_WFDB_to_DICOM import wfdb_to_dicom_ecg_phase1
import pandas as pd

# Prepare metadata DataFrame
df_info = pd.DataFrame([{
    'study_id': '12345678',
    'subject_id': '10000032',
    'cart_id': 'CART001',
    'ecg_time': '2020-01-01 12:00:00',
    # ... other required fields
}])

# Convert WFDB to DICOM
dcm_path = wfdb_to_dicom_ecg_phase1(
    sample_path_no_ext='/path/to/wfdb/record_name',  # without .hea/.dat extension
    out_dcm_path='/path/to/output/record_name.dcm',
    df_info=df_info
)
```

### Batch Conversion

```bash
python run_convert.py \
  --input_csv "/path/to/study_ids.csv" \
  --wfdb_dir  "/path/to/wfdb" \
  --dcm_dir   "/path/to/dicom_out" \
  --df_info_pkl "/path/to/machine_measurements_info.pkl" \
  --output_csv "/path/to/study_ids.with_dcm.csv" \
  --workers 12 \
  --timeout 60 \
  --checkpoint 2000
```

## Project Structure

```
mimic-iv-ecg-to-dcm/
├── README.md                          # This file
├── Transform_WFDB_to_DICOM.py         # Core conversion function
├── Transform_WFDB_to_DICOM.md        # Detailed conversion documentation
├── run_convert.py                     # Batch processing script
└── run_convert.md                     # Batch processing documentation
```

## Input Data Requirements

### WFDB Files

Each ECG study requires:
* `.hea` file: Header containing metadata (signal names, sampling frequency, ADC gain, etc.)
* `.dat` file: Raw waveform samples

The WFDB record is read using:
```python
wfdb.rdrecord(sample_path_no_ext, physical=False)
```

This ensures access to **raw digital samples (`d_signal`)** rather than scaled physical values.

### Metadata DataFrame

The `df_info` DataFrame must contain the following columns for each `study_id`:

* `study_id`: Unique identifier matching the WFDB record name
* `subject_id`: MIMIC subject identifier
* `cart_id`: ECG acquisition cart identifier
* `ecg_time`: ECG acquisition timestamp
* Additional fields as needed (e.g., `lowpassfilter`, `highpassfilter`)

Example preprocessing:
```python
df_csv = pd.read_csv("machine_measurements.csv")
df_info = df_csv[["subject_id", "study_id", "cart_id", "ecg_time", "bandwidth"]]
df_info["lowpassfilter"] = df_info["bandwidth"].str.split("-").str[0].str.replace("Hz", "").str.strip()
df_info["highpassfilter"] = df_info["bandwidth"].str.split("-").str[1].str.replace("Hz", "").str.strip()
df_info.to_pickle("machine_measurements_info.pkl")
```

## Output Format

### DICOM Structure

The conversion generates a **DICOM 12-lead ECG Waveform Storage SOP Instance** with:

* **SOP Class UID**: `1.2.840.10008.5.1.4.1.1.9.1.1`
* **Transfer Syntax**: Explicit VR Little Endian
* **Waveform Data**: Signed 16-bit integers (`SS`), channel-interleaved, sample-major order

### Key DICOM Attributes

| Source | DICOM Attribute | Tag | Description |
|--------|----------------|-----|-------------|
| `subject_id` | Patient ID | (0010,0020) | MIMIC subject identifier |
| `study_id` | Study ID | (0020,0010) | Used for cross-table joins |
| `cart_id` | Station Name | (0008,1010) | ECG acquisition cart |
| WFDB base date/time or `ecg_time` | Acquisition DateTime | (0008,002A) | ECG acquisition timestamp |

### Waveform Encoding

* **Data Type**: Raw int16 digital samples (no rescaling)
* **Layout**: Sample-major, channel-interleaved `(s1c1, s1c2, …, s1cN, s2c1, …)`
* **Physical Units**: Defined via Channel Sensitivity (mV/count)


## Design Principles

* **Transparency**: Raw ADC counts are preserved without re-quantization
* **Standards compliance**: DICOM ECG IOD and CID 3001 (ECG Lead) are used wherever applicable
* **Reproducibility**: Acquisition and machine parameters are explicitly encoded
* **Separation of concerns**: Signal storage → DICOM Waveform; Clinical measurements → OMOP CDM

## Documentation

For detailed information, see:

* **[Transform_WFDB_to_DICOM.md](Transform_WFDB_to_DICOM.md)**: Detailed documentation on the conversion function, DICOM structure, and mapping rules
* **[run_convert.md](run_convert.md)**: Comprehensive guide to batch processing, including troubleshooting and performance tuning

## Example Workflow

1. **Prepare metadata**:
   ```python
   import pandas as pd
   df_info = pd.read_csv("machine_measurements.csv")
   # Preprocess df_info as needed
   df_info.to_pickle("machine_measurements_info.pkl")
   ```

2. **Create input CSV**:
   ```python
   df_input = pd.DataFrame({
       'study_id': [12345678, 12345679, ...],
       'path': ['p1000/p10000032/s40689238/40689238', ...]
   })
   df_input.to_csv('study_ids.csv', index=False)
   ```

3. **Run batch conversion**:
   ```bash
   python run_convert.py \
     --input_csv study_ids.csv \
     --wfdb_dir /data/wfdb \
     --dcm_dir /data/dicom_out \
     --df_info_pkl machine_measurements_info.pkl \
     --output_csv study_ids.with_dcm.csv \
     --workers 8
   ```

4. **Check results**:
   ```python
   df_result = pd.read_csv('study_ids.with_dcm.csv')
   print(f"Successful: {df_result['dcm_error'].isna().sum()}")
   print(f"Failed: {df_result['dcm_error'].notna().sum()}")
   ```

## Troubleshooting

### Common Issues

* **All conversions timeout**: Check WFDB file accessibility, verify paths, increase `--timeout`
* **High error rate**: Check error log for patterns, verify metadata contains all study_ids
* **Memory issues**: Decrease `--workers` count
* **Slow performance**: Adjust worker count based on CPU vs I/O bound workload

See [run_convert.md](run_convert.md) for detailed troubleshooting guide.

## References

### MIMIC-IV-ECG Dataset

Gow, B., Pollard, T., Nathanson, L. A., Johnson, A., Moody, B., Fernandes, C., Greenbaum, N., Waks, J. W., Eslami, P., Carbonati, T., Chaudhari, A., Herbst, E., Moukheiber, D., Berkowitz, S., Mark, R., & Horng, S. (2023). MIMIC-IV-ECG: Diagnostic Electrocardiogram Matched Subset (version 1.0). PhysioNet. RRID:SCR_007345. https://doi.org/10.13026/4nqg-sb35

### PhysioNet Platform

Goldberger, A., Amaral, L., Glass, L., Hausdorff, J., Ivanov, P. C., Mark, R., ... & Stanley, H. E. (2000). PhysioBank, PhysioToolkit, and PhysioNet: Components of a new research resource for complex physiologic signals. Circulation [Online]. 101 (23), pp. e215–e220. RRID:SCR_007345.

## License



## Contributing


