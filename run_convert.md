# Batch WFDB-to-DICOM Conversion Tool

`run_convert.py` is a batch processing tool for converting large numbers of WFDB ECG files (MIMIC-IV-ECG format) to DICOM 12-lead ECG Waveform Storage format. It provides parallel processing, resume capability, timeout protection, and comprehensive error handling.

## Features

* **Parallel Processing**: Uses `ProcessPoolExecutor` for efficient multi-core utilization
* **Resume Capability**: Automatically skips already-successful conversions when resuming
* **Hard Timeout Protection**: Prevents stuck conversions from blocking the pipeline
* **Checkpoint Writes**: Periodic saves to prevent data loss during long-running conversions
* **DICOM Integrity Validation**: Quick validation after each conversion to ensure output quality
* **Error Logging**: Comprehensive error tracking in a separate log file
* **Indexed Lookup**: Fast O(1) lookup of metadata by study_id

## Prerequisites

* Python 3.7+
* Required packages:
  ```bash
  pip install pandas pydicom wfdb tqdm
  ```
* Preprocessed `machine_measurements` DataFrame saved as a pickle file (`.pkl`)

## Input Format

### Input CSV

The input CSV file must contain at least two columns:

* `study_id`: Unique identifier for each ECG study
* `path`: Relative path to the WFDB file (relative to `wfdb_dir`)

Example:
```csv
study_id,path
12345678,p1000/p10000032/s40689238/40689238
12345679,p1000/p10000032/s44458630/44458630
```

The `path` column can include or omit file extensions (`.hea`, `.dat`). The script will automatically handle both cases.

### Metadata Pickle File

The `df_info_pkl` file should contain a preprocessed pandas DataFrame with the following columns:

* `study_id`: Must match the study_id in the input CSV
* `subject_id`: MIMIC subject identifier
* `cart_id`: ECG acquisition cart identifier
* `ecg_time`: ECG acquisition timestamp
* Additional columns as needed by `wfdb_to_dicom_ecg_phase1`

The DataFrame is indexed by normalized `study_id` for fast O(1) lookup during conversion.

## Output Format

### Output CSV

The output CSV contains all columns from the input CSV plus:

* `dcm_path`: Full path to the generated DICOM file (or `NA` if conversion failed)
* `dcm_error`: Error message if conversion failed (or `NA` if successful)

Example:
```csv
study_id,path,dcm_path,dcm_error
12345678,p1000/p10000032/s40689238/40689238,/path/to/dicom_out/p1000/p10000032/s40689238/40689238.dcm,
12345679,p1000/p10000032/s44458630/44458630,,Timeout>60s
```

### Error Log

Errors are logged to a separate file (default: `{output_csv}.errors.log`) with the format:
```
study_id<TAB>path<TAB>error_message
```

## Usage

### Basic Usage (Linux/macOS)

```bash
python run_convert.py \
  --input_csv "/path/to/study_ids.csv" \
  --wfdb_dir  "/path/to/wfdb" \
  --dcm_dir   "/path/to/dicom_out" \
  --df_info_pkl "/path/to/machine_measurements_info.pkl" \
  --output_csv "/path/to/study_ids.with_dcm.csv" \
  --study_id_col "study_id" \
  --path_col "path" \
  --workers 12 \
  --timeout 60 \
  --checkpoint 2000
```

### Basic Usage (Windows)

```cmd
python run_convert.py ^
  --input_csv "C:\Projects\ecg_data\mimic-iv-ecg\study_ids.csv" ^
  --wfdb_dir  "C:\Projects\ecg_data\mimic-iv-ecg\wfdb" ^
  --dcm_dir   "C:\Projects\ecg_data\mimic-iv-ecg\dicom_out" ^
  --df_info_pkl "C:\Projects\ecg_data\mimic-iv-ecg\sample\machine_measurements_info.pkl" ^
  --output_csv "C:\Projects\ecg_data\mimic-iv-ecg\study_ids.with_dcm.csv" ^
  --study_id_col "study_id" ^
  --path_col "path" ^
  --workers 12 ^
  --timeout 60 ^
  --checkpoint 2000
```

## Command-Line Arguments

### Required Arguments

| Argument | Description |
|----------|-------------|
| `--input_csv` | Path to input CSV file with `study_id` and `path` columns |
| `--output_csv` | Path to output CSV file (will be created or updated) |
| `--wfdb_dir` | Root directory containing WFDB files |
| `--dcm_dir` | Output directory for DICOM files |
| `--df_info_pkl` | Path to pickle file containing preprocessed `machine_measurements` DataFrame |

### Optional Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--study_id_col` | `study_id` | Column name for study ID in input CSV |
| `--path_col` | `path` | Column name for relative path in input CSV |
| `--dcm_path_col` | `dcm_path` | Column name for DICOM path in output CSV |
| `--error_col` | `dcm_error` | Column name for error message in output CSV |
| `--workers` | CPU count | Number of parallel worker processes |
| `--timeout` | `60` | Hard timeout per study in seconds |
| `--checkpoint` | `2000` | Save checkpoint every N completed studies (0 to disable) |
| `--overwrite` | `False` | Overwrite existing DICOM files |
| `--error_log` | `{output_csv}.errors.log` | Path to error log file |

## How It Works

### 1. Resume Logic

When `output_csv` already exists, the script:
1. Reads the existing output CSV
2. Validates each existing DICOM file
3. Skips rows with valid, existing DICOM files
4. Processes only rows that failed or haven't been converted yet

This allows safe interruption and resumption of long-running conversions.

### 2. Parallel Processing

The script uses `ProcessPoolExecutor` to process multiple studies concurrently:
- Each worker process loads the `df_info` DataFrame once at initialization
- Workers share no state, ensuring thread-safety
- Results are collected asynchronously using `as_completed`

### 3. Timeout Protection

Each conversion runs in a separate child process with a hard timeout:
- If a conversion exceeds the timeout, the process is terminated
- The error is logged as `Timeout>{timeout}s`
- This prevents stuck conversions from blocking the entire pipeline

### 4. Checkpointing

Periodically (every `--checkpoint` studies), the script:
1. Applies accumulated results to the DataFrame
2. Writes the updated CSV to disk
3. Clears the results buffer

This ensures progress is saved even if the script is interrupted.

### 5. DICOM Validation

After each conversion, the script performs a quick integrity check:
- Verifies `WaveformSequence` exists
- Validates `NumberOfWaveformChannels`, `NumberOfWaveformSamples`, `SamplingFrequency`
- Checks `WaveformData` length matches expected size (n_channels × n_samples × 2 bytes)
- Verifies data type is `SS` (Signed Short, 16-bit)

If validation fails, the error is logged and the row is marked as failed.

## Error Handling

### Common Error Types

* **`Timeout>{N}s`**: Conversion exceeded the timeout limit
* **`Missing df_info row for study_id`**: No matching metadata found for the study_id
* **`ValidationFailed: {message}`**: DICOM file was created but failed validation
* **`WorkerCrash: {exception}`**: Unexpected error in the worker process
* **`{ExceptionType}: {message}`**: Other exceptions during conversion

### Error Log Format

Errors are logged to the error log file with tab-separated values:
```
study_id<TAB>path<TAB>error_message
```

Example:
```
12345678	p1000/p10000032/s40689238/40689238	Timeout>60s
12345679	p1000/p10000032/s44458630/44458630	Missing df_info row for study_id
```

## Performance Considerations

### Worker Count

* Default: Number of CPU cores
* Recommendation: Start with CPU count, adjust based on I/O vs CPU-bound workload
* Too many workers may cause memory pressure or I/O contention

### Checkpoint Frequency

* Default: Every 2000 studies
* Lower values: More frequent saves, safer but slower
* Higher values: Less overhead, but more work lost on interruption
* Set to `0` to disable checkpointing (not recommended for large batches)

### Timeout Setting

* Default: 60 seconds per study
* Adjust based on typical conversion time
* Too low: May kill legitimate conversions
* Too high: Stuck conversions take longer to detect

## Example Workflow

1. **Prepare input CSV**:
   ```python
   import pandas as pd
   df = pd.DataFrame({
       'study_id': [12345678, 12345679, ...],
       'path': ['p1000/p10000032/s40689238/40689238', ...]
   })
   df.to_csv('study_ids.csv', index=False)
   ```

2. **Prepare metadata pickle**:
   ```python
   df_info = pd.read_csv('machine_measurements.csv')
   # Preprocess df_info as needed
   df_info.to_pickle('machine_measurements_info.pkl')
   ```

3. **Run conversion**:
   ```bash
   python run_convert.py \
     --input_csv study_ids.csv \
     --wfdb_dir /data/wfdb \
     --dcm_dir /data/dicom_out \
     --df_info_pkl machine_measurements_info.pkl \
     --output_csv study_ids.with_dcm.csv \
     --workers 8 \
     --checkpoint 1000
   ```

4. **Monitor progress**: The script displays a progress bar showing conversion status.

5. **Resume if interrupted**: Simply run the same command again; it will automatically resume from where it left off.

6. **Check results**:
   ```python
   df_result = pd.read_csv('study_ids.with_dcm.csv')
   print(f"Successful: {df_result['dcm_error'].isna().sum()}")
   print(f"Failed: {df_result['dcm_error'].notna().sum()}")
   ```

## Troubleshooting

### All conversions timeout

* Check if WFDB files are accessible
* Verify `wfdb_dir` path is correct
* Increase `--timeout` value
* Check system resources (CPU, memory, disk I/O)

### High error rate

* Check error log for patterns
* Verify `df_info_pkl` contains all required study_ids
* Ensure WFDB files are not corrupted
* Check disk space availability

### Slow performance

* Increase `--workers` if CPU-bound
* Decrease `--checkpoint` frequency if I/O-bound
* Check disk I/O performance
* Consider using faster storage (SSD)

### Memory issues

* Decrease `--workers` count
* Ensure sufficient system memory
* Check for memory leaks in conversion function

## Related Documentation

* See `Transform_WFDB_to_DICOM.md` for details on the conversion function
* See `Transform_WFDB_to_DICOM.py` for the implementation of `wfdb_to_dicom_ecg_phase1`
