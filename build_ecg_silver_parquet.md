pip install pydicom pyarrow
python build_ecg_silver_parquet.py \
  --dicom_root /mnt/ebs/mimic-iv-ecg-dcm/files \
  --out_dir /mnt/ebs/mimic-iv-ecg-parquet/silver \
  --skip_waveform_data \
  --shard_rows 2000000
