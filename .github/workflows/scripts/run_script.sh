#!/bin/bash
set -e

# -------- Config --------
DOWNLOAD_DIR="./downloaded_logs"
OUTPUT_DIR="./converted_txts"
PYTHON_SCRIPT="./scripts/convert_parquet_to_txt.py"
S3_LIST_FILE="./scripts/s3_files.txt"

mkdir -p "$DOWNLOAD_DIR"
mkdir -p "$OUTPUT_DIR"

# -------- Check S3 list file --------
if [[ ! -f "$S3_LIST_FILE" ]]; then
    echo "❌ S3 list file '$S3_LIST_FILE' not found!"
    exit 1
fi

# -------- Process Each S3 File --------
while IFS= read -r s3_path; do
    [[ -z "$s3_path" || "$s3_path" =~ ^# ]] && continue

    echo "📥 Downloading: $s3_path"
    filename=$(basename "$s3_path")
    local_file="$DOWNLOAD_DIR/$filename"

    aws s3 cp "$s3_path" "$local_file"

    echo "🔄 Converting: $local_file"
    python3 "$PYTHON_SCRIPT" "$local_file" "$OUTPUT_DIR"

done < "$S3_LIST_FILE"

echo "✅ All files downloaded and processed."
