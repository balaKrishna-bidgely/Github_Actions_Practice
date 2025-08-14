# convert_parquet_to_txt.py
import sys
import os
import pandas as pd

if len(sys.argv) != 3:
    print("Usage: python3 convert_parquet_to_txt.py <parquet_file> <output_dir>")
    sys.exit(1)

input_file = sys.argv[1]
output_dir = sys.argv[2]
output_file = os.path.join(output_dir, os.path.basename(input_file) + ".txt")

try:
    df = pd.read_parquet(input_file)
    df.to_csv(output_file, sep='\t', index=False)
    print(f"✅ Converted to {output_file}")
except Exception as e:
    print(f"❌ Failed to convert {input_file}: {e}")
