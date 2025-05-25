import argparse
import pandas as pd
import os
from glob import glob
from elasticsearch import Elasticsearch, helpers

# Hardcoded values
PARQUET_DIR = "/data/Parquet"
INDEX_NAME = "your_index_name"

# Set up argument parser
parser = argparse.ArgumentParser(description="Load all Parquet files in a fixed directory to a fixed Elasticsearch index.")
parser.add_argument('--password', required=True, help="Password for Elasticsearch")
args = parser.parse_args()

# Connect to Elasticsearch
es = Elasticsearch(
    "https://localhost:9200",
    http_auth=("elastic", args.password),
    verify_certs=False
)

# Find all .parquet files in the directory
parquet_files = glob(os.path.join(PARQUET_DIR, "*.parquet"))

if not parquet_files:
    print("No parquet files found.")
    exit(1)

# Process each parquet file
for parquet_file in parquet_files:
    print(f"Processing: {parquet_file}")
    df = pd.read_parquet(parquet_file)

    actions = [
        {
            "_index": INDEX_NAME,
            "_source": row.to_dict()
        }
        for _, row in df.iterrows()
    ]

    helpers.bulk(es, actions)

print("All data loaded successfully!")
