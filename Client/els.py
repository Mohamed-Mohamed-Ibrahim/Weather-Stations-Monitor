import argparse
import pandas as pd
from elasticsearch import Elasticsearch, helpers

# Set up argument parser
parser = argparse.ArgumentParser(description="Load Parquet data to Elasticsearch.")
parser.add_argument('--password', required=True, help="Password for Elasticsearch")
args = parser.parse_args()

# Connect to Elasticsearch using the password argument
es = Elasticsearch(
    "https://localhost:9200",
    http_auth=("elastic", args.password),
    verify_certs=False
)

# Read your Parquet file
df = pd.read_parquet("/mnt/01D8D4FB872972F0/Life/collage/collage_labs/year_3/term2/DDIA/project/Weather-Stations-Monitor/Backend/BaseCentralStation/Parquet/0.parquet")

# Convert DataFrame rows to ES bulk format
actions = [
    {
        "_index": "your_index_name",
        "_source": row.to_dict()
    }
    for _, row in df.iterrows()
]

# Bulk load data to Elasticsearch
helpers.bulk(es, actions)
print("Data loaded successfully!")
