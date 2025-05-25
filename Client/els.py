import pandas as pd
from elasticsearch import Elasticsearch, helpers

# Connect to Elasticsearch (update with your credentials if needed)
es = Elasticsearch("https://localhost:9200",
                    # use_ssl = False,
                    http_auth=("elastic", "49EO-a9NAqytA1Mnt8gB"),
                    # compatible_with=7,
                    # ca_certs=False,
                    verify_certs=False)

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

# Bulk load data to ES
helpers.bulk(es, actions)
print("Data loaded successfully!")
