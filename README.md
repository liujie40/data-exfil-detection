# data-exfil-detection

This project is an attempt at detecting data exfiltration just before or during the exfiltration process. The model will monitor the amount data flowing into or out of a device and look to find deviations from the expected amount. Abnormal deviations can then be flagged as potential data exfiltration events to be investigated.

## Prerequisites

Netflow data should be downloaded, decompressed and stored in a GCS bucket. The data can be downloaded from https://csr.lanl.gov/data/2017/.

## Setup

#### 1. Clone this repo
```
git clone https://github.com/danielyates2/data-exfil-detection.git
```

#### 2. Setup big query datasets and table
```
bq mk --dataset --location=EU lanl_netflow
bq mk --dataset --location=EU test_data

bq mk --table --schema=./data-eng/schemas/netflow_table_schema.json lanl_netflow.netflow

bq mk --table --schema=./data-eng/schemas/netflow_table_schema.json test_data.netflow
bq mk --table --schema=./data-eng/schemas/device_level_table_schema.json test_data.device_level_data
```

#### 3. Create test data
```
cat data-eng/create_test_data.sql | bq --location=EU query \
   --use_legacy_sql=false
```

#### 4. Load netflow data into big query
```
bq --location=EU load \
   --noreplace \
   --source_format=CSV \
   data-exfil-detection:lanl_netflow.netflow \
   gs://lanl-netflow/uncompressed/netflow_day-* \
   data-eng/schemas/netflow_table_schema.json
```

#### 5. Transform the netflow data
```
cat data_eng/sql/transform.sql | bq --location=EU query \
   --use_legacy_sql=false
```

## TODO
* Finish unit tests for first stored proc
* Finish reformatting to comply with pylint