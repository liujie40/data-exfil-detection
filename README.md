# data-exfil-detection

This project is an attempt at detecting data exfiltration just before or during the exfiltration process. The model will monitor the amount data flowing into or out of a device and look to find deviations from the expected amount.

## Prerequisites

Netflow data should be downloaded, decompressed and stored in a GCS bucket. The data can be downloaded from https://csr.lanl.gov/data/2017/.

## Setup

#### 1. Clone this repo
```
git clone https://github.com/danielyates2/data-exfil-detection.git
```

#### 2. Setup big query datasets and table
```
bq mk --dataset lanl_netflow --location=us-central1
bq mk --dataset test_data --location=us-central1

bq mk --table lanl_netflow.netflow --schema=./data-eng/netflow_table_schema.json
bq mk --table test_data.netflow --schema=./data-eng/netflow_table_schema.json
```

#### 3. Create test data
```
./data-eng/create_test_data.sql
```

## TODO
* setup CI pipeline using github actions
