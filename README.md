# data-exfil-detection

This project is an attempt at detecting data exfiltration just before or during the exfiltration process. The model will monitor the amount data flowing into or out of a device and look to find deviations from the expected amount.

## Prerequisites

Netflow data should be downloaded and stored in bigquery. The data can be downloaded from https://csr.lanl.gov/data/2017/.

## Setup

1. Clone this repo
2. Run the setup.sql script to get the netflow data on a device level

## TODO
* setup CI pipeline using github actions
