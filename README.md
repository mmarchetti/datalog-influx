# datalog-influx
Push FRC WPILib datalogs into InfluxDB 2.

## Installation

Install `influxdb-client`: `pip install influxdb-client`.

## Setup

Create `config.ini` file in the current directory following the [InfluxDB docs](https://github.com/influxdata/influxdb-client-python#via-file). Include a `datalog-influx` section defining your bucket:
```
[influx2]
url=http://localhost:8086
org=my-org
token=my-token

[datalog-influx]
bucket=my-bucket
```

## Usage

Run main.py and pass in your datalog file:
```
python main.py RC_20221101_003004.wpilog
```
