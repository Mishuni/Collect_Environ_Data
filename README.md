# KETI Pre Data Collection

> data collection

# Collect_Environ_Data
Collect Korean weather and air data using open API

```sh
$ docker build -t data_collector:v0.1 .
$ docker run --name collector --restart always -d data_collector:v0.1
```

## Open API
* AirKorea : https://www.data.go.kr/data/15073861/openapi.do
* Korea Open MET Data portal: https://data.kma.go.kr/api/selectApiDetail.do?pgmNo=42&openApiNo=241




#### influx_setting.py Template (ignored)
```python
# ~/Collect_Environ_Data/data_collection/influxdb_management/influx_setting.py

# DB Server
host_='localhost'
port_=8086
user_='admin'
pass_='admin'
protocol ='line'

# Weather API key
weather_api_key = '....'
# Air API key
air_api_key = '....'
```

