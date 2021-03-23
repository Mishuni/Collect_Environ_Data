# Collect_Environ_Data
Collect Korean weather and air data using open API

```sh
$ docker build -t data_collector:v0.1 .
$ docker run --name collector --restart always -d data_collector:v0.1
```

## Open API
* AirKorea : https://www.data.go.kr/data/15073861/openapi.do
* Korea Open MET Data portal: https://data.kma.go.kr/api/selectApiDetail.do?pgmNo=42&openApiNo=241