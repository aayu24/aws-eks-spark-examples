CREATE TABLE IF NOT EXISTS demo.logs (
    date STRING,
    timestamp TIMESTAMP,
    datetime STRING,
    ip STRING,
    referrer STRING,
    request STRING,
    method STRING,
    endpoint STRING,
    protocol STRING,
    size STRING,
    status STRING,
    useragent STRING,
    device STRING
) USING ICEBERG
LOCATION 'warehouse/demo/logs';