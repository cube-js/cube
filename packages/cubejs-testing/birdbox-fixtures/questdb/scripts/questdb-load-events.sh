#!/bin/bash
exec 2>&1
set -x
set -e

apt-get update
apt-get install curl -y

echo $(pwd)
echo $(ls /data)

curl -F schema='[
{"name":"id", "type": "LONG"},
{"name":"type", "type": "SYMBOL"},
{"name":"actor", "type": "STRING"},
{"name":"public", "type": "SYMBOL"},
{"name":"created_at", "type": "TIMESTAMP", "pattern": "yyyy-MM-dd HH:mm:ss"},
{"name":"payload", "type": "STRING"}
]' -F data=@/data/github-events-2015-01-01.1000.csv 'http://localhost:9000/imp?timestamp=created_at&name=events&partitionBy=DAY'
