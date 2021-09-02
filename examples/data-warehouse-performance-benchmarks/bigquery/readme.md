# Load Testing BigQuery

Load tests written with K6.io. There are a total of three tests. 

- Node.js app connecting to BigQuery via SDK
- Node.js app connecting to Cube Cloud
- HTTP requests hitting Cube Cloud directly

*TODO: Add link to full tutorial in blog.*

## Get Started

First add your GCP key for accessing the BigQuery API.

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/key.json"
```

I suggest adding the key to the root of the repo and running this command:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/key.json"
```

## Load Test Node.js API with BigQuery SDK

Start the Node.js app.

```bash
node bq-api.js
```

Run the k6 load test.

```bash
docker run -i --add-host=host.docker.internal:host-gateway loadimpact/k6 run --vus 30 --duration 10s - <k6-bq.js
```

My tests returned:

```
data_received..................: 26 kB 2.2 kB/s
data_sent......................: 16 kB 1.3 kB/s
http_req_blocked...............: avg=88.02µs  min=3µs      med=10.13µs  max=2.85ms   p(90)=308.29µs p(95)=526.44µs
http_req_connecting............: avg=65.69µs  min=0s       med=0s       max=2.81ms   p(90)=210.63µs p(95)=401.97µs
http_req_duration..............: avg=960.02ms min=718.96ms med=880.58ms max=1.48s    p(90)=1.44s    p(95)=1.46s   
  { expected_response:true }...: avg=960.02ms min=718.96ms med=880.58ms max=1.48s    p(90)=1.44s    p(95)=1.46s   
http_req_failed................: 0.00% ✓ 0         ✗ 174 
http_req_receiving.............: avg=134.17µs min=35.25µs  med=134.19µs max=262.33µs p(90)=193.58µs p(95)=208.91µs
http_req_sending...............: avg=43.24µs  min=11.27µs  med=41.51µs  max=136.78µs p(90)=71.46µs  p(95)=86.83µs 
http_req_tls_handshaking.......: avg=0s       min=0s       med=0s       max=0s       p(90)=0s       p(95)=0s      
http_req_waiting...............: avg=959.84ms min=718.79ms med=880.39ms max=1.48s    p(90)=1.44s    p(95)=1.46s   
http_reqs......................: 174   14.666624/s
iteration_duration.............: avg=1.96s    min=1.71s    med=1.88s    max=2.48s    p(90)=2.44s    p(95)=2.46s   
iterations.....................: 174   14.666624/s
vus............................: 24    min=24      max=30
vus_max........................: 30    min=30      max=30
```

## Load Test Node.js API with Cube

Start the Node.js app.

```bash
node cube-api.js
```

Run the k6 load test.

```bash
docker run -i --add-host=host.docker.internal:host-gateway loadimpact/k6 run --vus 30 --duration 10s - <k6-cube-local.js
```

My tests returned:

```
data_received..................: 32 kB 3.1 kB/s
data_sent......................: 19 kB 1.8 kB/s
http_req_blocked...............: avg=386.86µs min=5.66µs   med=10.64µs  max=12ms     p(90)=127.55µs p(95)=419.63µs
http_req_connecting............: avg=237.78µs min=0s       med=0s       max=11.93ms  p(90)=94.08µs  p(95)=311.99µs
http_req_duration..............: avg=482.6ms  min=428.14ms med=478.46ms max=587.81ms p(90)=516.86ms p(95)=528ms   
  { expected_response:true }...: avg=482.6ms  min=428.14ms med=478.46ms max=587.81ms p(90)=516.86ms p(95)=528ms   
http_req_failed................: 0.00% ✓ 0         ✗ 210 
http_req_receiving.............: avg=146.35µs min=72.9µs   med=139.83µs max=386.55µs p(90)=201.13µs p(95)=212.34µs
http_req_sending...............: avg=81.32µs  min=21.35µs  med=47.16µs  max=3.59ms   p(90)=77.26µs  p(95)=153.73µs
http_req_tls_handshaking.......: avg=0s       min=0s       med=0s       max=0s       p(90)=0s       p(95)=0s      
http_req_waiting...............: avg=482.37ms min=427.83ms med=478.14ms max=587.6ms  p(90)=516.62ms p(95)=527.85ms
http_reqs......................: 210   20.115602/s
iteration_duration.............: avg=1.48s    min=1.42s    med=1.47s    max=1.58s    p(90)=1.52s    p(95)=1.52s   
iterations.....................: 210   20.115602/s
vus............................: 30    min=30      max=30
vus_max........................: 30    min=30      max=30
```


## Load Test Cube Cloud

Run the k6 load test.

```bash
docker run -i --add-host=host.docker.internal:host-gateway loadimpact/k6 run --vus 30 --duration 10s - <k6-cube-cloud.js
```

My tests returned:

```
data_received..................: 1.5 MB 138 kB/s
data_sent......................: 124 kB 11 kB/s
http_req_blocked...............: avg=51.78ms  min=721ns    med=995ns    max=470.28ms p(90)=451.58ms p(95)=461.92ms
http_req_connecting............: avg=15.48ms  min=0s       med=0s       max=140.1ms  p(90)=136.41ms p(95)=137.6ms 
http_req_duration..............: avg=180.66ms min=144.24ms med=160.42ms max=312.64ms p(90)=286.72ms p(95)=300.21ms
  { expected_response:true }...: avg=180.66ms min=144.24ms med=160.42ms max=312.64ms p(90)=286.72ms p(95)=300.21ms
http_req_failed................: 0.00%  ✓ 0         ✗ 267 
http_req_receiving.............: avg=361.96µs min=191.76µs med=302.54µs max=9.27ms   p(90)=393.71µs p(95)=432.89µs
http_req_sending...............: avg=355.82µs min=187.82µs med=328.27µs max=1.47ms   p(90)=464.66µs p(95)=549.13µs
http_req_tls_handshaking.......: avg=28.21ms  min=0s       med=0s       max=260.25ms p(90)=243.01ms p(95)=252.49ms
http_req_waiting...............: avg=179.95ms min=143.66ms med=159.78ms max=311.93ms p(90)=286.04ms p(95)=299.5ms 
http_reqs......................: 267    23.954856/s
iteration_duration.............: avg=1.23s    min=1.14s    med=1.16s    max=1.77s    p(90)=1.75s    p(95)=1.75s   
iterations.....................: 267    23.954856/s
vus............................: 24     min=24      max=30
vus_max........................: 30     min=30      max=30
```


## Results

Load testing three different approaches resulted in vastly different performance.

The Node.js app using the BigQuery SDK had these request duration percentiles:
```
p(90)=1.44s
p(95)=1.46s
```

The Node.js app using Cube Cloud to access BigQuery had these request duration percentiles:
```
p(90)=516.86ms
p(95)=528ms
```

Load testing the Cube Cloud API directly resulted in these request duration percentiles:
```
p(90)=286.72ms
p(95)=300.21ms
```

This shows a significant improvement in performance when using Cube with pre-aggregations and caching.
