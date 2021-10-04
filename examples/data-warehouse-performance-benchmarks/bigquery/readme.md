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

Run the k6 load test for 200 virtual users.

```bash
k6 run k6-bq.js
```

My tests returned:

```
running (40.0s), 000/200 VUs, 591 complete and 74 interrupted iterations
default ✓ [======================================] 200 VUs  10s

     data_received..................: 90 kB 2.2 kB/s
     data_sent......................: 53 kB 1.3 kB/s
     http_req_blocked...............: avg=99.2µs  min=1.67µs   med=4.03µs  max=4.24ms   p(90)=127.24µs p(95)=594.97µs
     http_req_connecting............: avg=65.99µs min=0s       med=0s      max=2.06ms   p(90)=74.42µs  p(95)=485.42µs
     http_req_duration..............: avg=2.57s   min=805.87ms med=2.42s   max=6.36s    p(90)=3.8s     p(95)=3.98s   
       { expected_response:true }...: avg=2.57s   min=805.87ms med=2.42s   max=6.36s    p(90)=3.8s     p(95)=3.98s   
     http_req_failed................: 0.00% ✓ 0        ✗ 591  
     http_req_receiving.............: avg=91.32µs min=29.96µs  med=77.26µs max=817.65µs p(90)=156.68µs p(95)=179.56µs
     http_req_sending...............: avg=216.6µs min=8.41µs   med=22.31µs max=7.51ms   p(90)=65.16µs  p(95)=685.81µs
     http_req_tls_handshaking.......: avg=0s      min=0s       med=0s      max=0s       p(90)=0s       p(95)=0s      
     http_req_waiting...............: avg=2.57s   min=805.8ms  med=2.42s   max=6.36s    p(90)=3.8s     p(95)=3.98s   
     http_reqs......................: 591   14.77134/s
     iteration_duration.............: avg=2.57s   min=805.97ms med=2.42s   max=6.36s    p(90)=3.8s     p(95)=3.98s   
     iterations.....................: 591   14.77134/s
     vus............................: 74    min=74     max=200
     vus_max........................: 200   min=200    max=200

```

## Load Test Node.js API with Cube

Start the Node.js app.

```bash
node cube-api.js
```

Run the k6 load test.

```bash
k6 run k6-cube-local.js
```

My tests returned:

```
running (12.3s), 000/200 VUs, 1013 complete and 0 interrupted iterations
default ✓ [======================================] 200 VUs  10s

     data_received..................: 154 kB 13 kB/s
     data_sent......................: 81 kB  6.6 kB/s
     http_req_blocked...............: avg=157.08µs min=1.56µs   med=6.84µs  max=10.48ms  p(90)=393.37µs p(95)=1.02ms  
     http_req_connecting............: avg=142.47µs min=0s       med=0s      max=10.37ms  p(90)=360.18µs p(95)=983.48µs
     http_req_duration..............: avg=2.19s    min=772.42ms med=2.27s   max=3.51s    p(90)=2.37s    p(95)=2.63s   
       { expected_response:true }...: avg=2.19s    min=772.42ms med=2.27s   max=3.51s    p(90)=2.37s    p(95)=2.63s   
     http_req_failed................: 0.00%  ✓ 0         ✗ 1013 
     http_req_receiving.............: avg=120.02µs min=33.11µs  med=115.1µs max=392.55µs p(90)=194.46µs p(95)=209.77µs
     http_req_sending...............: avg=117.34µs min=8.01µs   med=40.55µs max=1.86ms   p(90)=250.06µs p(95)=528.91µs
     http_req_tls_handshaking.......: avg=0s       min=0s       med=0s      max=0s       p(90)=0s       p(95)=0s      
     http_req_waiting...............: avg=2.19s    min=772.25ms med=2.27s   max=3.51s    p(90)=2.37s    p(95)=2.63s   
     http_reqs......................: 1013   82.388192/s
     iteration_duration.............: avg=2.19s    min=775.15ms med=2.27s   max=3.51s    p(90)=2.37s    p(95)=2.63s   
     iterations.....................: 1013   82.388192/s
     vus............................: 25     min=25      max=200
     vus_max........................: 200    min=200     max=200

```


## Load Test Cube Cloud

Run the k6 load test.

```bash
k6 run k6-cube-cloud.js
```

My tests returned:

```
running (11.3s), 000/200 VUs, 1320 complete and 0 interrupted iterations
default ✓ [======================================] 200 VUs  10s

     data_received..................: 8.5 MB 751 kB/s
     data_sent......................: 631 kB 56 kB/s
     http_req_blocked...............: avg=72.78ms  min=318ns    med=1.1µs    max=663.03ms p(90)=425.83ms p(95)=517.25ms
     http_req_connecting............: avg=22.07ms  min=0s       med=0s       max=166.97ms p(90)=140.46ms p(95)=149.7ms 
     http_req_duration..............: avg=1.56s    min=574.29ms med=1.46s    max=2.85s    p(90)=2.21s    p(95)=2.33s   
       { expected_response:true }...: avg=1.56s    min=574.29ms med=1.46s    max=2.85s    p(90)=2.21s    p(95)=2.33s   
     http_req_failed................: 0.00%  ✓ 0          ✗ 1320 
     http_req_receiving.............: avg=26.6ms   min=47.55µs  med=194.95µs max=152.74ms p(90)=135.72ms p(95)=137.19ms
     http_req_sending...............: avg=274.17µs min=45.28µs  med=183.92µs max=14.65ms  p(90)=295.29µs p(95)=345.87µs
     http_req_tls_handshaking.......: avg=48.89ms  min=0s       med=0s       max=496.98ms p(90)=272.02ms p(95)=347.98ms
     http_req_waiting...............: avg=1.54s    min=573.83ms med=1.43s    max=2.71s    p(90)=2.19s    p(95)=2.32s   
     http_reqs......................: 1320   116.817149/s
     iteration_duration.............: avg=1.64s    min=574.74ms med=1.52s    max=3.34s    p(90)=2.29s    p(95)=2.54s   
     iterations.....................: 1320   116.817149/s
     vus............................: 66     min=66       max=200
     vus_max........................: 200    min=200      max=200

```


## Results

Load testing three different approaches resulted in vastly different performance.

The Node.js app using the BigQuery SDK had these request duration percentiles:
```
p(90)=3.8s
p(95)=3.98s   
```

The Node.js app using Cube Cloud to access BigQuery had these request duration percentiles:
```
p(90)=2.37s
p(95)=2.63s 
```

Load testing the Cube Cloud API directly resulted in these request duration percentiles:
```
p(90)=2.21s
p(95)=2.33s
```

This shows a significant improvement in performance when using Cube with pre-aggregations and caching.
