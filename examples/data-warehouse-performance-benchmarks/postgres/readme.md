# Load Testing PostgreSQL

Load tests written with K6.io. There are a total of three tests. 

- Node.js app connecting to PostgreSQL via [node-postgres npm package](https://node-postgres.com/)
- Node.js app connecting to [Cube Cloud](https://cubecloud.dev/auth/signup)

*TODO: Add link to full tutorial in blog.*

## Get Started

About the database:

- Cube's test database with sample data
- 2vCPU 7.5GB RAM managed PostgreSQL database on Google Cloud
- 1GB TPC-H dataset with >1.5M rows

```
host: 'demo-db-examples.cube.dev'
database: 'tpch'
port: 5432
user: 'cube'
password: '12345'
```

## Load Test Node.js API with node-postgres npm package

Start the Node.js app.

```bash
node pg-api.js
```

### Run the k6 load test for 1 virtual user.

```bash
k6 run -e vus=1 k6-bq.js
```

My tests returned:

```
adnanrahic@instance-1:~/benchmarks$ k6 run -e vus=1 k6-pg.js

          /\      |‾‾| /‾‾/   /‾‾/
     /\  /  \     |  |/  /   /  /
    /  \/    \    |     (   /   ‾‾\
   /          \   |  |\  \ |  (‾)  |
  / __________ \  |__| \__\ \_____/ .io

  execution: local
     script: k6-pg.js
     output: -

  scenarios: (100.00%) 1 scenario, 1 max VUs, 35s max duration (incl. graceful stop):
           * default: 1 looping VUs for 5s (gracefulStop: 30s)


running (05.0s), 0/1 VUs, 23 complete and 0 interrupted iterations
default ✓ [======================================] 1 VUs  5s

     data_received..................: 3.5 kB 698 B/s
     data_sent......................: 1.8 kB 367 B/s
     http_req_blocked...............: avg=62.15µs  min=2.28µs   med=3.91µs   max=1.34ms   p(90)=5.54µs   p(95)=6.63µs
     http_req_connecting............: avg=49.22µs  min=0s       med=0s       max=1.13ms   p(90)=0s       p(95)=0s
     http_req_duration..............: avg=217.53ms min=203.23ms med=207.03ms max=271.88ms p(90)=242.42ms p(95)=257.74ms
       { expected_response:true }...: avg=217.53ms min=203.23ms med=207.03ms max=271.88ms p(90)=242.42ms p(95)=257.74ms
     http_req_failed................: 0.00%  ✓ 0        ✗ 23
     http_req_receiving.............: avg=90.53µs  min=58.41µs  med=82.45µs  max=201.7µs  p(90)=120.19µs p(95)=120.87µs
     http_req_sending...............: avg=20.62µs  min=10.73µs  med=16.46µs  max=69.33µs  p(90)=28.1µs   p(95)=32.22µs
     http_req_tls_handshaking.......: avg=0s       min=0s       med=0s       max=0s       p(90)=0s       p(95)=0s
     http_req_waiting...............: avg=217.42ms min=203.13ms med=206.94ms max=271.79ms p(90)=242.26ms p(95)=257.59ms
     http_reqs......................: 23     4.591477/s
     iteration_duration.............: avg=217.71ms min=203.37ms med=207.15ms max=272ms    p(90)=243.7ms  p(95)=258.01ms
     iterations.....................: 23     4.591477/s
     vus............................: 1      min=1      max=1
     vus_max........................: 1      min=1      max=1

```

Condensed request duration p(90) and p(95):

```
p(90)=242.42ms
p(95)=257.74ms
```

### Run the k6 load test for 10 virtual users.

```bash
k6 run -e vus=10 k6-bq.js
```

My tests returned:

```
adnanrahic@instance-1:~/benchmarks$ k6 run -e vus=10 k6-pg.js

          /\      |‾‾| /‾‾/   /‾‾/
     /\  /  \     |  |/  /   /  /
    /  \/    \    |     (   /   ‾‾\
   /          \   |  |\  \ |  (‾)  |
  / __________ \  |__| \__\ \_____/ .io

  execution: local
     script: k6-pg.js
     output: -

  scenarios: (100.00%) 1 scenario, 10 max VUs, 35s max duration (incl. graceful stop):
           * default: 10 looping VUs for 5s (gracefulStop: 30s)


running (06.0s), 00/10 VUs, 28 complete and 0 interrupted iterations
default ✗ [======================================] 10 VUs  5s

     data_received..................: 4.3 kB 711 B/s
     data_sent......................: 2.2 kB 374 B/s
     http_req_blocked...............: avg=302.34µs min=2.23µs   med=3.41µs  max=1.68ms   p(90)=1.04ms   p(95)=1.26ms
     http_req_connecting............: avg=279.85µs min=0s       med=0s      max=1.5ms    p(90)=1ms      p(95)=1.22ms
     http_req_duration..............: avg=2.01s    min=865.64ms med=1.65s   max=4.91s    p(90)=3.4s     p(95)=3.72s
       { expected_response:true }...: avg=2.01s    min=865.64ms med=1.65s   max=4.91s    p(90)=3.4s     p(95)=3.72s
     http_req_failed................: 0.00%  ✓ 0        ✗ 28
     http_req_receiving.............: avg=88.51µs  min=52.34µs  med=82.95µs max=151.03µs p(90)=117.18µs p(95)=132.62µs
     http_req_sending...............: avg=50µs     min=12.82µs  med=25.57µs max=182.98µs p(90)=135.01µs p(95)=138.64µs
     http_req_tls_handshaking.......: avg=0s       min=0s       med=0s      max=0s       p(90)=0s       p(95)=0s
     http_req_waiting...............: avg=2.01s    min=865.55ms med=1.65s   max=4.91s    p(90)=3.4s     p(95)=3.72s
     http_reqs......................: 28     4.676568/s
     iteration_duration.............: avg=2.01s    min=865.8ms  med=1.65s   max=4.91s    p(90)=3.4s     p(95)=3.72s
     iterations.....................: 28     4.676568/s
     vus............................: 10     min=10     max=10
     vus_max........................: 10     min=10     max=10

```

Condensed request duration p(90) and p(95):

```
p(90)=3.4s
p(95)=3.72s
```

This is already unacceptable at only 10 concurrent requests!! 

## Load Test Node.js API with Cube

Start the Node.js app.

```bash
node cube-api.js
```

### Run the k6 load test for 10 virtual users.

```bash
k6 run -e vus=10 k6-cube.js
```

My tests returned:

```
adnanrahic@instance-1:~/benchmarks$ k6 run -e vus=10 k6-cube.js

          /\      |‾‾| /‾‾/   /‾‾/
     /\  /  \     |  |/  /   /  /
    /  \/    \    |     (   /   ‾‾\
   /          \   |  |\  \ |  (‾)  |
  / __________ \  |__| \__\ \_____/ .io

  execution: local
     script: k6-cube.js
     output: -

  scenarios: (100.00%) 1 scenario, 10 max VUs, 35s max duration (incl. graceful stop):
           * default: 10 looping VUs for 5s (gracefulStop: 30s)


running (05.2s), 00/10 VUs, 769 complete and 0 interrupted iterations
default ✗ [======================================] 10 VUs  5s

     data_received..................: 117 kB 22 kB/s
     data_sent......................: 62 kB  12 kB/s
     http_req_blocked...............: avg=11.78µs min=1.23µs  med=2.94µs  max=2.91ms   p(90)=4.58µs   p(95)=6.01µs
     http_req_connecting............: avg=3.76µs  min=0s      med=0s      max=1.31ms   p(90)=0s       p(95)=0s
     http_req_duration..............: avg=67.11ms min=20.76ms med=51.8ms  max=311.24ms p(90)=117.58ms p(95)=170.52ms
       { expected_response:true }...: avg=67.11ms min=20.76ms med=51.8ms  max=311.24ms p(90)=117.58ms p(95)=170.52ms
     http_req_failed................: 0.00%  ✓ 0          ✗ 769
     http_req_receiving.............: avg=67.3µs  min=18.76µs med=63.46µs max=244.19µs p(90)=96.05µs  p(95)=107.29µs
     http_req_sending...............: avg=17.42µs min=5.85µs  med=13.47µs max=479.33µs p(90)=25.09µs  p(95)=32.12µs
     http_req_tls_handshaking.......: avg=0s      min=0s      med=0s      max=0s       p(90)=0s       p(95)=0s
     http_req_waiting...............: avg=67.03ms min=20.67ms med=51.71ms max=311.18ms p(90)=117.41ms p(95)=170.32ms
     http_reqs......................: 769    147.603699/s
     iteration_duration.............: avg=67.22ms min=20.86ms med=51.89ms max=311.33ms p(90)=117.92ms p(95)=171.3ms
     iterations.....................: 769    147.603699/s
     vus............................: 10     min=10       max=10
     vus_max........................: 10     min=10       max=10
    
```

Condensed request duration p(90) and p(95):

```
p(90)=117.58ms
p(95)=170.52ms
```

This is quicker with 10 concurrent requests than PostgreSQL alone is with 1 concurrent request.


### Run the k6 load test for 30 virtual users.

```bash
k6 run -e vus=30 k6-cube.js
```

My tests returned:

```
adnanrahic@instance-1:~/benchmarks$ k6 run -e vus=30 k6-cube.js

          /\      |‾‾| /‾‾/   /‾‾/
     /\  /  \     |  |/  /   /  /
    /  \/    \    |     (   /   ‾‾\
   /          \   |  |\  \ |  (‾)  |
  / __________ \  |__| \__\ \_____/ .io

  execution: local
     script: k6-cube.js
     output: -

  scenarios: (100.00%) 1 scenario, 30 max VUs, 35s max duration (incl. graceful stop):
           * default: 30 looping VUs for 5s (gracefulStop: 30s)


running (05.1s), 00/30 VUs, 850 complete and 0 interrupted iterations
default ✓ [======================================] 30 VUs  5s

     data_received..................: 129 kB 25 kB/s
     data_sent......................: 68 kB  13 kB/s
     http_req_blocked...............: avg=65.97µs  min=1.17µs  med=2.83µs   max=11.47ms  p(90)=4.64µs   p(95)=8.71µs
     http_req_connecting............: avg=14.48µs  min=0s      med=0s       max=4.3ms    p(90)=0s       p(95)=0s
     http_req_duration..............: avg=178.41ms min=19.13ms med=166.65ms max=585.11ms p(90)=370.08ms p(95)=457.51ms
       { expected_response:true }...: avg=178.41ms min=19.13ms med=166.65ms max=585.11ms p(90)=370.08ms p(95)=457.51ms
     http_req_failed................: 0.00%  ✓ 0          ✗ 850
     http_req_receiving.............: avg=64.98µs  min=18.22µs med=60.94µs  max=439.96µs p(90)=90.05µs  p(95)=104.03µs
     http_req_sending...............: avg=18.16µs  min=5.94µs  med=12.8µs   max=276.83µs p(90)=25.47µs  p(95)=39.29µs
     http_req_tls_handshaking.......: avg=0s       min=0s      med=0s       max=0s       p(90)=0s       p(95)=0s
     http_req_waiting...............: avg=178.33ms min=19.06ms med=166.56ms max=585.04ms p(90)=369.99ms p(95)=457.45ms
     http_reqs......................: 850    166.506005/s
     iteration_duration.............: avg=178.58ms min=19.21ms med=166.73ms max=585.17ms p(90)=370.16ms p(95)=457.58ms
     iterations.....................: 850    166.506005/s
     vus............................: 30     min=30       max=30
     vus_max........................: 30     min=30       max=30

```

Condensed request duration p(90) and p(95):

```
p(90)=370.08ms
p(95)=457.51ms
```

This is quicker with 30 concurrent requests than PostgreSQL alone is with 1 concurrent request.

### Run the k6 load test for 50 virtual users.

```bash
k6 run -e vus=50 k6-cube.js
```

My tests returned:

```
adnanrahic@instance-1:~/benchmarks$ k6 run -e vus=50 k6-cube.js

          /\      |‾‾| /‾‾/   /‾‾/
     /\  /  \     |  |/  /   /  /
    /  \/    \    |     (   /   ‾‾\
   /          \   |  |\  \ |  (‾)  |
  / __________ \  |__| \__\ \_____/ .io

  execution: local
     script: k6-cube.js
     output: -

  scenarios: (100.00%) 1 scenario, 50 max VUs, 35s max duration (incl. graceful stop):
           * default: 50 looping VUs for 5s (gracefulStop: 30s)


running (05.1s), 00/50 VUs, 1011 complete and 0 interrupted iterations
default ✓ [======================================] 50 VUs  5s

     data_received..................: 154 kB 30 kB/s
     data_sent......................: 81 kB  16 kB/s
     http_req_blocked...............: avg=36.41µs  min=1.09µs  med=2.77µs   max=6.38ms   p(90)=4.72µs   p(95)=33.4µs
     http_req_connecting............: avg=30.87µs  min=0s      med=0s       max=6.34ms   p(90)=0s       p(95)=0s
     http_req_duration..............: avg=250.77ms min=21.84ms med=238.14ms max=819.83ms p(90)=498.55ms p(95)=607.99ms
       { expected_response:true }...: avg=250.77ms min=21.84ms med=238.14ms max=819.83ms p(90)=498.55ms p(95)=607.99ms
     http_req_failed................: 0.00%  ✓ 0          ✗ 1011
     http_req_receiving.............: avg=60.73µs  min=19.85µs med=56.33µs  max=685.57µs p(90)=83.91µs  p(95)=94.53µs
     http_req_sending...............: avg=25.49µs  min=6.13µs  med=12.36µs  max=1.57ms   p(90)=26.17µs  p(95)=44µs
     http_req_tls_handshaking.......: avg=0s       min=0s      med=0s       max=0s       p(90)=0s       p(95)=0s
     http_req_waiting...............: avg=250.68ms min=21.79ms med=238.06ms max=819.77ms p(90)=498.49ms p(95)=607.93ms
     http_reqs......................: 1011   196.438691/s
     iteration_duration.............: avg=250.89ms min=21.92ms med=238.23ms max=819.89ms p(90)=498.62ms p(95)=608.07ms
     iterations.....................: 1011   196.438691/s
     vus............................: 50     min=50       max=50
     vus_max........................: 50     min=50       max=50
    
```

Condensed request duration p(90) and p(95):

```
p(90)=498.55ms 
p(95)=607.99ms
```

This is still responding within half-a-second even at 50 concurrent requests.

### Run the k6 load test for 100 virtual users.

```bash
k6 run -e vus=100 k6-cube.js
```

My tests returned:

```
adnanrahic@instance-1:~/benchmarks$ k6 run -e vus=100 k6-cube.js

          /\      |‾‾| /‾‾/   /‾‾/
     /\  /  \     |  |/  /   /  /
    /  \/    \    |     (   /   ‾‾\
   /          \   |  |\  \ |  (‾)  |
  / __________ \  |__| \__\ \_____/ .io

  execution: local
     script: k6-cube.js
     output: -

  scenarios: (100.00%) 1 scenario, 100 max VUs, 35s max duration (incl. graceful stop):
           * default: 100 looping VUs for 5s (gracefulStop: 30s)


running (05.2s), 000/100 VUs, 1200 complete and 0 interrupted iterations
default ✓ [======================================] 100 VUs  5s

     data_received..................: 182 kB 35 kB/s
     data_sent......................: 96 kB  18 kB/s
     http_req_blocked...............: avg=94.64µs  min=1.16µs  med=2.77µs   max=3.94ms   p(90)=12.51µs  p(95)=527.97µs
     http_req_connecting............: avg=85.07µs  min=0s      med=0s       max=3.9ms    p(90)=0s       p(95)=445.8µs
     http_req_duration..............: avg=426.35ms min=45.27ms med=389.96ms max=1.14s    p(90)=624.04ms p(95)=708.01ms
       { expected_response:true }...: avg=426.35ms min=45.27ms med=389.96ms max=1.14s    p(90)=624.04ms p(95)=708.01ms
     http_req_failed................: 0.00%  ✓ 0          ✗ 1200
     http_req_receiving.............: avg=56.81µs  min=17.58µs med=53.07µs  max=221.64µs p(90)=80.59µs  p(95)=90.44µs
     http_req_sending...............: avg=71.41µs  min=5.68µs  med=12.59µs  max=11.08ms  p(90)=31.09µs  p(95)=299.34µs
     http_req_tls_handshaking.......: avg=0s       min=0s      med=0s       max=0s       p(90)=0s       p(95)=0s
     http_req_waiting...............: avg=426.23ms min=44.99ms med=389.88ms max=1.14s    p(90)=623.97ms p(95)=707.95ms
     http_reqs......................: 1200   229.542717/s
     iteration_duration.............: avg=426.54ms min=49.05ms med=390.05ms max=1.14s    p(90)=624.12ms p(95)=708.14ms
     iterations.....................: 1200   229.542717/s
     vus............................: 100    min=100      max=100
     vus_max........................: 100    min=100      max=100

```

Condensed request duration p(90) and p(95):

```
p(90)=624.04ms
p(95)=708.01ms
```

And at 100 concurrent requests, it's still well under a second to get a response.

### Run the k6 load test for 200 virtual users.

```bash
k6 run -e vus=200 k6-cube.js
```

My tests returned:

```
adnanrahic@instance-1:~/benchmarks$ k6 run -e vus=200 k6-cube.js

          /\      |‾‾| /‾‾/   /‾‾/
     /\  /  \     |  |/  /   /  /
    /  \/    \    |     (   /   ‾‾\
   /          \   |  |\  \ |  (‾)  |
  / __________ \  |__| \__\ \_____/ .io

  execution: local
     script: k6-cube.js
     output: -

  scenarios: (100.00%) 1 scenario, 200 max VUs, 35s max duration (incl. graceful stop):
           * default: 200 looping VUs for 5s (gracefulStop: 30s)


running (05.8s), 000/200 VUs, 1316 complete and 0 interrupted iterations
default ✗ [======================================] 200 VUs  5s

     data_received..................: 200 kB 35 kB/s
     data_sent......................: 105 kB 18 kB/s
     http_req_blocked...............: avg=186.77µs min=1.06µs  med=2.92µs   max=9.96ms p(90)=349.65µs p(95)=1.48ms
     http_req_connecting............: avg=172.69µs min=0s      med=0s       max=9.8ms  p(90)=280.35µs p(95)=1.28ms
     http_req_duration..............: avg=823.38ms min=55.08ms med=782.73ms max=1.84s  p(90)=1.51s    p(95)=1.63s
       { expected_response:true }...: avg=823.38ms min=55.08ms med=782.73ms max=1.84s  p(90)=1.51s    p(95)=1.63s
     http_req_failed................: 0.00%  ✓ 0          ✗ 1316
     http_req_receiving.............: avg=62.06µs  min=16.02µs med=56.89µs  max=1.06ms p(90)=89.89µs  p(95)=103.87µs
     http_req_sending...............: avg=112.87µs min=5.46µs  med=13.82µs  max=3.53ms p(90)=133.1µs  p(95)=678.84µs
     http_req_tls_handshaking.......: avg=0s       min=0s      med=0s       max=0s     p(90)=0s       p(95)=0s
     http_req_waiting...............: avg=823.21ms min=54.97ms med=782.6ms  max=1.84s  p(90)=1.51s    p(95)=1.63s
     http_reqs......................: 1316   227.797529/s
     iteration_duration.............: avg=823.69ms min=55.22ms med=784.44ms max=1.84s  p(90)=1.51s    p(95)=1.63s
     iterations.....................: 1316   227.797529/s
     vus............................: 200    min=200      max=200
     vus_max........................: 200    min=200      max=200

```

Condensed request duration p(90) and p(95):

```
p(90)=1.51s
p(95)=1.63s
```

And at 200 concurrent requests, we see the Node.js API is starting to struggle a bit with the concurrency. The response is now hovering at around 1.5 seconds.

## Results

Load testing three different approaches resulted in vastly different performance.

The Node.js app using the node-postgres npm package had these request duration percentiles for 10 concurrent users:

```
p(90)=3.4s
p(95)=3.72s
```

The Node.js app using Cube Cloud to access PostgreSQL had these request duration percentiles for 10 concurrent users:

```
p(90)=117.58ms
p(95)=170.52ms 
```

The Node.js app using Cube Cloud to access PostgreSQL had these request duration percentiles for 100 concurrent users:

```
p(90)=624.04ms
p(95)=708.01ms
```

The Node.js app using Cube Cloud to access PostgreSQL had these request duration percentiles for 200 concurrent users:

```
p(90)=1.51s
p(95)=1.63s
```

This shows a significant improvement in performance when using Cube with pre-aggregations and caching.
