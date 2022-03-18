# Cube Store benchmark template

## Setting up

* Update `cubejs-cubestore/docker-compose.yml` and `cubejs-postgres/docker-compose.yml` with data source credentials (see `TODO` comments)
* Update `cubejs-cubestore/schema` and `cubejs-postgres/schema` with relevant data schema that matches your data source
* Update `loadtest/queries.js` with relevant queries

## Running

* Run `docker-compose -p cubejs-cubestore -f cubejs-cubestore/docker-compose.yml up`
* Run `docker-compose -p cubejs-postgres -f cubejs-postgres/docker-compose.yml up`
* Go to `loadtest` and run `yarn install`
* Then, start the relay server using `yarn start`
* Then, run the load test using `RPS=<requests per second> DURATION=<duration, seconds>s npm test` (e.g., `RPS=1 DURATION=10s npm test`)

Example run:

```shell
% RPS=5 DURATION=10s yarn test
yarn run v1.22.17
$ k6 run --summary-time-unit s --summary-export result.json test.js

          /\      |‾‾| /‾‾/   /‾‾/   
     /\  /  \     |  |/  /   /  /    
    /  \/    \    |     (   /   ‾‾\  
   /          \   |  |\  \ |  (‾)  | 
  / __________ \  |__| \__\ \_____/ .io

  execution: local
     script: test.js
     output: -

  scenarios: (100.00%) 2 scenarios, 1200 max VUs, 5m10s max duration (incl. graceful stop):
           * cubestore: 5.00 iterations/s for 10s (maxVUs: 600, exec: cubestore, gracefulStop: 5m0s)
           * postgres: 5.00 iterations/s for 10s (maxVUs: 600, exec: postgres, gracefulStop: 5m0s)


running (0m37.7s), 0000/1200 VUs, 101 complete and 0 interrupted iterations
cubestore ✓ [======================================] 000/600 VUs  10s  5 iters/s
postgres  ✓ [======================================] 000/600 VUs  10s  5 iters/s

     ✓ is status 200

     checks..............................: 100.00% ✓ 101      ✗ 0     
     data_received.......................: 15 kB   388 B/s
     data_sent...........................: 8.9 kB  237 B/s
     http_req_blocked....................: avg=0.00s  min=0.00s med=0.00s  max=0.00s  p(90)=0.00s  p(95)=0.00s 
     http_req_connecting.................: avg=0.00s  min=0.00s med=0.00s  max=0.00s  p(90)=0.00s  p(95)=0.00s 
     http_req_duration...................: avg=10.18s min=0.50s med=10.13s max=29.11s p(90)=19.64s p(95)=23.29s
       { expected_response:true }........: avg=10.18s min=0.50s med=10.13s max=29.11s p(90)=19.64s p(95)=23.29s
     http_req_failed.....................: 0.00%   ✓ 0        ✗ 101   
     http_req_receiving..................: avg=0.00s  min=0.00s med=0.00s  max=0.00s  p(90)=0.00s  p(95)=0.00s 
     http_req_sending....................: avg=0.00s  min=0.00s med=0.00s  max=0.00s  p(90)=0.00s  p(95)=0.00s 
     http_req_tls_handshaking............: avg=0.00s  min=0.00s med=0.00s  max=0.00s  p(90)=0.00s  p(95)=0.00s 
     http_req_waiting....................: avg=10.18s min=0.50s med=10.13s max=29.11s p(90)=19.64s p(95)=23.29s
     http_reqs...........................: 101     2.678161/s
     iteration_duration..................: avg=10.18s min=0.50s med=10.13s max=29.11s p(90)=19.64s p(95)=23.29s
     iterations..........................: 101     2.678161/s
     Latency (Cube.js with Cube Store)...: avg=5.71s  min=0.50s med=5.03s  max=13.52s p(90)=10.82s p(95)=11.52s
     Latency (Cube.js with Postgres).....: avg=14.73s min=0.63s med=15.12s max=29.11s p(90)=23.32s p(95)=24.15s
     vus.................................: 600     min=600    max=1200
     vus_max.............................: 1200    min=1200   max=1200

✨  Done in 38.74s.

```