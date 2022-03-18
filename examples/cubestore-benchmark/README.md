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
$ RPS=1 DURATION=10s yarn test
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
           * cubestore: 1.00 iterations/s for 10s (maxVUs: 600, exec: cubestore, gracefulStop: 5m0s)
           * postgres: 1.00 iterations/s for 10s (maxVUs: 600, exec: postgres, gracefulStop: 5m0s)


running (0m10.5s), 0000/1200 VUs, 21 complete and 0 interrupted iterations
cubestore ✓ [======================================] 000/600 VUs  10s  1 iters/s
postgres  ✓ [======================================] 000/600 VUs  10s  1 iters/s

     ✓ is status 200

     checks..............................: 100.00% ✓ 21       ✗ 0     
     data_received.......................: 3.0 kB  289 B/s
     data_sent...........................: 1.9 kB  176 B/s
     http_req_blocked....................: avg=0.00s min=0.00s med=0.00s max=0.00s p(90)=0.00s p(95)=0.00s
     http_req_connecting.................: avg=0.00s min=0.00s med=0.00s max=0.00s p(90)=0.00s p(95)=0.00s
     http_req_duration...................: avg=0.67s min=0.27s med=0.70s max=0.91s p(90)=0.83s p(95)=0.88s
       { expected_response:true }........: avg=0.67s min=0.27s med=0.70s max=0.91s p(90)=0.83s p(95)=0.88s
     http_req_failed.....................: 0.00%   ✓ 0        ✗ 21    
     http_req_receiving..................: avg=0.00s min=0.00s med=0.00s max=0.00s p(90)=0.00s p(95)=0.00s
     http_req_sending....................: avg=0.00s min=0.00s med=0.00s max=0.00s p(90)=0.00s p(95)=0.00s
     http_req_tls_handshaking............: avg=0.00s min=0.00s med=0.00s max=0.00s p(90)=0.00s p(95)=0.00s
     http_req_waiting....................: avg=0.67s min=0.27s med=0.70s max=0.91s p(90)=0.83s p(95)=0.88s
     http_reqs...........................: 21      1.993169/s
     iteration_duration..................: avg=0.67s min=0.27s med=0.70s max=0.91s p(90)=0.83s p(95)=0.88s
     iterations..........................: 21      1.993169/s
     Latency (Cube.js with Cube Store)...: avg=0.58s min=0.27s med=0.61s max=0.82s p(90)=0.82s p(95)=0.82s
     Latency (Cube.js with Postgres).....: avg=0.77s min=0.63s med=0.77s max=0.91s p(90)=0.88s p(95)=0.89s
     vus.................................: 600     min=600    max=1200
     vus_max.............................: 1200    min=1200   max=1200

✨  Done in 11.55s.

```