# Cube Store benchmark template

## Setting up

* Update `cubejs-cubestore/docker-compose.yml` and `cubejs-postgres/docker-compose.yml` with data source credentials (see `TODO` comments)
* Update `cubejs-cubestore/schema` and `cubejs-postgres/schema` with relevant data schema that matches your data source
* Update `loadtest/queries.js` with relevant queries

## Running

* Go to `cubejs-cubestore` and run `docker-compose -p cubejs-cubestore up`
* Go to `cubejs-postgres` and run `docker-compose -p cubejs-postgres up`
* Go to `loadtest` and run `npm install`
* Then, start the relay server using `npm start`
* Then, run the load test using `RPS=<requests per second> DURATION=<duration, seconds>s npm test` (e.g., `RPS=10 DURATION=10s npm test`)