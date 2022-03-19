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
* Then, run the load test using `RPS=<requests per second> DURATION=<duration, seconds>s yarn test` (e.g., `RPS=5 DURATION=10s yarn test`)
