# Cube Store benchmark template

## Setting up

* Create a configuraton file with your BigQuery credentials, e.g.
```shell
$ cat <<EOF > ~/.env.bigquery
CUBEJS_DB_BQ_PROJECT_ID=...
CUBEJS_DB_EXPORT_BUCKET=...
CUBEJS_DB_BQ_CREDENTIALS=...
EOF
```
* Update `cubejs-cubestore/schema` and `cubejs-postgres/schema` with relevant data schema that matches your data source
* Update `loadtest/queries.js` with relevant queries

## Running

* Run `env $(cat ~/.env.bigquery | xargs) docker-compose -p cubejs-cubestore -f cubejs-cubestore/docker-compose.yml up`
* Run `env $(cat ~/.env.bigquery | xargs) docker-compose -p cubejs-postgres -f cubejs-postgres/docker-compose.yml up`
* Go to `loadtest` and run `yarn install`
* Then, start the relay server using `yarn start`
* Then, run the load test using `RPS=<requests per second> DURATION=<duration, seconds>s yarn test` (e.g., `RPS=5 DURATION=10s yarn test`)
