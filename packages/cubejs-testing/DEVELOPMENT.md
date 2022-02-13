### Run cloud driver tests

Setup a file outside of the git repo, for example for example ${HOME}/.env, containing cloud database config and
credentials. For example:

```shell
$ cat <<EOF > ${HOME}/.env
CUBEJS_AWS_KEY=...
CUBEJS_AWS_SECRET=...
CUBEJS_AWS_REGION=...
CUBEJS_AWS_S3_OUTPUT_LOCATION=...
EOF
```

Run integration tests:

```shell
$ cd packages/cubejs-athena-driver
$ CUBEJS_TEST_ENV=${HOME}/.env yarn test

$ cd packages/cubejs-bigquery-driver
$ CUBEJS_TEST_ENV=${HOME}/.env yarn test
```

Run e2e tests:

```shell
$ cd packages/cubejs-testing
$ yarn birdbox:driver --env-file=${HOME}/.env --mode=local --type=athena
$ yarn birdbox:driver --env-file=${HOME}/.env --mode=local --type=bigquery
```

### Run Cypress tests

$ docker build . -f packages/cubejs-docker/dev.Dockerfile -t localhost:5000/cubejs/cube:testx
$ cd packages/cubejs-testing
$ export BIRDBOX_CUBEJS_VERSION=testx
$ export BIRDBOX_CUBEJS_REGISTRY_PATH=localhost:5000/
$ export BIRDBOX_CYPRESS_BROWSER=chrome
$ export BIRDBOX_CYPRESS_TARGET=postgresql
$ export DEBUG=testcontainers
$ yarn database:minimal
$ yarn cypress:install
$ yarn cypress:birdbox

### Convert Postgres dump into csv and upload it to BigQuery

```shell
$ yarn dataset:minimal
$ psql template1 -c 'drop database test;'  
$ psql template1 -c 'create database test with owner test;'
$ psql -U test -d test -f birdbox-fixtures/datasets/test.sql
$ psql -U test -d test -c "\copy (SELECT * FROM public.events) to 'github-events-2015-01-01.csv' with csv header"

$ gsutil cp github-events-2015-01-01.csv gs://cube-cloud-staging-export-bucket/test/github-events-2015-01-01.csv
$ bq mk public
$ bq load --autodetect --source_format=CSV public.events gs://cube-cloud-staging-export-bucket/test/github-events-2015-01-01.csv
```
