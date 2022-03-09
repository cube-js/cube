### Run cloud driver tests

Setup a file outside of the git repo, for example for example ${HOME}/.env, containing cloud database config and
credentials. For example:

```shell
$ cat <<EOF > ${HOME}/.env.athena
CUBEJS_AWS_KEY=...
CUBEJS_AWS_SECRET=...
CUBEJS_AWS_REGION=...
CUBEJS_AWS_S3_OUTPUT_LOCATION=...
CUBEJS_DB_EXPORT_BUCKET=...
EOF
$ cat <<EOF > ${HOME}/.env.athena
CUBEJS_DB_BQ_PROJECT_ID=...
CUBEJS_DB_BQ_CREDENTIALS=...
CUBEJS_DB_EXPORT_BUCKET=...
EOF
```

Run integration tests:

```shell
$ cd packages/cubejs-athena-driver
$ env $(cat ~/.env.athena | xargs) yarn test

$ cd packages/cubejs-bigquery-driver
$ env $(cat ~/.env.bigquery | xargs) yarn test
```

Run e2e tests:

```shell
$ cd packages/cubejs-testing

$ env $(cat ~/.env.athena | xargs) yarn birdbox:athena --mode=local
$ env $(cat ~/.env.bigquery | xargs) yarn birdbox:bigquery --mode=local

$ env $(cat ~/.env.athena | xargs) yarn birdbox:athena --mode=docker
$ env $(cat ~/.env.bigquery | xargs) yarn birdbox:bigquery --mode=docker
```

### Run Cypress tests

```shell
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
```

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
