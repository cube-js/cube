<p align="center"><a href="https://cube.dev"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[Website](https://cube.dev) • [Docs](https://cube.dev/docs) • [Blog](https://cube.dev/blog) • [Slack](https://slack.cube.dev) • [Discourse](https://forum.cube.dev/) • [Twitter](https://twitter.com/thecubejs)

[![npm version](https://badge.fury.io/js/%40cubejs-backend%2Fserver.svg)](https://badge.fury.io/js/%40cubejs-backend%2Fserver)
[![GitHub Actions](https://github.com/cube-js/cube.js/workflows/Build/badge.svg)](https://github.com/cube-js/cube.js/actions?query=workflow%3ABuild+branch%3Amaster)

# Cube.js Testing

Internal package for testing.

[Learn more](https://github.com/cube-js/cube.js#getting-started)

### License

Cube.js Client Core is [MIT licensed](./LICENSE).

### Manually run cloud driver tests

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

Run end2end tests:

```shell
$ cd packages/cubejs-testing
$ yarn birdbox:driver --env-file=${HOME}/.env --mode=local --type=athena
$ yarn birdbox:driver --env-file=${HOME}/.env --mode=local --type=bigquery
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
