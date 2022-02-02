<p align="center"><a href="https://cube.dev"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[Website](https://cube.dev) • [Docs](https://cube.dev/docs) • [Blog](https://cube.dev/blog) • [Slack](https://slack.cube.dev) • [Discourse](https://forum.cube.dev/) • [Twitter](https://twitter.com/thecubejs)

[![npm version](https://badge.fury.io/js/%40cubejs-backend%2Fserver.svg)](https://badge.fury.io/js/%40cubejs-backend%2Fserver)
[![GitHub Actions](https://github.com/cube-js/cube.js/workflows/Build/badge.svg)](https://github.com/cube-js/cube.js/actions?query=workflow%3ABuild+branch%3Amaster)

# Cube.js Testing

Internal package for testing.

[Learn more](https://github.com/cube-js/cube.js#getting-started)

### License

Cube.js Client Core is [MIT licensed](./LICENSE).

### Convert Postgres dump into csv

$ yarn dataset:minimal
$ psql template1 -c 'drop database test;'  
$ psql template1 -c 'create database test with owner test;'
$ psql -U test -d test -f birdbox-fixtures/datasets/test.sql
$ psql -U test -d test -c "\copy (SELECT * FROM public.events) to 'github-events-2015-01-01.csv' with csv header"

### Setup BQ data

```shell
$ gsutil cp github-events-2015-01-01.csv gs://cube-cloud-staging-export-bucket/test/github-events-2015-01-01.csv
$ bq mk public
$ bq load --autodetect --source_format=CSV public.events gs://cube-cloud-staging-export-bucket/test/github-events-2015-01-01.csv
```