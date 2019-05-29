# Change Log

All notable changes to this project will be documented in this file.
See [Conventional Commits](https://conventionalcommits.org) for commit guidelines.

## [0.9.8](https://github.com/statsbotco/cube.js/compare/v0.9.7...v0.9.8) (2019-05-29)


### Features

* **cubejs-cli:** add token generation ([#67](https://github.com/statsbotco/cube.js/issues/67)) ([2813fed](https://github.com/statsbotco/cube.js/commit/2813fed))
* **postgres-driver:** SSL error hint for Heroku users ([0e9b9cb](https://github.com/statsbotco/cube.js/commit/0e9b9cb))





## [0.9.7](https://github.com/statsbotco/cube.js/compare/v0.9.6...v0.9.7) (2019-05-27)


### Features

* **postgres-driver:** support CUBEJS_DB_SSL option ([67a767e](https://github.com/statsbotco/cube.js/commit/67a767e))





## [0.9.6](https://github.com/statsbotco/cube.js/compare/v0.9.5...v0.9.6) (2019-05-24)


### Bug Fixes

* contains filter does not work with MS SQL Server database ([35210f6](https://github.com/statsbotco/cube.js/commit/35210f6)), closes [#113](https://github.com/statsbotco/cube.js/issues/113)


### Features

* better npm fail message in Playground ([545a020](https://github.com/statsbotco/cube.js/commit/545a020))
* **playground:** better add to dashboard error messages ([94e8dbf](https://github.com/statsbotco/cube.js/commit/94e8dbf))





## [0.9.5](https://github.com/statsbotco/cube.js/compare/v0.9.4...v0.9.5) (2019-05-22)


### Features

* Propagate `renewQuery` option from API to orchestrator ([9c640ba](https://github.com/statsbotco/cube.js/commit/9c640ba)), closes [#112](https://github.com/statsbotco/cube.js/issues/112)





## [0.9.4](https://github.com/statsbotco/cube.js/compare/v0.9.3...v0.9.4) (2019-05-22)


### Features

* Add `refreshKeyRenewalThreshold` option ([aa69449](https://github.com/statsbotco/cube.js/commit/aa69449)), closes [#112](https://github.com/statsbotco/cube.js/issues/112)





## [0.9.3](https://github.com/statsbotco/cube.js/compare/v0.9.2...v0.9.3) (2019-05-21)


### Bug Fixes

* **playground:** revert back create-react-app to npx as there're much more problems with global npm ([e434939](https://github.com/statsbotco/cube.js/commit/e434939))





## [0.9.2](https://github.com/statsbotco/cube.js/compare/v0.9.1...v0.9.2) (2019-05-11)


### Bug Fixes

* External rollups serverless implementation ([6d13370](https://github.com/statsbotco/cube.js/commit/6d13370))





## [0.9.1](https://github.com/statsbotco/cube.js/compare/v0.9.0...v0.9.1) (2019-05-11)


### Bug Fixes

* update BaseDriver dependencies ([a7aef2b](https://github.com/statsbotco/cube.js/commit/a7aef2b))





# [0.9.0](https://github.com/statsbotco/cube.js/compare/v0.8.7...v0.9.0) (2019-05-11)


### Features

* External rollup implementation ([d22a809](https://github.com/statsbotco/cube.js/commit/d22a809))





## [0.8.7](https://github.com/statsbotco/cube.js/compare/v0.8.6...v0.8.7) (2019-05-09)


### Bug Fixes

* **cubejs-react:** add core-js dependency ([#107](https://github.com/statsbotco/cube.js/issues/107)) ([0e13ffe](https://github.com/statsbotco/cube.js/commit/0e13ffe))
* **query-orchestrator:** Athena got swamped by fetch schema requests ([d8b5440](https://github.com/statsbotco/cube.js/commit/d8b5440))





## [0.8.6](https://github.com/statsbotco/cube.js/compare/v0.8.5...v0.8.6) (2019-05-05)


### Bug Fixes

* **cli:** Update Slack Community Link ([#101](https://github.com/statsbotco/cube.js/issues/101)) ([c5fd43f](https://github.com/statsbotco/cube.js/commit/c5fd43f))
* **playground:** Update Slack Community Link ([#102](https://github.com/statsbotco/cube.js/issues/102)) ([61a9bb0](https://github.com/statsbotco/cube.js/commit/61a9bb0))


### Features

* Replace codesandbox by running dashboard react-app directly ([861c817](https://github.com/statsbotco/cube.js/commit/861c817))





## [0.8.5](https://github.com/statsbotco/cube.js/compare/v0.8.4...v0.8.5) (2019-05-02)


### Bug Fixes

* **clickhouse-driver:** merging config with custom queryOptions which were not passing along the database ([#100](https://github.com/statsbotco/cube.js/issues/100)) ([dedc279](https://github.com/statsbotco/cube.js/commit/dedc279))





## [0.8.4](https://github.com/statsbotco/cube.js/compare/v0.8.3...v0.8.4) (2019-05-02)


### Features

* Angular client ([#99](https://github.com/statsbotco/cube.js/issues/99)) ([640e6de](https://github.com/statsbotco/cube.js/commit/640e6de))





## [0.8.3](https://github.com/statsbotco/cube.js/compare/v0.8.2...v0.8.3) (2019-05-01)


### Features

* clickhouse dialect implementation ([#98](https://github.com/statsbotco/cube.js/issues/98)) ([7236e29](https://github.com/statsbotco/cube.js/commit/7236e29)), closes [#93](https://github.com/statsbotco/cube.js/issues/93)





## [0.8.2](https://github.com/statsbotco/cube.js/compare/v0.8.1...v0.8.2) (2019-04-30)


### Bug Fixes

* Wrong variables when creating new BigQuery backed project ([bae6348](https://github.com/statsbotco/cube.js/commit/bae6348)), closes [#97](https://github.com/statsbotco/cube.js/issues/97)





## [0.8.1](https://github.com/statsbotco/cube.js/compare/v0.8.0...v0.8.1) (2019-04-30)


### Bug Fixes

* add the missing @cubejs-client/vue package ([#95](https://github.com/statsbotco/cube.js/issues/95)) ([9e8c4be](https://github.com/statsbotco/cube.js/commit/9e8c4be))


### Features

* Driver for ClickHouse database support ([#94](https://github.com/statsbotco/cube.js/issues/94)) ([0f05321](https://github.com/statsbotco/cube.js/commit/0f05321)), closes [#1](https://github.com/statsbotco/cube.js/issues/1)
* Serverless Google Cloud Platform in CLI support ([392ba1e](https://github.com/statsbotco/cube.js/commit/392ba1e))





# [0.8.0](https://github.com/statsbotco/cube.js/compare/v0.7.10...v0.8.0) (2019-04-29)


### Features

* Serverless Google Cloud Platform support ([89ec0ec](https://github.com/statsbotco/cube.js/commit/89ec0ec))





## [0.7.10](https://github.com/statsbotco/cube.js/compare/v0.7.9...v0.7.10) (2019-04-25)


### Bug Fixes

* **client-core:** Table pivot incorrectly behaves with multiple measures ([adb2270](https://github.com/statsbotco/cube.js/commit/adb2270))
* **client-core:** use ',' as standard axisValue delimiter ([e889955](https://github.com/statsbotco/cube.js/commit/e889955)), closes [#19](https://github.com/statsbotco/cube.js/issues/19)





## [0.7.9](https://github.com/statsbotco/cube.js/compare/v0.7.8...v0.7.9) (2019-04-24)


### Features

* **schema-compiler:** Allow to pass functions to USER_CONTEXT ([b489090](https://github.com/statsbotco/cube.js/commit/b489090)), closes [#88](https://github.com/statsbotco/cube.js/issues/88)





## [0.7.8](https://github.com/statsbotco/cube.js/compare/v0.7.7...v0.7.8) (2019-04-24)


### Bug Fixes

* **playground:** Dashboard doesn't work on Windows ([48a2ec4](https://github.com/statsbotco/cube.js/commit/48a2ec4)), closes [#82](https://github.com/statsbotco/cube.js/issues/82)





## [0.7.7](https://github.com/statsbotco/cube.js/compare/v0.7.6...v0.7.7) (2019-04-24)


### Bug Fixes

* **playground:** Dashboard doesn't work on Windows ([7c48aa4](https://github.com/statsbotco/cube.js/commit/7c48aa4)), closes [#82](https://github.com/statsbotco/cube.js/issues/82)





## [0.7.6](https://github.com/statsbotco/cube.js/compare/v0.7.5...v0.7.6) (2019-04-23)


### Bug Fixes

* **playground:** Cannot read property 'content' of undefined at e.value ([7392feb](https://github.com/statsbotco/cube.js/commit/7392feb))
* Use cross-fetch instead of isomorphic-fetch to allow React-Native builds ([#92](https://github.com/statsbotco/cube.js/issues/92)) ([79150f4](https://github.com/statsbotco/cube.js/commit/79150f4))
* **query-orchestrator:** add RedisFactory and promisify methods manually ([#89](https://github.com/statsbotco/cube.js/issues/89)) ([cdfcd87](https://github.com/statsbotco/cube.js/commit/cdfcd87)), closes [#84](https://github.com/statsbotco/cube.js/issues/84)


### Features

* Support member key in filters in query ([#91](https://github.com/statsbotco/cube.js/issues/91)) ([e1fccc0](https://github.com/statsbotco/cube.js/commit/e1fccc0))
* **schema-compiler:** Athena rollingWindow support ([f112c69](https://github.com/statsbotco/cube.js/commit/f112c69))





## [0.7.5](https://github.com/statsbotco/cube.js/compare/v0.7.4...v0.7.5) (2019-04-18)


### Bug Fixes

* **schema-compiler:** Athena, Mysql and BigQuery doesn't respect multiple contains filter ([0a8f324](https://github.com/statsbotco/cube.js/commit/0a8f324))





## [0.7.4](https://github.com/statsbotco/cube.js/compare/v0.7.3...v0.7.4) (2019-04-17)


### Bug Fixes

* Make dashboard app creation explicit. Show error messages if dashboard failed to create. ([3b2a22b](https://github.com/statsbotco/cube.js/commit/3b2a22b))
* **api-gateway:** measures is always required ([04adb7d](https://github.com/statsbotco/cube.js/commit/04adb7d))
* **mongobi-driver:** fix ssl configuration ([#78](https://github.com/statsbotco/cube.js/issues/78)) ([ddc4dff](https://github.com/statsbotco/cube.js/commit/ddc4dff))





## [0.7.3](https://github.com/statsbotco/cube.js/compare/v0.7.2...v0.7.3) (2019-04-16)


### Bug Fixes

* Allow SSR: use isomorphic-fetch instead of whatwg-fetch. ([902e581](https://github.com/statsbotco/cube.js/commit/902e581)), closes [#1](https://github.com/statsbotco/cube.js/issues/1)





## [0.7.2](https://github.com/statsbotco/cube.js/compare/v0.7.1...v0.7.2) (2019-04-15)


### Bug Fixes

* Avoid 502 for Playground in serverless: minimize babel ([f9d3171](https://github.com/statsbotco/cube.js/commit/f9d3171))


### Features

* MS SQL database driver ([48fbe66](https://github.com/statsbotco/cube.js/commit/48fbe66)), closes [#76](https://github.com/statsbotco/cube.js/issues/76)





## [0.7.1](https://github.com/statsbotco/cube.js/compare/v0.7.0...v0.7.1) (2019-04-15)


### Bug Fixes

* **serverless:** `getApiHandler` called on undefined ([0ee5121](https://github.com/statsbotco/cube.js/commit/0ee5121))
* Allow Playground to work in Serverless mode ([2c0c89c](https://github.com/statsbotco/cube.js/commit/2c0c89c))





# [0.7.0](https://github.com/statsbotco/cube.js/compare/v0.6.2...v0.7.0) (2019-04-15)


### Features

* App multi-tenancy support in single ServerCore instance ([6f0220f](https://github.com/statsbotco/cube.js/commit/6f0220f))





## [0.6.2](https://github.com/statsbotco/cube.js/compare/v0.6.1...v0.6.2) (2019-04-12)


### Features

* Natural language date range support ([b962e80](https://github.com/statsbotco/cube.js/commit/b962e80))
* **api-gateway:** Order support ([670237b](https://github.com/statsbotco/cube.js/commit/670237b))





## [0.6.1](https://github.com/statsbotco/cube.js/compare/v0.6.0...v0.6.1) (2019-04-11)


### Bug Fixes

* Get Playground API_URL from window.location until provided explicitly in env. Remote server playground case. ([7b1a0ff](https://github.com/statsbotco/cube.js/commit/7b1a0ff))


### Features

* Disable authentication checks in developer mode ([bc09eba](https://github.com/statsbotco/cube.js/commit/bc09eba))
* Formatted error logging in developer mode ([3376a50](https://github.com/statsbotco/cube.js/commit/3376a50))





# [0.6.0](https://github.com/statsbotco/cube.js/compare/v0.5.2...v0.6.0) (2019-04-09)


### Bug Fixes

* **playground:** no such file or directory, scandir 'dashboard-app/src' ([64ec481](https://github.com/statsbotco/cube.js/commit/64ec481))


### Features

* query validation added in api-gateway ([#73](https://github.com/statsbotco/cube.js/issues/73)) ([21f6176](https://github.com/statsbotco/cube.js/commit/21f6176)), closes [#39](https://github.com/statsbotco/cube.js/issues/39)
* QueryBuilder heuristics. Playground area, table and number implementation. ([c883a48](https://github.com/statsbotco/cube.js/commit/c883a48))
* Vue.js reactivity on query update ([#70](https://github.com/statsbotco/cube.js/issues/70)) ([167fdbf](https://github.com/statsbotco/cube.js/commit/167fdbf))





## [0.5.2](https://github.com/statsbotco/cube.js/compare/v0.5.1...v0.5.2) (2019-04-05)


### Features

* Add redshift to postgres driver link ([#71](https://github.com/statsbotco/cube.js/issues/71)) ([4797588](https://github.com/statsbotco/cube.js/commit/4797588))
* Playground UX improvements ([6760a1d](https://github.com/statsbotco/cube.js/commit/6760a1d))





## [0.5.1](https://github.com/statsbotco/cube.js/compare/v0.5.0...v0.5.1) (2019-04-02)


### Features

* BigQuery driver ([654edac](https://github.com/statsbotco/cube.js/commit/654edac))
* Vue package improvements and docs ([fc38e69](https://github.com/statsbotco/cube.js/commit/fc38e69)), closes [#68](https://github.com/statsbotco/cube.js/issues/68)





# [0.5.0](https://github.com/statsbotco/cube.js/compare/v0.4.6...v0.5.0) (2019-04-01)


### Bug Fixes

* **schema-compiler:** joi@10.6.0 upgrade to joi@14.3.1 ([#59](https://github.com/statsbotco/cube.js/issues/59)) ([f035531](https://github.com/statsbotco/cube.js/commit/f035531))
* mongobi issue with parsing schema file with nested fields ([eaf1631](https://github.com/statsbotco/cube.js/commit/eaf1631)), closes [#55](https://github.com/statsbotco/cube.js/issues/55)


### Features

* add basic vue support ([#65](https://github.com/statsbotco/cube.js/issues/65)) ([f45468b](https://github.com/statsbotco/cube.js/commit/f45468b))
* use local queue and cache for local dev server instead of Redis one ([50f1bbb](https://github.com/statsbotco/cube.js/commit/50f1bbb))





## [0.4.6](https://github.com/statsbotco/cube.js/compare/v0.4.5...v0.4.6) (2019-03-27)


### Features

* Dashboard Generator for Playground ([28a42ee](https://github.com/statsbotco/cube.js/commit/28a42ee))





## [0.4.5](https://github.com/statsbotco/cube.js/compare/v0.4.4...v0.4.5) (2019-03-21)


### Bug Fixes

* client-react - query prop now has default blank value ([#54](https://github.com/statsbotco/cube.js/issues/54)) ([27e7090](https://github.com/statsbotco/cube.js/commit/27e7090))


### Features

* Make API path namespace configurable ([#53](https://github.com/statsbotco/cube.js/issues/53)) ([b074a3d](https://github.com/statsbotco/cube.js/commit/b074a3d))
* Playground filters implementation ([de4315d](https://github.com/statsbotco/cube.js/commit/de4315d))





## [0.4.4](https://github.com/statsbotco/cube.js/compare/v0.4.3...v0.4.4) (2019-03-17)


### Bug Fixes

* Postgres doesn't show any data for queries with time dimension. ([e95e6fe](https://github.com/statsbotco/cube.js/commit/e95e6fe))


### Features

* Introduce Schema generation UI in Playground ([349c7d0](https://github.com/statsbotco/cube.js/commit/349c7d0))





## [0.4.3](https://github.com/statsbotco/cube.js/compare/v0.4.2...v0.4.3) (2019-03-15)


### Bug Fixes

* **mongobi-driver:** implement `convert_tz` as a simple hour shift ([c97e451](https://github.com/statsbotco/cube.js/commit/c97e451)), closes [#50](https://github.com/statsbotco/cube.js/issues/50)





## [0.4.2](https://github.com/statsbotco/cube.js/compare/v0.4.1...v0.4.2) (2019-03-14)


### Bug Fixes

* **mongobi-driver:** Fix Server does not support secure connnection on connection to localhost ([3202508](https://github.com/statsbotco/cube.js/commit/3202508))





## [0.4.1](https://github.com/statsbotco/cube.js/compare/v0.4.0...v0.4.1) (2019-03-14)


### Bug Fixes

* concat called on undefined for empty MongoDB password ([7d75b1e](https://github.com/statsbotco/cube.js/commit/7d75b1e))


### Features

* Allow to use custom checkAuth middleware ([19d5cd8](https://github.com/statsbotco/cube.js/commit/19d5cd8)), closes [#42](https://github.com/statsbotco/cube.js/issues/42)





# [0.4.0](https://github.com/statsbotco/cube.js/compare/v0.3.5-alpha.0...v0.4.0) (2019-03-13)


### Features

* Add MongoBI connector and schema adapter support ([3ebbbf0](https://github.com/statsbotco/cube.js/commit/3ebbbf0))





## [0.3.5-alpha.0](https://github.com/statsbotco/cube.js/compare/v0.3.5...v0.3.5-alpha.0) (2019-03-12)

**Note:** Version bump only for package cubejs
