# Change Log

All notable changes to this project will be documented in this file.
See [Conventional Commits](https://conventionalcommits.org) for commit guidelines.

# [0.10.0](https://github.com/statsbotco/cubejs-client/compare/v0.9.24...v0.10.0) (2019-06-21)


### Features

* **api-gateway:** `queryTransformer` security hook ([a9c41b2](https://github.com/statsbotco/cubejs-client/commit/a9c41b2))





## [0.9.20](https://github.com/statsbotco/cubejs-client/compare/v0.9.19...v0.9.20) (2019-06-16)


### Bug Fixes

* **api-gateway:** Unexpected token u in JSON at position 0 at JSON.parse ([f95cea8](https://github.com/statsbotco/cubejs-client/commit/f95cea8))





## [0.9.19](https://github.com/statsbotco/cubejs-client/compare/v0.9.18...v0.9.19) (2019-06-13)


### Bug Fixes

* **api-gateway:** handle can't parse date: Cannot read property 'end' of undefined ([a61b0da](https://github.com/statsbotco/cubejs-client/commit/a61b0da))
* Handle requests for hidden members: TypeError: Cannot read property 'type' of undefined at R.pipe.R.map.p ([5cdf71b](https://github.com/statsbotco/cubejs-client/commit/5cdf71b))





## [0.9.12](https://github.com/statsbotco/cubejs-client/compare/v0.9.11...v0.9.12) (2019-06-03)


### Bug Fixes

* **api-gateway:** Unexpected token u in JSON at position 0 at JSON.parse ([91ca994](https://github.com/statsbotco/cubejs-client/commit/91ca994))





## [0.9.5](https://github.com/statsbotco/cubejs-client/compare/v0.9.4...v0.9.5) (2019-05-22)


### Features

* Propagate `renewQuery` option from API to orchestrator ([9c640ba](https://github.com/statsbotco/cubejs-client/commit/9c640ba)), closes [#112](https://github.com/statsbotco/cubejs-client/issues/112)





# [0.9.0](https://github.com/statsbotco/cubejs-client/compare/v0.8.7...v0.9.0) (2019-05-11)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.8.4](https://github.com/statsbotco/cubejs-client/compare/v0.8.3...v0.8.4) (2019-05-02)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.8.1](https://github.com/statsbotco/cubejs-client/compare/v0.8.0...v0.8.1) (2019-04-30)

**Note:** Version bump only for package @cubejs-backend/api-gateway





# [0.8.0](https://github.com/statsbotco/cubejs-client/compare/v0.7.10...v0.8.0) (2019-04-29)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.7.6](https://github.com/statsbotco/cubejs-client/compare/v0.7.5...v0.7.6) (2019-04-23)


### Features

* Support member key in filters in query ([#91](https://github.com/statsbotco/cubejs-client/issues/91)) ([e1fccc0](https://github.com/statsbotco/cubejs-client/commit/e1fccc0))





## [0.7.4](https://github.com/statsbotco/cubejs-client/compare/v0.7.3...v0.7.4) (2019-04-17)


### Bug Fixes

* **api-gateway:** measures is always required ([04adb7d](https://github.com/statsbotco/cubejs-client/commit/04adb7d))





# [0.7.0](https://github.com/statsbotco/cubejs-client/compare/v0.6.2...v0.7.0) (2019-04-15)


### Features

* App multi-tenancy support in single ServerCore instance ([6f0220f](https://github.com/statsbotco/cubejs-client/commit/6f0220f))





## [0.6.2](https://github.com/statsbotco/cubejs-client/compare/v0.6.1...v0.6.2) (2019-04-12)


### Features

* Natural language date range support ([b962e80](https://github.com/statsbotco/cubejs-client/commit/b962e80))
* **api-gateway:** Order support ([670237b](https://github.com/statsbotco/cubejs-client/commit/670237b))





## [0.6.1](https://github.com/statsbotco/cubejs-client/compare/v0.6.0...v0.6.1) (2019-04-11)


### Features

* Disable authentication checks in developer mode ([bc09eba](https://github.com/statsbotco/cubejs-client/commit/bc09eba))
* Formatted error logging in developer mode ([3376a50](https://github.com/statsbotco/cubejs-client/commit/3376a50))





# [0.6.0](https://github.com/statsbotco/cubejs-client/compare/v0.5.2...v0.6.0) (2019-04-09)


### Features

* query validation added in api-gateway ([#73](https://github.com/statsbotco/cubejs-client/issues/73)) ([21f6176](https://github.com/statsbotco/cubejs-client/commit/21f6176)), closes [#39](https://github.com/statsbotco/cubejs-client/issues/39)





# [0.5.0](https://github.com/statsbotco/cubejs-client/compare/v0.4.6...v0.5.0) (2019-04-01)

**Note:** Version bump only for package @cubejs-backend/api-gateway





## [0.4.5](https://github.com/statsbotco/cubejs-client/compare/v0.4.4...v0.4.5) (2019-03-21)


### Features

* Make API path namespace configurable ([#53](https://github.com/statsbotco/cubejs-client/issues/53)) ([b074a3d](https://github.com/statsbotco/cubejs-client/commit/b074a3d))





## [0.4.4](https://github.com/statsbotco/cubejs-client/compare/v0.4.3...v0.4.4) (2019-03-17)


### Bug Fixes

* Postgres doesn't show any data for queries with time dimension. ([e95e6fe](https://github.com/statsbotco/cubejs-client/commit/e95e6fe))





## [0.4.3](https://github.com/statsbotco/cubejs-client/compare/v0.4.2...v0.4.3) (2019-03-15)


### Bug Fixes

* **mongobi-driver:** implement `convert_tz` as a simple hour shift ([c97e451](https://github.com/statsbotco/cubejs-client/commit/c97e451)), closes [#50](https://github.com/statsbotco/cubejs-client/issues/50)





## [0.4.1](https://github.com/statsbotco/cubejs-client/compare/v0.4.0...v0.4.1) (2019-03-14)


### Features

* Allow to use custom checkAuth middleware ([19d5cd8](https://github.com/statsbotco/cubejs-client/commit/19d5cd8)), closes [#42](https://github.com/statsbotco/cubejs-client/issues/42)





## [0.3.5-alpha.0](https://github.com/statsbotco/cubejs-client/compare/v0.3.5...v0.3.5-alpha.0) (2019-03-12)

**Note:** Version bump only for package @cubejs-backend/api-gateway
