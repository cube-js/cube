<p align="center"><a href="https://cube.dev"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[Website](https://cube.dev) • [Docs](https://cube.dev/docs) • [Blog](https://cube.dev/blog) • [Slack](https://slack.cube.dev) • [Discourse](https://forum.cube.dev/) • [Twitter](https://twitter.com/thecubejs)

[![npm version](https://badge.fury.io/js/%40cubejs-backend%2Fserver.svg)](https://badge.fury.io/js/%40cubejs-backend%2Fserver)
[![GitHub Actions](https://github.com/cube-js/cube.js/workflows/Build/badge.svg)](https://github.com/cube-js/cube.js/actions?query=workflow%3ABuild+branch%3Amaster)

# Cube.js SAP HANA Database Driver
SAP HANA driver.
Works with SAP HANA Cloud and On-Premise HANA (Tested on HANA SP05)

[Learn more](https://github.com/cube-js/cube.js#getting-started)

## How to use
In your cube project, you can use `.env` or `cube.js` to config the HANA data source

In cube.js
```ts
module.exports = {
    driverFactory: ({ dataSource }) => {
        return {
            type: 'hana',
            serverNode: '<hana-instance-id>.hana.prod-us20.hanacloud.ondemand.com:443',
            uid: '<username>',
            pwd: '<password>',
        }
    }
};
```
Or in local .env file

```properties
CUBEJS_DB_TYPE=hana
CUBEJS_DB_HOST=<hana-instance-id>.hana.prod-us20.hanacloud.ondemand.com:443
CUBEJS_DB_USER=<username>
CUBEJS_DB_PASS=<password>
```

### Support

This driver has been contributed by Cube.js community member [Ethan Zhang](https://github.com/zhjuncai). This package is **community supported** and should be used at your own risk. 

### License

Cube.js SAP HANA Database Driver is [Apache 2.0 licensed](./LICENSE).
