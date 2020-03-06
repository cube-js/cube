/* globals describe,test,expect,jest */

const request = require('supertest');
const express = require('express');
const ApiGateway = require('./index');

const compilerApi = jest.fn().mockImplementation(() => ({
  async getSql() {
    return {
      sql: ['SELECT * FROM test', []],
      aliasNameToMember: {
        foo__bar: 'Foo.bar',
        foo__time: 'Foo.time'
      }
    };
  },

  async metaConfig() {
    return [{
      config: {
        name: 'Foo',
        measures: [{
          name: 'Foo.bar'
        }],
        dimensions: [{
          name: 'Foo.id'
        }, {
          name: 'Foo.time'
        }]
      }
    }];
  }
}));
const adapterApi = jest.fn().mockImplementation(() => ({
  async executeQuery() {
    return {
      data: [{ foo__bar: 42 }]
    };
  }
}));
const logger = (type, message) => console.log({ type, ...message });

describe(`API Gateway`, () => {
  process.env.NODE_ENV = 'production';
  const apiGateway = new ApiGateway('secret', compilerApi, adapterApi, logger);
  process.env.NODE_ENV = null;
  const app = express();
  apiGateway.initApp(app);

  test(`working token`, async () => {
    const res = await request(app)
      .get('/cubejs-api/v1/load?query={"measures":["Foo.bar"]}')
      .set('Authorization', 'foo')
      .expect(403);
    expect(res.body && res.body.error).toStrictEqual('Invalid token');
  });

  test(`requires auth`, async () => {
    const res = await request(app)
      .get('/cubejs-api/v1/load?query={"measures":["Foo.bar"]}')
      .expect(403);
    expect(res.body && res.body.error).toStrictEqual("Authorization header isn't set");
  });

  test(`passes correct token`, async () => {
    const res = await request(app)
      .get('/cubejs-api/v1/load?query={}')
      .set('Authorization', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M')
      .expect(400);
    expect(res.body && res.body.error).toStrictEqual("Query should contain either measures, dimensions or timeDimensions with granularities in order to be valid");
  });

  test(`null filter values`, async () => {
    const res = await request(app)
      .get('/cubejs-api/v1/load?query={"measures":["Foo.bar"],"filters":[{"dimension":"Foo.id","operator":"equals","values":[null]}]}')
      .set('Authorization', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M')
      .expect(200);
    console.log(res.body);
    expect(res.body && res.body.data).toStrictEqual([{ 'Foo.bar': 42 }]);
  });

  test(`date range padding`, async () => {
    const res = await request(app)
      .get('/cubejs-api/v1/load?query={"measures":["Foo.bar"],"timeDimensions":[{"dimension":"Foo.time","granularity":"hour","dateRange":["2020-01-01","2020-01-01"]}]}')
      .set('Authorization', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.t-IDcSemACt8x4iTMCda8Yhe3iZaWbvV5XKSTbuAn0M')
      .expect(200);
    console.log(res.body);
    expect(res.body.query.timeDimensions[0].dateRange).toStrictEqual([
      "2020-01-01T00:00:00.000",
      "2020-01-01T23:59:59.999"
    ]);
  });
});
