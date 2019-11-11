/* globals describe,test,expect,jest */

const request = require('supertest');
const express = require('express');
const ApiGateway = require('./index');

const compilerApi = jest.fn().mockImplementation(() => ({
  getSql() {
    return 'SELECT * FROM test';
  },

  metaConfig() {
    return [];
  }
}));
const adapterApi = jest.fn();
const logger = jest.fn();

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
});
