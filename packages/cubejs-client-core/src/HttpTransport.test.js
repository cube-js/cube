/* eslint-disable import/first */
/* eslint-disable import/newline-after-import */
/* globals describe,test,expect,jest,afterEach */
import '@babel/runtime/regenerator';
jest.mock('cross-fetch');
import fetch from 'cross-fetch';
import HttpTransport from './HttpTransport';

describe('HttpTransport', () => {
  const apiUrl = 'http://localhost:3000/cubejs-api/v1';
  const query = {
    measures: ['Orders.count'],
    dimensions: ['Users.country']
  };
  const queryUrlEncoded = '%7B%22measures%22%3A%5B%22Orders.count%22%5D%2C%22dimensions%22%3A%5B%22Users.country%22%5D%7D';

  afterEach(() => {
    fetch.mockClear();
  });

  test('it serializes the query object and sends it in the query string', async () => {
    const transport = new HttpTransport({
      authorization: 'token',
      apiUrl,
    });
    const req = transport.request('load', { query });
    await req.subscribe(() => { });
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(fetch).toHaveBeenCalledWith(`${apiUrl}/load?query=${queryUrlEncoded}`, {
      headers: {
        Authorization: 'token',
      }
    });
  });

  test('it passes extra headers and serializes extra params', async () => {
    const extraParams = { foo: 'bar' };
    const serializedExtraParams = encodeURIComponent(JSON.stringify(extraParams));
    const transport = new HttpTransport({
      authorization: 'token',
      apiUrl,
      headers: {
        'X-Extra-Header': '42'
      }
    });
    const req = transport.request('meta', { extraParams });
    await req.subscribe(() => { });
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(fetch).toHaveBeenCalledWith(`${apiUrl}/meta?extraParams=${serializedExtraParams}`, {
      headers: {
        Authorization: 'token',
        'X-Extra-Header': '42'
      }
    });
  });
});
