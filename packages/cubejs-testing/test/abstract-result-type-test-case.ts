/* eslint-disable import/no-extraneous-dependencies */

import { jest, expect, beforeAll, afterAll } from '@jest/globals';
import cubejs, { CubejsApi } from '@cubejs-client/core';
import WebSocketTransport from '@cubejs-client/ws-transport';
import { BirdBox } from '../src';

export function createBirdBoxTestCase(name: string, entrypoint: () => Promise<BirdBox>) {
  describe(name, () => {
    jest.setTimeout(60 * 5 * 1000);

    let birdbox: BirdBox;
    let transport: WebSocketTransport;
    let http: CubejsApi;
    let httpCompact: CubejsApi;
    let ws: CubejsApi;
    let wsCompact: CubejsApi;

    beforeAll(async () => {
      try {
        birdbox = await entrypoint();
        transport = new WebSocketTransport({
          apiUrl: birdbox.configuration.apiUrl,
        });
        http = cubejs(async () => 'test', {
          apiUrl: birdbox.configuration.apiUrl,
        });
        httpCompact = cubejs(async () => 'test', {
          apiUrl: birdbox.configuration.apiUrl,
          resType: 'compact',
        });
        ws = cubejs(async () => 'test', {
          apiUrl: birdbox.configuration.apiUrl,
          transport,
        });
        wsCompact = cubejs(async () => 'test', {
          apiUrl: birdbox.configuration.apiUrl,
          transport,
          resType: 'compact',
        });
      } catch (e) {
        console.log(e);
        process.exit(1);
      }
    });

    afterAll(async () => {
      await transport.close();
      await birdbox.stop();
    });

    it('HTTP Transport', async () => {
      const response = await http.load({
        dimensions: ['Orders.status'],
        measures: ['Orders.totalAmount'],
        limit: 2
      });
      expect(response.rawData()).toMatchSnapshot('result-type');
    });

    it('HTTP Compact Transport', async () => {
      const response = await httpCompact.load({
        dimensions: ['Orders.status'],
        measures: ['Orders.totalAmount'],
        limit: 2
      });
      expect(response.rawData()).toMatchSnapshot('result-type');
    });

    it('WS Transport', async () => {
      const response = await ws.load({
        dimensions: ['Orders.status'],
        measures: ['Orders.totalAmount'],
        limit: 2
      });
      expect(response.rawData()).toMatchSnapshot('result-type');
    });

    it('WS Compact Transport', async () => {
      const response = await wsCompact.load({
        dimensions: ['Orders.status'],
        measures: ['Orders.totalAmount'],
        limit: 2
      });
      expect(response.rawData()).toMatchSnapshot('result-type');
    });
  });
}
