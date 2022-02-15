/* eslint-disable import/no-extraneous-dependencies */

import { jest, expect, beforeAll, afterAll } from '@jest/globals';
import cubejs, { CubejsApi } from '@cubejs-client/core';
import WebSocketTransport from '@cubejs-client/ws-transport';
import { BirdBox } from '../src';

export function createBirdBoxTestCase(name: string, entrypoint: () => Promise<BirdBox>) {
  describe(name, () => {
    jest.setTimeout(60 * 5 * 1000);

    const responses: unknown[] = [];
    let birdbox: BirdBox;
    let transport: WebSocketTransport;
    let http: CubejsApi;
    let ws: CubejsApi;

    beforeAll(async () => {
      try {
        birdbox = await entrypoint();
        transport = new WebSocketTransport({
          apiUrl: birdbox.configuration.apiUrl,
        });
        http = cubejs(async () => 'test', {
          apiUrl: birdbox.configuration.apiUrl,
        });
        ws = cubejs(async () => 'test', {
          apiUrl: birdbox.configuration.apiUrl,
          transport,
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

    it('http+responseFormat=default', async () => {
      const response = await http.load({
        dimensions: ['Orders.status'],
        measures: ['Orders.totalAmount'],
        limit: 2,
      });
      responses.push(response);
      expect(response.rawData()).toMatchSnapshot('result-type');
    });

    it('http+responseFormat=compact option#1', async () => {
      const response = await http.load({
        dimensions: ['Orders.status'],
        measures: ['Orders.totalAmount'],
        limit: 2,
        responseFormat: 'compact',
      });
      responses.push(response);
      expect(response.rawData()).toMatchSnapshot('result-type');
    });

    it('http+responseFormat=compact option#2', async () => {
      const response = await http.load(
        {
          dimensions: ['Orders.status'],
          measures: ['Orders.totalAmount'],
          limit: 2,
        },
        undefined,
        undefined,
        'compact',
      );
      responses.push(response);
      expect(response.rawData()).toMatchSnapshot('result-type');
    });

    it('http+responseFormat=compact option#1+2', async () => {
      const response = await http.load(
        {
          dimensions: ['Orders.status'],
          measures: ['Orders.totalAmount'],
          limit: 2,
          responseFormat: 'compact',
        },
        undefined,
        undefined,
        'compact',
      );
      responses.push(response);
      expect(response.rawData()).toMatchSnapshot('result-type');
    });

    it('ws+responseFormat=default', async () => {
      const response = await ws.load({
        dimensions: ['Orders.status'],
        measures: ['Orders.totalAmount'],
        limit: 2,
      });
      responses.push(response);
      expect(response.rawData()).toMatchSnapshot('result-type');
    });

    it('ws+responseFormat=compact option#1', async () => {
      const response = await ws.load({
        dimensions: ['Orders.status'],
        measures: ['Orders.totalAmount'],
        limit: 2,
        responseFormat: 'compact',
      });
      responses.push(response);
      expect(response.rawData()).toMatchSnapshot('result-type');
    });

    it('ws+responseFormat=compact option#2', async () => {
      const response = await ws.load(
        {
          dimensions: ['Orders.status'],
          measures: ['Orders.totalAmount'],
          limit: 2,
        },
        undefined,
        undefined,
        'compact',
      );
      responses.push(response);
      expect(response.rawData()).toMatchSnapshot('result-type');
    });

    it('ws+responseFormat=compact option#1+2', async () => {
      const response = await ws.load(
        {
          dimensions: ['Orders.status'],
          measures: ['Orders.totalAmount'],
          limit: 2,
          responseFormat: 'compact',
        },
        undefined,
        undefined,
        'compact',
      );
      responses.push(response);
      expect(response.rawData()).toMatchSnapshot('result-type');
    });

    it('responses', () => {
      // @ts-ignore
      expect(responses[0].rawData()).toEqual(responses[1].rawData());
      // @ts-ignore
      expect(responses[0].rawData()).toEqual(responses[2].rawData());
      // @ts-ignore
      expect(responses[0].rawData()).toEqual(responses[3].rawData());
      // @ts-ignore
      expect(responses[0].rawData()).toEqual(responses[4].rawData());
      // @ts-ignore
      expect(responses[0].rawData()).toEqual(responses[5].rawData());
      // @ts-ignore
      expect(responses[0].rawData()).toEqual(responses[6].rawData());
      // @ts-ignore
      expect(responses[0].rawData()).toEqual(responses[7].rawData());
    });
  });
}
