/* eslint-disable camelcase */
import { Cast } from 'src/dataset';

export type Fixture = {
  cast: Cast,
  cube: {
    environment: {
      [key: string]: string,
    },
    volumes: string[],
    ports: string[],
    depends_on: string[],
    links: string[],
  },
  data?: {
    image: string,
    environment: {
      [key: string]: string,
    },
    volumes: string[],
    ports: string[],
    [key: string]: any,
  },
  preAggregations?: {
    Products: [{ name: string, [prop: string]: unknown }],
    Customers: [{ name: string, [prop: string]: unknown }],
    ECommerce: [{ name: string, [prop: string]: unknown }],
  },
  skip?: string[],
};
