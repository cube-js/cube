/* eslint-disable camelcase */
import { Cast } from 'src/dataset';

export type Fixture = {
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
  cast: Cast,
  tables: {
    products: string,
    customers: string,
    ecommerce: string,
  },
  preAggregations?: {
    Products: [{ name: string, [prop: string]: unknown }],
    Customers: [{ name: string, [prop: string]: unknown }],
    ECommerce: [{ name: string, [prop: string]: unknown }],
  },
  skip?: string[],
};
