/* eslint-disable camelcase */
import { Cast } from './Cast';

export type Fixture = {
  extendedEnvs: {
    [key: string]: any
  },
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
    [table: string]: string,
  },
  preAggregations?: {
    [cube: string]: [{ name: string, [prop: string]: unknown }],
  },
  skip?: string[],
  tesseractSkip?: string[],
};
