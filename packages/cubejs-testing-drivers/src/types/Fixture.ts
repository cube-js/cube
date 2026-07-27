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
  // Extra data-plane compose services for drivers whose backend is not a single
  // container (e.g. Apache Pinot: zookeeper + controller + broker + server). Each
  // entry is spread verbatim into the generated docker-compose (see getComposePath).
  services?: {
    [name: string]: {
      image: string,
      [key: string]: any,
    },
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
