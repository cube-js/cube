/* eslint-disable camelcase */

import fs from 'fs-extra';
import path from 'path';
import * as YAML from 'yaml';
import { getFixtures } from './getFixtures';

/**
 * Returns schema yaml file by data source type.
 */
export function getSchemaPath(type: string): [path: string, file: string] {
  const _path = path.resolve(process.cwd(), './.temp/schemas');
  const _file = 'ecommerce.yaml';
  const { preAggregations } = getFixtures(type);
  const _content = JSON.parse(fs.readFileSync(
    path.resolve(process.cwd(), './fixtures/_schemas.json'),
    'utf-8'
  ));
  _content.cubes.forEach(
    (cube: {
      name: 'Products' | 'Customers' | 'ECommerce',
      [prop: string]: unknown
    }) => {
      const pre_aggregations: {
        [x: string]: unknown;
        name: string;
      }[] = [];
      if (preAggregations && preAggregations[cube.name]) {
        const preaggs = preAggregations[cube.name];
        preaggs.forEach((pa) => {
          if (pa.refresh_key) delete pa.refresh_key;
          pa.scheduled_refresh = false;

          const ext = { ...pa };
          ext.external = true;
          ext.name = `${pa.name}External`;
          pre_aggregations.push(ext);

          const int = { ...pa };
          int.external = false;
          int.name = `${pa.name}Internal`;
          pre_aggregations.push(int);
        });
        cube.pre_aggregations = pre_aggregations;
      }
    }
  );

  fs.writeFileSync(
    path.resolve(_path, _file),
    YAML.stringify(_content),
  );
  return [_path, _file];
}
