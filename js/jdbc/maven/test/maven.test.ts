import * as fs from 'fs';
import path from 'path';

import { generateXml, resolveDependencies } from '../src/maven';

describe('Maven tests', () => {
  describe('generateXml', () => {
    it('project with hive', async () => {
      jest.setTimeout(10 * 1000);

      expect(
        generateXml([
          {
            groupId: 'org.apache.hive',
            artifactId: 'hive-jdbc',
            version: '2.3.5'
          }
        ])
      ).toEqual(
        fs.readFileSync(path.resolve(__dirname, '../../test/fixtures/generate-xml-1.xml'), { encoding: 'utf-8' }).trimEnd()
      );
    });
  });

  describe('resolve (download deps)', () => {
    it('project with hive', async () => {
      jest.setTimeout(10 * 1000);

      await resolveDependencies(
        [
          {
            groupId: 'org.apache.hive',
            artifactId: 'hive-jdbc',
            version: '2.3.5'
          }
        ],
        {
          showOutput: true,
        }
      );
    });
  });
});
