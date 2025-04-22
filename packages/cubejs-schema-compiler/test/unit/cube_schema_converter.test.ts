import { CubePreAggregationConverter, CubeSchemaConverter } from '../../src';
import {
  createCubeSchema,
  createCubeSchemaWithCustomGranularities,
  createCubeSchemaYaml, createECommerceSchema,
  createSchemaYaml
} from './utils';

const repo = {
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([
    { fileName: 'single_cube_no_preaggs.js', content: createCubeSchema({ name: 'single_cube' }) },
    { fileName: 'orders_and_users.js', content: createCubeSchemaWithCustomGranularities('js_orders') },
    { fileName: 'single_cube.yaml', content: createCubeSchemaYaml({ name: 'yml_orders', sqlTable: 'yml_orders' }) },
    { fileName: 'multi_ecom.yaml', content: createSchemaYaml(createECommerceSchema()) },
    { fileName: 'empty1.yaml', content: '       ' },
    { fileName: 'empty2.yaml', content: 'string     ' },
    { fileName: 'empty3.yaml', content: 'cubes: string     ' },
    { fileName: 'empty4.yaml', content: '# just comment  ' },
  ])
};

describe('CubeSchemaConverter', () => {
  it('converts all schema repository models (no changes, without additional converters)', async () => {
    const schemaConverter = new CubeSchemaConverter(repo, []);
    await schemaConverter.generate();
    const regeneratedFiles = schemaConverter.getSourceFiles();
    regeneratedFiles.forEach((regeneratedFile) => {
      expect(regeneratedFile.source).toMatchSnapshot(regeneratedFile.fileName);
    });
  });

  it('adds a pre-aggregation to JS model using CubePreAggregationConverter', async () => {
    const cubeName = 'yml_orders';
    const preAggregationName = 'yml_orders_main';
    const code = `
name: main
measures:
  - yml_orders.count
timeDimension: yml_orders.createdAt
granularity: day
`;

    const schemaConverter = new CubeSchemaConverter(repo, [new CubePreAggregationConverter({
      cubeName,
      preAggregationName,
      code
    })]);

    await schemaConverter.generate(cubeName);
    const regeneratedFiles = schemaConverter.getSourceFiles();
    regeneratedFiles.forEach((regeneratedFile) => {
      expect(regeneratedFile.source).toMatchSnapshot(regeneratedFile.fileName);
    });
  });
});
