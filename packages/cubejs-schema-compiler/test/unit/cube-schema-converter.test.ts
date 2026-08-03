import { CubePreAggregationConverter, CubeSchemaConverter } from '../../src';
import {
  createCubeSchema,
  createCubeSchemaWithCustomGranularitiesAndTimeShift,
  createCubeSchemaYaml,
  createECommerceSchema,
  createSchemaYaml
} from './utils';

const repo = {
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([
    { fileName: 'single_cube_no_preaggs.js', content: createCubeSchema({ name: 'single_cube' }) },
    { fileName: 'single_cube_with_preaggs.js',
      content: createCubeSchema({
        name: 'single_preagg_cube',
        preAggregations: 'existing_pre_agg: {\n  measures: [\n    single_preagg_cube.count\n  ],\n  timeDimension: single_preagg_cube.createdAt,\n  granularity: `month`\n}'
      })
    },
    { fileName: 'orders_and_users.js', content: createCubeSchemaWithCustomGranularitiesAndTimeShift('js_orders') },
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

  it('throws error if can not parse source schema js file (syntax error)', async () => {
    const lRepo = {
      localPath: () => __dirname,
      dataSchemaFiles: () => Promise.resolve([
        { fileName: 'model.js', content: 'cube(\'name, {\n        description: \'test cube from createCubeSchema\',' }
      ])
    };
    const schemaConverter = new CubeSchemaConverter(lRepo, []);

    try {
      await schemaConverter.generate();
      throw new Error('should throw earlier');
    } catch (e: any) {
      expect(e.toString()).toMatch(/Syntax error during 'model.js' parsing/);
    }
  });

  it('throws error if can not parse source schema js file (no cube name)', async () => {
    const lRepo = {
      localPath: () => __dirname,
      dataSchemaFiles: () => Promise.resolve([
        { fileName: 'model.js', content: 'cube({}, {\n        description: \'test cube from createCubeSchema\'});' }
      ])
    };
    const schemaConverter = new CubeSchemaConverter(lRepo, []);

    try {
      await schemaConverter.generate();
      throw new Error('should throw earlier');
    } catch (e: any) {
      expect(e.toString()).toMatch(/Error parsing model.js/);
    }
  });

  it('adds a pre-aggregation to YAML model (w/o pre-agg) using CubePreAggregationConverter', async () => {
    const cubeName = 'yml_orders';
    const preAggregationName = 'yml_orders_main';
    const code = `
name: yml_orders_main
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

  it('adds a pre-aggregation to JS model (w/o pre-agg) using CubePreAggregationConverter', async () => {
    const cubeName = 'js_orders';
    const preAggregationName = 'js_orders_main';
    const code = `{
  measures: [
    js_orders.count
  ],
  timeDimension: js_orders.createdAt,
  granularity: \`day\`
}
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

  it('adds a pre-aggregation to JS model (with empty pre-aggs property) using CubePreAggregationConverter', async () => {
    const cubeName = 'single_cube';
    const preAggregationName = 'single_cube_main';
    const code = `{
  measures: [
    js_orders.count
  ],
  timeDimension: js_orders.createdAt,
  granularity: \`day\`
}
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

  it('adds a pre-aggregation to JS model (with existing pre-aggs) using CubePreAggregationConverter', async () => {
    const cubeName = 'single_preagg_cube';
    const preAggregationName = 'single_preagg_cube_main';
    const code = `{
  measures: [
    single_preagg_cube.count
  ],
  timeDimension: single_preagg_cube.createdAt,
  granularity: \`day\`
}
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

  it('adds a pre-aggregation to YAML model (with pre-aggs) using CubePreAggregationConverter', async () => {
    const cubeName = 'orders';
    const preAggregationName = 'orders_main';
    const code = `
name: orders_main
measures:
  - orders.count
timeDimension: orders.created_at
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

  it('throws error for malformed (not object) yaml pre-agg code', async () => {
    const cubeName = 'orders';
    const preAggregationName = 'orders_main';
    const code = `
- name: orders_main
  measures:
    - orders.count
  timeDimension: orders.created_at
  granularity: day
`;

    const schemaConverter = new CubeSchemaConverter(repo, [new CubePreAggregationConverter({
      cubeName,
      preAggregationName,
      code
    })]);

    try {
      await schemaConverter.generate(cubeName);
      throw new Error('should throw earlier');
    } catch (e: any) {
      expect(e.toString()).toMatch(/Pre-aggregation YAML must be a map\/object/);
    }
  });

  it('throws error if can not parse source schema yaml file (pre-aggs is not a map)', async () => {
    const lRepo = {
      localPath: () => __dirname,
      dataSchemaFiles: () => Promise.resolve([
        { fileName: 'model.yaml', content: `
    cubes:
      - name: orders
        sql_table: table
        pre_aggregations:
          name: pre-agg1
        ` }
      ])
    };

    const cubeName = 'orders';
    const preAggregationName = 'orders_main';
    const code = `
name: orders_main
measures:
  - orders.count
timeDimension: orders.created_at
granularity: day
`;

    const schemaConverter = new CubeSchemaConverter(lRepo, [new CubePreAggregationConverter({
      cubeName,
      preAggregationName,
      code
    })]);

    try {
      await schemaConverter.generate();
      throw new Error('should throw earlier');
    } catch (e: any) {
      expect(e.toString()).toMatch(/'pre_aggregations' must be a sequence/);
    }
  });

  it('throws error for malformed (not object) js pre-agg code', async () => {
    const cubeName = 'single_cube';
    const preAggregationName = 'orders_main';
    const code = `[{
  measures: [
    js_orders.count
  ],
  timeDimension: js_orders.createdAt,
  granularity: \`day\`
}]
`;

    const schemaConverter = new CubeSchemaConverter(repo, [new CubePreAggregationConverter({
      cubeName,
      preAggregationName,
      code
    })]);

    try {
      await schemaConverter.generate(cubeName);
      throw new Error('should throw earlier');
    } catch (e: any) {
      expect(e.toString()).toMatch(/Pre-aggregation definition is malformed/);
    }
  });

  it('throws error if pre-aggregation with the same name exists (yaml model)', async () => {
    const cubeName = 'orders';
    const preAggregationName = 'orders_by_day_with_day';
    const code = `
name: orders_by_day_with_day
measures:
  - orders.count
timeDimension: orders.created_at
granularity: day
`;

    const schemaConverter = new CubeSchemaConverter(repo, [new CubePreAggregationConverter({
      cubeName,
      preAggregationName,
      code
    })]);

    try {
      await schemaConverter.generate(cubeName);
      throw new Error('should throw earlier');
    } catch (e: any) {
      expect(e.toString()).toMatch(/Pre-aggregation 'orders_by_day_with_day' is already defined/);
    }
  });

  it('throws error if pre-aggregation with the same name exists (js model)', async () => {
    const cubeName = 'single_preagg_cube';
    const preAggregationName = 'existing_pre_agg';
    const code = `{
  measures: [
    single_preagg_cube.count
  ],
  timeDimension: single_preagg_cube.createdAt,
  granularity: \`day\`
}
`;

    const schemaConverter = new CubeSchemaConverter(repo, [new CubePreAggregationConverter({
      cubeName,
      preAggregationName,
      code
    })]);

    try {
      await schemaConverter.generate(cubeName);
      throw new Error('should throw earlier');
    } catch (e: any) {
      expect(e.toString()).toMatch(/Pre-aggregation 'existing_pre_agg' is already defined/);
    }
  });
});
