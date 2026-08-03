import { prepareYamlCompiler } from './PrepareCompiler';
import { createSchemaYaml } from './utils';

describe('Dimension key property', () => {
  it('resolves key reference correctly', async () => {
    const { compiler, cubeEvaluator, metaTransformer } = prepareYamlCompiler(
      createSchemaYaml({
        cubes: [{
          name: 'Products',
          sql_table: 'products',
          dimensions: [
            {
              name: 'id',
              sql: 'id',
              type: 'number',
              primary_key: true,
            },
            {
              name: 'name',
              sql: 'name',
              type: 'string',
              key: 'CUBE.id',
            },
          ],
        }],
      })
    );

    await compiler.compile();

    const productsDef = cubeEvaluator.getCubeDefinition('Products') as any;
    expect(productsDef.dimensions.name.keyReference).toBe('Products.id');

    // Check meta API exposure
    const productsMeta = metaTransformer.cubes
      .map((def) => def.config)
      .find((def) => def.name === 'Products');
    expect(productsMeta).toBeDefined();

    const nameDimension = productsMeta?.dimensions.find(
      (d) => d.name === 'Products.name'
    );
    expect(nameDimension?.key).toBe('Products.id');

    // id dimension should not have a key
    const idDimension = productsMeta?.dimensions.find(
      (d) => d.name === 'Products.id'
    );
    expect(idDimension?.key).toBeUndefined();
  });

  it('rejects cross-cube key references', async () => {
    const { compiler } = prepareYamlCompiler(
      createSchemaYaml({
        cubes: [
          {
            name: 'Products',
            sql_table: 'products',
            dimensions: [{
              name: 'id',
              sql: 'id',
              type: 'number',
              primary_key: true,
            }],
          },
          {
            name: 'Orders',
            sql_table: 'orders',
            dimensions: [{
              name: 'product_name',
              sql: 'product_name',
              type: 'string',
              key: 'Products.id',
            }],
          },
        ],
      })
    );

    await expect(compiler.compile()).rejects.toThrow(
      /key that references dimension.*from a different cube/
    );
  });

  it('rejects nested keys', async () => {
    const { compiler } = prepareYamlCompiler(
      createSchemaYaml({
        cubes: [{
          name: 'Products',
          sql_table: 'products',
          dimensions: [
            {
              name: 'id',
              sql: 'id',
              type: 'number',
              primary_key: true,
            },
            {
              name: 'sku',
              sql: 'sku',
              type: 'string',
              key: 'CUBE.id',
            },
            {
              name: 'name',
              sql: 'name',
              type: 'string',
              key: 'CUBE.sku',
            },
          ],
        }],
      })
    );

    await expect(compiler.compile()).rejects.toThrow(
      /Nested keys are not allowed/
    );
  });

  it('rejects key reference to non-existent dimension', async () => {
    const { compiler } = prepareYamlCompiler(
      createSchemaYaml({
        cubes: [{
          name: 'Products',
          sql_table: 'products',
          dimensions: [{
            name: 'name',
            sql: 'name',
            type: 'string',
            key: 'CUBE.nonExistent',
          }],
        }],
      })
    );

    // The error comes from symbol resolution before our validation
    await expect(compiler.compile()).rejects.toThrow(
      /cannot be resolved/
    );
  });

  it('key property is inherited in views', async () => {
    const { compiler, cubeEvaluator, metaTransformer } = prepareYamlCompiler(
      createSchemaYaml({
        cubes: [{
          name: 'Products',
          sql_table: 'products',
          dimensions: [
            {
              name: 'id',
              sql: 'id',
              type: 'number',
              primary_key: true,
            },
            {
              name: 'name',
              sql: 'name',
              type: 'string',
              key: 'CUBE.id',
            },
          ],
        }],
        views: [{
          name: 'ProductsView',
          cubes: [{
            join_path: 'Products',
            includes: ['id', 'name'],
          }],
        }],
      })
    );

    await compiler.compile();

    // Check that key reference is transformed to view naming
    const viewDef = cubeEvaluator.getCubeDefinition('ProductsView') as any;
    expect(viewDef.dimensions.name.keyReference).toBe('ProductsView.id');

    // Check meta API
    const viewMeta = metaTransformer.cubes
      .map((def) => def.config)
      .find((def) => def.name === 'ProductsView');
    expect(viewMeta).toBeDefined();

    const nameDim = viewMeta?.dimensions.find(
      (d) => d.name === 'ProductsView.name'
    );
    expect(nameDim?.key).toBe('ProductsView.id');
  });

  it('key property with prefixed includes', async () => {
    const { compiler, cubeEvaluator, metaTransformer } = prepareYamlCompiler(
      createSchemaYaml({
        cubes: [{
          name: 'Products',
          sql_table: 'products',
          dimensions: [
            {
              name: 'id',
              sql: 'id',
              type: 'number',
              primary_key: true,
            },
            {
              name: 'name',
              sql: 'name',
              type: 'string',
              key: 'CUBE.id',
            },
          ],
        }],
        views: [{
          name: 'AllDataView',
          cubes: [{
            join_path: 'Products',
            includes: '*',
            prefix: true,
          }],
        }],
      })
    );

    await compiler.compile();

    const viewDef = cubeEvaluator.getCubeDefinition('AllDataView') as any;
    expect(viewDef.dimensions.Products_name.keyReference).toBe('AllDataView.Products_id');

    const viewMeta = metaTransformer.cubes
      .map((def) => def.config)
      .find((def) => def.name === 'AllDataView');
    const nameDim = viewMeta?.dimensions.find(
      (d) => d.name === 'AllDataView.Products_name'
    );
    expect(nameDim?.key).toBe('AllDataView.Products_id');
  });

  it('key property with aliased key dimension', async () => {
    const { compiler, cubeEvaluator, metaTransformer } = prepareYamlCompiler(
      createSchemaYaml({
        cubes: [{
          name: 'Products',
          sql_table: 'products',
          dimensions: [
            {
              name: 'id',
              sql: 'id',
              type: 'number',
              primary_key: true,
            },
            {
              name: 'name',
              sql: 'name',
              type: 'string',
              key: 'CUBE.id',
            },
          ],
        }],
        views: [{
          name: 'ProductsView',
          cubes: [{
            join_path: 'Products',
            includes: [
              { name: 'id', alias: 'product_id' },
              'name',
            ],
          }],
        }],
      })
    );

    await compiler.compile();

    const viewDef = cubeEvaluator.getCubeDefinition('ProductsView') as any;
    expect(viewDef.dimensions.name.keyReference).toBe('ProductsView.product_id');

    const viewMeta = metaTransformer.cubes
      .map((def) => def.config)
      .find((def) => def.name === 'ProductsView');
    const nameDim = viewMeta?.dimensions.find(
      (d) => d.name === 'ProductsView.name'
    );
    expect(nameDim?.key).toBe('ProductsView.product_id');
  });

  it('rejects view that excludes key dimension', async () => {
    const { compiler } = prepareYamlCompiler(
      createSchemaYaml({
        cubes: [{
          name: 'Products',
          sql_table: 'products',
          dimensions: [
            {
              name: 'id',
              sql: 'id',
              type: 'number',
              primary_key: true,
            },
            {
              name: 'name',
              sql: 'name',
              type: 'string',
              key: 'CUBE.id',
            },
          ],
        }],
        views: [{
          name: 'ProductsView',
          cubes: [{
            join_path: 'Products',
            includes: '*',
            excludes: ['id'],
          }],
        }],
      })
    );

    await expect(compiler.compile()).rejects.toThrow(
      /key dimension is not included in view/
    );
  });
});
