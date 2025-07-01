import { BaseQuery, PostgresQuery } from '../../../src/adapter';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('Views Join Order', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
cube(\`fact\`, {
  sql: \`SELECT 1 as id, 1 as id_product, 10 as quantity\`,
  dimensions: {
    id: {
      sql: 'id',
      type: 'number',
      primary_key: true,
    },
    id_product: {
      sql: 'id_product',
      type: 'number',
    },
  },
  measures: {
    quantity: {
      sql: 'quantity',
      type: 'sum',
    },
  },
  joins: {
    product: {
      relationship: 'many_to_one',
      sql: \`\${CUBE}.id_product = \${product.id_product}\`,
    },
  },
});

cube(\`product\`, {
  sql: \`SELECT 1 as id_product, 1 as id_sub_category, 1 as id_model, 'Product'::text as description\`,
  dimensions: {
    id_product: {
      sql: 'id_product',
      type: 'number',
      primary_key: true,
    },
    description: {
      sql: 'description',
      type: 'number',
    },
  },
  joins: {
    model: {
      relationship: 'many_to_one',
      sql: \`\${CUBE}.id_model = \${model.id_model}\`,
    },
    sub_category: {
      relationship: 'many_to_one',
      sql: \`\${CUBE}.id_sub_category = \${sub_category.id_sub_category}\`,
    },
  },
});

cube(\`model\`, {
  sql: \`SELECT 1 as id_model, 1 as id_brand, 'Model'::text as description\`,
  dimensions: {
    id_model: {
      sql: 'id_model',
      type: 'number',
      primary_key: true,
    },
    description: {
      sql: 'description',
      type: 'number',
    },
  },
  joins: {
    brand: {
      relationship: 'many_to_one',
      sql: \`\${CUBE}.id_brand = \${brand.id_brand}\`,
    },
  },
});

cube(\`brand\`, {
  sql: \`SELECT 1 as id_brand, 'Brand'::text as description\`,
  dimensions: {
    id_brand: {
      sql: 'id_brand',
      type: 'number',
      primary_key: true,
    },
    description: {
      sql: 'description',
      type: 'number',
    },
  },
});

cube(\`sub_category\`, {
  sql: \`SELECT 1 as id_sub_category, 1 as id_category, 'Sub Category'::text as description\`,
  dimensions: {
    id_sub_category: {
      sql: 'id_sub_category',
      type: 'number',
      primary_key: true,
    },
    description: {
      sql: 'description',
      type: 'number',
    },
  },
  joins: {
    category: {
      relationship: 'many_to_one',
      sql: \`\${CUBE}.id_category = \${category.id_category}\`,
    },
  },
});

cube(\`category\`, {
  sql: \`SELECT 1 as id_category, 'Category'::text as description\`,
  dimensions: {
    id_category: {
      sql: 'id_category',
      type: 'number',
      primary_key: true,
    },
    description: {
      sql: 'description',
      type: 'number',
    },
  },
});

view(\`Product_Stock\`, {
  public: true,
  cubes: [
    {
      join_path: fact,
      includes: ['quantity'],
    },
    {
      join_path: fact.product,
      includes: [
        {
          name: 'description',
          alias: 'product',
        },
      ],
    },
    {
      join_path: fact.product.sub_category,
      includes: [
        {
          name: 'description',
          alias: 'sub_category',
        },
      ],
    },
    {
      join_path: fact.product.sub_category.category,
      includes: [
        {
          name: 'description',
          alias: 'category',
        },
      ],
    },
    {
      join_path: fact.product.model,
      includes: [
        {
          name: 'description',
          alias: 'model',
        },
      ],
    },
    {
      join_path: fact.product.model.brand,
      includes: [
        {
          name: 'description',
          alias: 'brand',
        },
      ],
    },
  ],
});
    `);

  async function runQueryTest(q: any, expectedResult: any, additionalTest?: (query: BaseQuery) => any) {
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, { ...q, timezone: 'UTC', preAggregationsSchema: '' });

    console.log(query.buildSqlAndParams());

    console.log(query.cacheKeyQueries());

    const res = await dbRunner.evaluateQueryWithPreAggregations(query);
    console.log(JSON.stringify(res));

    if (additionalTest) {
      additionalTest(query);
    }

    expect(res).toEqual(
      expectedResult
    );
  }

  it('join order', async () => runQueryTest({
    measures: ['Product_Stock.quantity'],
    dimensions: [
      'Product_Stock.sub_category',
      'Product_Stock.brand'
    ],
    order: [{ id: 'Product_Stock.quantity' }]
  }, [{
    product__stock__sub_category: 'Sub Category',
    product__stock__brand: 'Brand',
    product__stock__quantity: '10',
  }]));
});
