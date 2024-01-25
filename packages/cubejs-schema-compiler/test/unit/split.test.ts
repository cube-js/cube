import moment from 'moment-timezone';
import { BaseQuery, PostgresQuery, MssqlQuery, UserError } from '../../src';
import { prepareCompiler, prepareYamlCompiler } from './PrepareCompiler';
import {
  createCubeSchema,
  createCubeSchemaYaml,
  createJoinedCubesSchema,
  createSchemaYaml,
} from './utils';

// one_to_one was known as has_one or hasOne
// one_to_many was known as has_many or hasMany
// many_to_one was known as belongs_to or belongsTo

const model = [
  {
    fileName: 'all.yml',
    content: `cubes:
    - name: orders
      sql_table: orders
  
      joins:
        - name: customers
          sql: "{CUBE}.customer_id = {customers}.id"
          relationship: many_to_one
   
      measures:
        - name: count
          sql: id
          type: count
   
        - name: total_revenue
          sql: revenue
          type: sum
   
      dimensions:
        - name: id
          sql: id
          type: number
          primary_key: true
   
        - name: customer_id
          sql: customer_id
          type: number
  
   ##################
    - name: customers
      sql_table: customers
  
      joins:

          
        - name: countries
          sql: "{CUBE}.country_id = {countries}.id"
          relationship: many_to_one
  
      dimensions:
        - name: genger
          sql: "'male'"
          type: string
  
  
  ##############################
    - name: countries
      sql_table: countries
  
      joins: []
  
      measures:
        - name: count
          sql: id
          type: count
   
      dimensions:
        - name: id
          sql: id
          type: string
          primary_key: true
  
        - name: country
          sql: country
          type: string`,
  },
  {
    fileName: 'view.yml',
    content: `
views:   
  - name: sp 
    cubes:
      - join_path: orders
        includes: "*"
        prefix: true
        # split: true

      - join_path: customers.countries
        includes: "*"
        # prefix: true
        split: true

  - name: orders_view
    cubes:
      - join_path: customers
        prefix: true
        includes: "*"
 
      - join_path: orders
        # split: true
        split: true
        includes: "*"

      - join_path: customers.countries
        # split: true
        split: true
        includes: "*"  
  `,
  },
];

describe('Split views', () => {
  describe('Common - Yaml - syntax sugar', () => {
    const compilers = /** @type Compilers */ prepareYamlCompiler(model);

    it('Simple query', async () => {
      const res = await compilers.compiler.compile();

      // const j0 = compilers.joinGraph.buildJoin([['customers', 'countries'], 'orders']);
      // console.log('>>>', 'jjj0', JSON.stringify(j0, null, 2));
      // const j = compilers.joinGraph.buildJoin(['orders', ['customers', 'countries']]);
      // console.log('>>>', 'jjj', JSON.stringify(j, null, 2));

      const { cubes } = compilers.metaTransformer;
      
      console.log('>>>', JSON.stringify(cubes));
      // console.log('>>>', JSON.stringify(compilers.cubeEvaluator.cubeList));

      const spView = cubes.find((cube) => cube.config.name === 'sp');
      expect(spView.config.join).toEqual(
        [
          {
            relationship: 'belongsTo',
            to: 'sp_countries',
          }
        ]
      );
      
      const orders = cubes.find((cube) => cube.config.name === 'orders_view_orders');
      
      expect(orders.config.join).toEqual([
        {
          // many_to_one
          relationship: 'belongsTo',
          // directly to customers
          to: 'orders_view',
        },
        {
          // many_to_one
          relationship: 'belongsTo',
          // via customers
          to: 'orders_view_countries',
        }
      ]);
      
      const customers = cubes.find((cube) => cube.config.name === 'orders_view');
      expect(customers.config.join).toEqual(
        [
          {
            relationship: 'belongsTo',
            to: 'orders_view_countries',
          }
        ]
      );
    });
  });
});
