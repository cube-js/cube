import { prepareYamlCompiler } from './PrepareCompiler';

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
  
##############################

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

      - join_path: customers.countries
        includes: "*"
        split: true

  - name: orders_view
    cubes:
      - join_path: customers
        prefix: true
        includes: "*"
 
      - join_path: orders
        split: true
        includes: "*"

      - join_path: customers.countries
        split: true
        includes: "*"  
  `,
  },
];

describe('Split views', () => {
  const compilers = /** @type Compilers */ prepareYamlCompiler(model);

  it('Finds split joins', async () => {
    await compilers.compiler.compile();

    const { cubes } = compilers.metaTransformer;
      
    const spView = cubes.find((cube) => cube.config.name === 'sp');
    expect(spView.config.splitJoins).toEqual(
      [
        {
          relationship: 'belongsTo',
          to: 'sp_countries',
        }
      ]
    );
      
    const orders = cubes.find((cube) => cube.config.name === 'orders_view_orders');
      
    expect(orders.config.splitJoins).toEqual([
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
    expect(customers.config.splitJoins).toEqual(
      [
        {
          relationship: 'belongsTo',
          to: 'orders_view_countries',
        }
      ]
    );
  });
});
