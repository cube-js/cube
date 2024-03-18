import YAML from 'js-yaml';

interface CreateCubeSchemaOptions {
  name: string,
  publicly?: boolean,
  shown?: boolean,
  sqlTable?: string,
  refreshKey?: string,
  preAggregations?: string,
  joins?: string,
}

export function createCubeSchema({ name, refreshKey = '', preAggregations = '', sqlTable, publicly, shown, joins }: CreateCubeSchemaOptions): string {
  return ` 
    cube('${name}', {
        ${sqlTable ? `sqlTable: \`${sqlTable}\`` : 'sql: `select * from cards`'},

        ${publicly !== undefined ? `public: ${publicly},` : ''}
        ${shown !== undefined ? `shown: ${shown},` : ''}
        ${refreshKey}
        ${joins ? `joins: ${joins},` : ''}

        measures: {
          count: {
            type: 'count'
          },
          sum: {
            sql: \`amount\`,
            type: \`sum\`
          },
          max: {
            sql: \`amount\`,
            type: \`max\`
          },
          min: {
            sql: \`amount\`,
            type: \`min\`
          }
        },

        dimensions: {
          id: {
            type: 'number',
            sql: 'id',
            primaryKey: true
          },
          type: {
            type: 'string',
            sql: 'type'
          },
          createdAt: {
            type: 'time',
            sql: 'created_at'
          },
          location: {
            type: 'string',
            sql: 'location'
          }
        },

        segments: {
          sfUsers: {
            sql: \`\${CUBE}.location = 'San Francisco'\`
          }
        },

        preAggregations: {
            ${preAggregations}
        }
      }) 
  `;
}

export type CreateSchemaOptions = {
  cubes?: unknown[],
  views?: unknown[]
};

export function createSchemaYaml(schema: CreateSchemaOptions): string {
  return YAML.dump(schema);
}

export function createCubeSchemaYaml({ name, sqlTable }: CreateCubeSchemaOptions): string {
  return ` 
    cubes:
      - name: ${name}
        sql_table: ${sqlTable}
    
        measures:
          - name: count
            type: count
          - name: sum
            type: sum
            sql: amount
          - name: min
            sql: amount
            type: min
          - name: max
            sql: amount
            type: max
        dimensions:
          - name: id
            sql: id
            type: number
            primary_key: true
          - name: createdAt
            sql: created_at
            type: time
  `;
}

export function createECommerceSchema() {
  return {
    cubes: [{
      name: 'orders',
      sql_table: 'orders',
      measures: [{
        name: 'count',
        type: 'count',
      }],
      dimensions: [
        {
          name: 'created_at',
          sql: 'created_at',
          type: 'time',
        },
        {
          name: 'updated_at',
          sql: '{created_at}',
          type: 'time',
        },
        {
          name: 'status',
          sql: 'status',
          type: 'string',
        }
      ],
      preAggregations: [
        {
          name: 'orders_by_day_with_day',
          measures: ['count'],
          timeDimension: 'created_at',
          granularity: 'day',
          partition_granularity: 'day',
          build_range_start: {
            sql: 'SELECT NOW() - INTERVAL \'1000 day\'',
          },
          build_range_end: {
            sql: 'SELECT NOW()'
          },
        },
        {
          name: 'orders_by_day_with_day_by_status',
          measures: ['count'],
          dimensions: ['status'],
          timeDimension: 'created_at',
          granularity: 'day',
          partition_granularity: 'day',
          build_range_start: {
            sql: 'SELECT NOW() - INTERVAL \'1000 day\'',
          },
          build_range_end: {
            sql: 'SELECT NOW()'
          },
        }
      ]
    }],
    views: [{
      name: 'orders_view',
      cubes: [{
        join_path: 'orders',
        includes: [
          'created_at',
          'updated_at',
          'count',
          'status',
        ]
      }]
    }]
  };
}

/**
 * Returns joined test cubes schema. Schema looks like: A -< B -< C >- D >- E.
 * The original data set can be found under the link.
 * {@link https://docs.google.com/spreadsheets/d/1BNDpA7x4JLhlvvPdrQIC0c0PH4xZhdRrEFfXdRW1j4U/edit?usp=sharing|Dataset}
 */
export function createJoinedCubesSchema(): string {
  return `
    cube('A', {
      sql: \`
        select 1 as ID, 'A1' as A_VAL union all
        select 2 as ID, 'A2' as A_VAL union all
        select 3 as ID, 'A3' as A_VAL union all
        select 4 as ID, 'A4' as A_VAL union all
        select 5 as ID, 'A5' as A_VAL union all
        select 6 as ID, 'A6' as A_VAL union all
        select 7 as ID, 'A7' as A_VAL union all
        select 8 as ID, 'A8' as A_VAL
      \`,
      joins: {
        B: {
          relationship: 'hasMany',
          sql: \`\${CUBE}.ID = \${B}.A_ID\`,
        },
      },
      dimensions: {
        aid: {
          sql: 'ID',
          type: 'number',
          primaryKey: true,
        },
        aval: {
          sql: 'A_VAL',
          type: 'string',
        },
      },
      measures: {
        count: {
          type: 'count',
        },
        aval_count: {
          sql: 'A_VAL',
          type: 'count',
        },
      },
    });

    cube('B', {
      sql: \`
        select 1 as ID, 1 as A_ID, 10 as B_VAL union all
        select 2 as ID, 2 as A_ID, 10 as B_VAL union all
        select 3 as ID, 3 as A_ID, 20 as B_VAL union all
        select 4 as ID, 4 as A_ID, 20 as B_VAL union all
        select 5 as ID, 5 as A_ID, 30 as B_VAL union all
        select 6 as ID, 6 as A_ID, 30 as B_VAL union all
        select 7 as ID, 7 as A_ID, 40 as B_VAL union all
        select 8 as ID, 8 as A_ID, 40 as B_VAL
      \`,
      joins: {
        C: {
          relationship: 'hasMany',
          sql: \`\${CUBE}.ID = \${C}.B_ID\`,
        },
      },
      dimensions: {
        bid: {
          sql: 'ID',
          type: 'number',
          primaryKey: true,
        },
        aid: {
          sql: 'A_ID',
          type: 'number',
        },
        bval: {
          sql: 'B_VAL',
          type: 'number',
        },
      },
      measures: {
        count: {
          type: 'count',
        },
        bval_sum: {
          sql: 'B_VAL',
          type: 'sum',
        },
      },
    });

    cube('C', {
      sql: \`
        select 1 as ID, 1 as B_ID, 1 as D_ID union all
        select 2 as ID, 2 as B_ID, 2 as D_ID union all
        select 3 as ID, 3 as B_ID, 3 as D_ID union all
        select 4 as ID, 4 as B_ID, 4 as D_ID union all
        select 5 as ID, 5 as B_ID, 5 as D_ID union all
        select 6 as ID, 6 as B_ID, 6 as D_ID union all
        select 7 as ID, 7 as B_ID, 7 as D_ID union all
        select 8 as ID, 8 as B_ID, 8 as D_ID
      \`,
      joins: {
        D: {
          relationship: 'belongsTo',
          sql: \`\${CUBE}.D_ID = \${D}.ID\`,
        },
      },
      dimensions: {
        cid: {
          sql: 'ID',
          type: 'number',
          primaryKey: true,
        },
        bid: {
          sql: 'B_ID',
          type: 'number',
        },
        did: {
          sql: 'D_ID',
          type: 'number',
        },
      },
      measures: {
        count: {
          type: 'count',
        },
      },
    });

    cube('D', {
      sql: \`
        select 1 as ID, 1 as E_ID union all
        select 2 as ID, 2 as E_ID union all
        select 3 as ID, 3 as E_ID union all
        select 4 as ID, 4 as E_ID union all
        select 5 as ID, 5 as E_ID union all
        select 6 as ID, 6 as E_ID union all
        select 7 as ID, 7 as E_ID union all
        select 8 as ID, 8 as E_ID
      \`,
      joins: {
        E: {
          relationship: 'belongsTo',
          sql: \`\${CUBE}.E_ID = \${E}.ID\`,
        },
      },
      dimensions: {
        did: {
          sql: 'ID',
          type: 'number',
          primaryKey: true,
        },
        eid: {
          sql: 'E_ID',
          type: 'number',
        },
      },
      measures: {
        count: {
          type: 'count',
        },
      },
    });

    cube('E', {
      sql: \`
        select 1 as ID, 'E' as E_VAL union all
        select 2 as ID, 'E' as E_VAL union all
        select 3 as ID, 'F' as E_VAL union all
        select 4 as ID, 'F' as E_VAL union all
        select 5 as ID, 'G' as E_VAL union all
        select 6 as ID, 'G' as E_VAL union all
        select 7 as ID, 'H' as E_VAL union all
        select 8 as ID, 'H' as E_VAL
      \`,
      dimensions: {
        eid: {
          sql: 'ID',
          type: 'number',
          primaryKey: true,
        },
        eval: {
          sql: 'E_VAL',
          type: 'string',
        },
      },
      measures: {
        count: {
          type: 'count',
        },
      },
    });
  `;
}
