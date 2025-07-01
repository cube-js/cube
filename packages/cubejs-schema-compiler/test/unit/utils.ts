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
    // Useless comment for compilation, but is checked in
    // CubeSchemaConverter tests
    cube('${name}', {
        description: 'test cube from createCubeSchema',

        ${sqlTable ? `sqlTable: \`${sqlTable}\`` : 'sql: `select * from cards`'},

        ${publicly !== undefined ? `public: ${publicly},` : ''}
        ${shown !== undefined ? `shown: ${shown},` : ''}
        ${refreshKey}
        ${joins ? `joins: ${joins},` : ''}

        measures: {
          count: {
            description: 'count measure from createCubeSchema',
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
          },
          diff: {
            sql: \`\${max} - \${min}\`,
            type: \`number\`
          }
        },

        dimensions: {
          id: {
            type: 'number',
            description: 'id dimension from createCubeSchema',
            sql: 'id',
            primaryKey: true
          },
          id_cube: {
            type: 'number',
            sql: \`\${CUBE}.id\`,
          },
          other_id: {
            type: 'number',
            sql: 'other_id',
          },
          type: {
            type: 'string',
            sql: 'type'
          },
          type_with_cube: {
            type: 'string',
            sql: \`\${CUBE.type}\`,
          },
          type_complex: {
            type: 'string',
            sql: \`CONCAT(\${type}, ' ', \${location})\`,
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
            description: 'SF users segment from createCubeSchema',
            sql: \`\${CUBE}.location = 'San Francisco'\`
          }
        },

        preAggregations: {
            ${preAggregations}
        }
      })
  `;
}

export function createCubeSchemaWithAccessPolicy(name: string, extraPolicies: string = ''): string {
  return `cube('${name}', {
        description: 'test cube from createCubeSchemaWithAccessPolicy',
        sql: 'select * from cards',

        measures: {
          count: {
            description: 'count measure from createCubeSchemaWithAccessPolicy',
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
          },
          diff: {
            sql: \`\${max} - \${min}\`,
            type: \`number\`
          }
        },

        dimensions: {
          id: {
            type: 'number',
            description: 'id dimension from createCubeSchemaWithAccessPolicy',
            sql: 'id',
            primaryKey: true
          },
          id_cube: {
            type: 'number',
            sql: \`\${CUBE}.id\`,
          },
          other_id: {
            type: 'number',
            sql: 'other_id',
          },
          type: {
            type: 'string',
            sql: 'type'
          },
          type_with_cube: {
            type: 'string',
            sql: \`\${CUBE.type}\`,
          },
          type_complex: {
            type: 'string',
            sql: \`CONCAT(\${type}, ' ', \${location})\`,
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
        accessPolicy: [
          {
            role: "*",
            rowLevel: {
              allowAll: true
            }
          },
          {
            role: 'admin',
            conditions: [
              {
                if: \`true\`,
              }
            ],
            rowLevel: {
              filters: [
                {
                  member: \`$\{CUBE}.id\`,
                  operator: 'equals',
                  values: [\`1\`, \`2\`, \`3\`]
                }
              ]
            },
            memberLevel: {
              includes: \`*\`,
              excludes: [\`location\`, \`diff\`]
            },
          },
          {
            role: 'manager',
            conditions: [
              {
                if: security_context.userId === 1,
              }
            ],
            rowLevel: {
              filters: [
                {
                  or: [
                    {
                      member: \`location\`,
                      operator: 'startsWith',
                      values: [\`San\`]
                    },
                    {
                      member: \`location\`,
                      operator: 'startsWith',
                      values: [\`Lon\`]
                    }
                  ]
                }
              ]
            },
            memberLevel: {
              includes: \`*\`,
              excludes: [\`min\`, \`max\`]
            },
          },
          ${extraPolicies}
        ]
      })
  `;
}

export function createCubeSchemaWithCustomGranularitiesAndTimeShift(name: string): string {
  return `cube('${name}', {
        sql: 'select * from orders',
        public: true,
        dimensions: {
          createdAt: {
            public: true,
            sql: 'created_at',
            type: 'time',
            granularities: {
              half_year: {
                interval: '6 months',
                title: '6 month intervals'
              },
              half_year_by_1st_april: {
                title: 'Half year from Apr to Oct',
                interval: '6 months',
                offset: '3 months'
              },
              half_year_by_1st_march: {
                interval: '6 months',
                origin: '2020-03-01'
              },
              half_year_by_1st_june: {
                interval: '6 months',
                origin: '2020-06-01 10:00:00'
              }
            }
          },
          createdAtPredefinedYear: {
            public: true,
            sql: \`\${createdAt.year}\`,
            type: 'string',
          },
          createdAtPredefinedQuarter: {
            public: true,
            sql: \`\${createdAt.quarter}\`,
            type: 'string',
          },
          createdAtHalfYear: {
            public: true,
            sql: \`\${createdAt.half_year}\`,
            type: 'string',
          },
          createdAtHalfYearBy1stJune: {
            public: true,
            sql: \`\${createdAt.half_year_by_1st_june}\`,
            type: 'string',
          },
          createdAtHalfYearBy1stMarch: {
            public: true,
            sql: \`\${createdAt.half_year_by_1st_march}\`,
            type: 'string',
          },
          status: {
            type: 'string',
            sql: 'status',
          },
          id: {
            type: 'number',
            sql: 'id',
            primaryKey: true,
            public: true,
          }
        },
        measures: {
          count: {
            type: 'count'
          },
          count_shifted_year: {
            type: 'count',
            multiStage: true,
            timeShift: [{
              timeDimension: \`createdAt\`,
              interval: '1 year',
              type: 'prior'
            }]
          },
          rollingCountByTrailing2Day: {
            type: 'count',
            rollingWindow: {
              trailing: '2 day'
            }
          },
          rollingCountByLeading2Day: {
            type: 'count',
            rollingWindow: {
              leading: '3 day'
            }
          },
          rollingCountByUnbounded: {
            type: 'count',
            rollingWindow: {
              trailing: 'unbounded'
            }
          }
        },

        joins: {
          ${name}_users: {
            sql: \`\${${name}_users}.id = \${${name}}.user_id\`,
            relationship: \`one_to_many\`
          }
        }

      })

      cube(\`${name}_users\`, {
        sql: \`SELECT * FROM users\`,

        dimensions: {
          id: {
            type: 'number',
            sql: 'id',
            primaryKey: true,
            public: true,
          },
          name: {
            sql: 'name',
            type: 'string',
            public: true,
          },
          proxyCreatedAtPredefinedYear: {
            sql: \`\${${name}.createdAt.year}\`,
            type: \`string\`,
            public: true,
          },
          proxyCreatedAtHalfYear: {
            sql: \`\${${name}.createdAt.half_year}\`,
            type: 'string',
            public: true,
          }
        },

        measures: {
          count: {
            sql: 'user_id',
            type: 'count_distinct'
          }
        }
      })

      view(\`${name}_view\`, {
        cubes: [{
          join_path: ${name},
          includes: '*'
        }]
      })`;
}

export type CreateSchemaOptions = {
  cubes?: unknown[],
  views?: unknown[]
};

export function createSchemaYaml(schema: CreateSchemaOptions): string {
  return YAML.dump(schema);
}

export function createSchemaYamlForGroupFilterParamsTests(cubeDefSql: string): string {
  return createSchemaYaml({
    cubes: [
      {
        name: 'Order',
        sql: cubeDefSql,
        measures: [{
          name: 'count',
          type: 'count',
        }],
        dimensions: [
          {
            name: 'dim0',
            sql: 'dim0',
            type: 'string'
          },
          {
            name: 'dim1',
            sql: 'dim1',
            type: 'string'
          }
        ]
      },
    ],
    views: [{
      name: 'orders_view',
      cubes: [{
        join_path: 'Order',
        prefix: true,
        includes: [
          'count',
          'dim0',
          'dim1',
        ]
      }]
    }]
  });
}

export function createCubeSchemaYaml({ name, sqlTable }: CreateCubeSchemaOptions): string {
  return `
    # Useless comment for compilation, but is checked in
    # CubeSchemaConverter tests
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
    },
    {
      name: 'orders_indexes',
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
          name: 'status',
          sql: 'status',
          type: 'string',
        }
      ],
      preAggregations: [
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
          indexes: [
            {
              name: 'regular_index',
              columns: ['created_at', 'status']
            },
            {
              name: 'agg_index',
              columns: ['status'],
              type: 'aggregate'
            }

          ]
        }
      ]
    },
    ],
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
