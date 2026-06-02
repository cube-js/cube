export const preAggregationsResultFactory = () => ([
  {
    id: 'Usage.usages',
    preAggregationName: 'usages',
    preAggregation: {
      type: 'rollup',
      scheduledRefresh: true,
    },
    cube: 'Usage',
    references: {
      dimensions: [
        'Usage.deploymentId',
        'Usage.tenantId'
      ],
      measures: [
        'Usage.count'
      ],
      timeDimensions: [
        {
          dimension: 'Usage.createdAt',
          granularity: 'day'
        }
      ],
      rollups: []
    }
  }
]);

export const preAggregationPartitionsResultFactory = () => ([
  {
    timezones: ['UTC'],
    preAggregation: preAggregationsResultFactory()[0],
    partitions: [{
      tableName: 'dev_pre_aggregations.usage_usages20210430'
    }]
  }
]);

export const preAggregationVersionEntriesResultFactory = () => ({
  structureVersionsByTableName: {
    'dev_pre_aggregations.usage_usages20210501': 'ztptkip5',
    'dev_pre_aggregations.usage_usages20210430': 'osacmcoe',
  },
  versionEntriesByTableName: {
    'dev_pre_aggregations.usage_usages20210501': [{
      table_name: 'dev_pre_aggregations.usage_usages20210501',
      content_version: '1k5lbvhc',
      structure_version: 'ztptkip5',
      last_updated_at: 1621782171000,
      naming_version: 2
    }],
    'dev_pre_aggregations.usage_usages20210430': [{
      table_name: 'dev_pre_aggregations.usage_usages20210430',
      content_version: 'imocehmz',
      structure_version: 'osacmcoe',
      last_updated_at: 1621782171000,
      naming_version: 2
    }]
  }
});

export const compilerApi = jest.fn().mockImplementation(async () => ({
  async getSql() {
    return {
      sql: ['SELECT * FROM test', []],
      aliasNameToMember: {
        foo__bar: 'Foo.bar',
        foo__time: 'Foo.time',
      },
      order: [{ id: 'id', desc: true, }],
      dataSource: 'default'
    };
  },

  async getDbType() {
    return 'postgres';
  },

  async applyRowLevelSecurity(query: any) {
    return { query, denied: false };
  },

  async metaConfig(_ctx, options: any = {}) {
    const cubes = [
      {
        config: {
          name: 'Foo',
          type: 'cube',
          description: 'cube from compilerApi mock',
          measures: [
            {
              name: 'Foo.bar',
              description: 'measure from compilerApi mock',
              isVisible: true,
            },
          ],
          dimensions: [
            {
              name: 'Foo.id',
              description: 'id dimension from compilerApi mock',
              isVisible: true,
            },
            {
              name: 'Foo.time',
              isVisible: true,
            },
            {
              name: 'Foo.timeGranularities',
              isVisible: true,
              granularities: [
                {
                  name: 'half_year_by_1st_april',
                  title: 'Half Year By1 St April',
                  interval: '6 months',
                  offset: '3 months'
                }
              ]
            },
          ],
          segments: [
            {
              name: 'Foo.quux',
              description: 'segment from compilerApi mock',
              isVisible: true,
            },
          ],
        },
      },
      {
        config: {
          name: 'FooView',
          type: 'view',
          description: 'view from compilerApi mock',
          viewGroups: ['analytics'],
          measures: [
            {
              name: 'FooView.bar',
              isVisible: true,
            },
          ],
          dimensions: [
            {
              name: 'FooView.id',
              isVisible: true,
            },
          ],
          segments: [],
        },
      },
    ];

    if (options.includeCompilerId || options.includeViewGroups) {
      const result: any = { cubes };
      if (options.includeCompilerId) {
        result.compilerId = 'mock-compiler-id';
      }
      if (options.includeViewGroups) {
        result.viewGroups = [
          {
            name: 'analytics',
            title: 'Analytics',
            description: 'Analytics related views',
            views: ['FooView'],
            includes: [
              'FooView',
              {
                name: 'restricted',
                title: 'Restricted',
                // Only references a hidden view, so it must be pruned from meta.
                views: ['HiddenView'],
                includes: ['HiddenView'],
              },
            ],
          },
        ];
      }
      // NOTE: `views`/`includes` here represent the already-compiled meta shape
      // returned by the compiler (post-resolution), where both fields are
      // always present — not the authored view group definition.
      return result;
    }

    return cubes;
  },

  async metaConfigExtended() {
    const metaConfig = [
      {
        config: {
          name: 'Foo',
          description: 'cube from compilerApi mock',
          measures: [
            {
              name: 'Foo.bar',
              description: 'measure from compilerApi mock',
              sql: 'bar',
              isVisible: true,
            },
          ],
          dimensions: [
            {
              name: 'Foo.id',
              description: 'id dimension from compilerApi mock',
              isVisible: true,
            },
            {
              name: 'Foo.time',
              isVisible: true,
            },
          ],
          segments: [
            {
              name: 'Foo.quux',
              description: 'segment from compilerApi mock',
              isVisible: true,
            },
          ],
        },
      },
      {
        config: {
          name: 'FooView',
          type: 'view',
          description: 'view from compilerApi mock',
          measures: [
            {
              name: 'FooView.bar',
              isVisible: true,
            },
          ],
          dimensions: [
            {
              name: 'FooView.id',
              isVisible: true,
            },
          ],
          segments: [],
        },
      },
    ];

    const cubeDefinitions = {
      Foo: {
        sql: () => 'SELECT * FROM Foo',
        measures: {},
        dimension: {},
      },
      FooView: {
        measures: {},
        dimensions: {},
      },
    };

    return {
      metaConfig,
      cubeDefinitions,
    };
  },

  async preAggregations() {
    return preAggregationsResultFactory();
  },

  async dataSources() {
    return {
      dataSources: [{ dataSource: 'default', dbType: 'postgres' }]
    };
  },
}));

export class RefreshSchedulerMock {
  public async preAggregationPartitions() {
    return preAggregationPartitionsResultFactory();
  }
}

export class DataSourceStorageMock {
  public $testConnectionsDone: boolean = false;

  public $testOrchestratorConnectionsDone: boolean = false;

  public async testConnections() {
    this.$testConnectionsDone = true;

    return [];
  }

  public async testOrchestratorConnections() {
    this.$testOrchestratorConnectionsDone = true;

    return [];
  }
}

export class AdapterApiMock {
  public $testConnectionsDone: boolean = false;

  public $testOrchestratorConnectionsDone: boolean = false;

  public async testConnection() {
    this.$testConnectionsDone = true;

    return [];
  }

  public async testOrchestratorConnections() {
    this.$testOrchestratorConnectionsDone = true;

    return [];
  }

  public async executeQuery(query) {
    if (query?.query.includes('SELECT * FROM sql-runner')) {
      return {
        data: [
          { skip: 'skip' },
          { string: 'string', number: 1, buffer: { type: 'Buffer', data: [48, 48] }, bufferTwo: { type: 'Placeholder', data: [48, 48, 48, 48] }, object: { ob: 'object' } }
        ],
      };
    }

    return {
      data: [{ foo__bar: 42 }],
    };
  }

  public driverFactory() {
    return {
      wrapQueryWithLimit(query: { query: string; limit: number }) {
        query.query = `SELECT * FROM (${query.query}) AS t LIMIT ${query.limit}`;
      },
    };
  }

  public getQueryOrchestrator() {
    return {
      fetchSchema: () => ({
        other: {
          orders: [
            {
              name: 'id',
              type: 'integer',
              attributes: [],
            },
            {
              name: 'test_id',
              type: 'integer',
              attributes: [],
            },
          ],
        },
      })
    };
  }

  public addDataSeenSource() {
    return undefined;
  }

  public getPreAggregationVersionEntries() {
    return preAggregationVersionEntriesResultFactory();
  }
}
