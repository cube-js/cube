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
    timezone: 'UTC',
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

export const compilerApi = jest.fn().mockImplementation(() => ({
  async getSql() {
    return {
      sql: ['SELECT * FROM test', []],
      aliasNameToMember: {
        foo__bar: 'Foo.bar',
        foo__time: 'Foo.time',
      },
      order: [{ id: 'id', desc: true, }]
    };
  },

  async getDbType() {
    return 'postgres';
  },

  async metaConfig() {
    return [
      {
        config: {
          name: 'Foo',
          measures: [
            {
              name: 'Foo.bar',
              isVisible: true,
            },
          ],
          dimensions: [
            {
              name: 'Foo.id',
              isVisible: true,
            },
            {
              name: 'Foo.time',
              isVisible: true,
            },
          ],
        },
      },
    ];
  },

  async metaConfigExtended() {
    const metaConfig = [
      {
        config: {
          name: 'Foo',
          measures: [
            {
              name: 'Foo.bar',
              sql: 'bar',
              isVisible: true,
            },
          ],
          dimensions: [
            {
              name: 'Foo.id',
              isVisible: true,
            },
            {
              name: 'Foo.time',
              isVisible: true,
            },
          ],
        },
      },
    ];

    const cubeDefinitions = {
      Foo: {
        sql: () => 'SELECT * FROM Foo',
        measures: {},
        dimension: {},
      }
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
