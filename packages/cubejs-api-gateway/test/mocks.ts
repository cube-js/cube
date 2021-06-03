export const preAggregationsResultFactory = () => ([
  {
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
    partitions: [
      {
        timezone: 'UTC',
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
            granularity: 'day',
            dateRange: [
              '2021-04-30T00:00:00.000',
              '2021-04-30T23:59:59.999'
            ]
          }
        ],
        rollups: [],
        sql: {
          tableName: 'dev_pre_aggregations.usage_usages20210430'
        }
      }
    ]
  }
]);

export const preAggregationVersionEntriesResultFactory = () => ([
  {
    table_name: 'dev_pre_aggregations.usage_usages20210501',
    content_version: '1k5lbvhc',
    structure_version: 'ztptkip5',
    last_updated_at: 1621782171000,
    naming_version: 2
  },
  {
    table_name: 'dev_pre_aggregations.usage_usages20210430',
    content_version: 'imocehmz',
    structure_version: 'osacmcoe',
    last_updated_at: 1621782171000,
    naming_version: 2
  }
]);

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

  async metaConfig() {
    return [
      {
        config: {
          name: 'Foo',
          measures: [
            {
              name: 'Foo.bar',
            },
          ],
          dimensions: [
            {
              name: 'Foo.id',
            },
            {
              name: 'Foo.time',
            },
          ],
        },
      },
    ];
  },

  async preAggregations() {
    return preAggregationsResultFactory();
  }
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

  public async executeQuery() {
    return {
      data: [{ foo__bar: 42 }]
    };
  }

  public addDataSeenSource() {
    return undefined;
  }

  public getPreAggregationVersionEntries() {
    return preAggregationVersionEntriesResultFactory();
  }
}
