cube(`visitors`, {
  sql: `
      select * from visitors WHERE ${FILTER_PARAMS.visitors.createdAt.filter('created_at')}
      AND ${FILTER_PARAMS.ReferenceOriginalSql.createdAt.filter('created_at')}
      `,

  joins: {
    visitor_checkins: {
      relationship: 'hasMany',
      sql: `${CUBE.id} = ${visitor_checkins.visitor_id}`
    },

    cards: {
      relationship: 'hasMany',
      sql: `${visitors.id} = ${cards.visitorId}`
    }
  },

  measures: {
    count: {
      type: 'count'
    },

    checkinsTotal: {
      sql: `${checkinsCount}`,
      type: 'sum'
    },

    checkinsRollingTotal: {
      sql: `${checkinsCount}`,
      type: 'sum',
      rollingWindow: {
        trailing: 'unbounded'
      }
    },

    checkinsRolling2day: {
      sql: `${checkinsCount}`,
      type: 'sum',
      rollingWindow: {
        trailing: '2 day'
      }
    },

    uniqueSourceCount: {
      sql: 'source',
      type: 'countDistinct'
    },

    countDistinctApprox: {
      sql: 'id',
      type: 'countDistinctApprox'
    },

    ratio: {
      sql: `${uniqueSourceCount} / nullif(${checkinsTotal}, 0)`,
      type: 'number'
    }
  },

  dimensions: {
    id: {
      type: 'number',
      sql: 'id',
      primaryKey: true
    },
    source: {
      type: 'string',
      sql: 'source'
    },
    createdAt: {
      type: 'time',
      sql: 'created_at'
    },
    checkinsCount: {
      type: 'number',
      sql: `${visitor_checkins.count}`,
      subQuery: true,
      propagateFiltersToSubQuery: true
    }
  },

  segments: {
    google: {
      sql: `source = 'google'`
    }
  },

  preAggregations: {
    default: {
      type: 'originalSql',
      refreshKey: {
        sql: 'select NOW()'
      },
      indexes: {
        source: {
          columns: ['source', 'created_at']
        }
      },
      partitionGranularity: 'month',
      timeDimensionReference: createdAt
    },
    googleRollup: {
      type: 'rollup',
      measureReferences: [checkinsTotal],
      segmentReferences: [google],
      timeDimensionReference: createdAt,
      granularity: 'week',
    },
    approx: {
      type: 'rollup',
      measureReferences: [countDistinctApprox],
      timeDimensionReference: createdAt,
      granularity: 'day'
    },
    multiStage: {
      useOriginalSqlPreAggregations: true,
      type: 'rollup',
      measureReferences: [checkinsTotal],
      timeDimensionReference: createdAt,
      granularity: 'month',
      partitionGranularity: 'day',
      refreshKey: {
        sql: `SELECT CASE WHEN ${FILTER_PARAMS.visitors.createdAt.filter((from, to) => `${to}::timestamp > now()`)} THEN now() END`
      }
    },
    partitioned: {
      type: 'rollup',
      measureReferences: [checkinsTotal],
      dimensionReferences: [source],
      timeDimensionReference: createdAt,
      granularity: 'day',
      partitionGranularity: 'month',
      refreshKey: {
        every: '1 hour',
        incremental: true,
        updateWindow: '7 days'
      }
    },
    partitionedHourly: {
      type: 'rollup',
      measureReferences: [checkinsTotal],
      dimensionReferences: [source],
      timeDimensionReference: createdAt,
      granularity: 'hour',
      partitionGranularity: 'hour'
    },
    partitionedHourlyForRange: {
      type: 'rollup',
      measureReferences: [checkinsTotal],
      dimensionReferences: [source, createdAt],
      timeDimensionReference: createdAt,
      granularity: 'hour',
      partitionGranularity: 'hour'
    },
    ratioRollup: {
      type: 'rollup',
      measureReferences: [checkinsTotal, uniqueSourceCount],
      timeDimensionReference: createdAt,
      granularity: 'day'
    },
    forJoin: {
      type: 'rollup',
      dimensionReferences: [id, source]
    },
    forJoinIncCards: {
      type: 'rollup',
      dimensionReferences: [id, source, cards.visitorId]
    },
    partitionedHourlyForJoin: {
      type: 'rollup',
      dimensionReferences: [id, source],
      timeDimensionReference: createdAt,
      granularity: 'hour',
      partitionGranularity: 'hour'
    },
    partitionedRolling: {
      type: 'rollup',
      measureReferences: [checkinsRollingTotal, checkinsRolling2day, count],
      dimensionReferences: [source],
      timeDimensionReference: createdAt,
      granularity: 'hour',
      partitionGranularity: 'month',
      external: true
    },
  }
});

cube('visitor_checkins', {
  sql: `
      select * from visitor_checkins
      `,

  sqlAlias: 'vc',

  measures: {
    count: {
      type: 'count'
    }
  },

  dimensions: {
    id: {
      type: 'number',
      sql: 'id',
      primaryKey: true
    },
    visitor_id: {
      type: 'number',
      sql: 'visitor_id'
    },
    source: {
      type: 'string',
      sql: 'source'
    },
    created_at: {
      type: 'time',
      sql: 'created_at'
    }
  },

  preAggregations: {
    main: {
      type: 'originalSql'
    },
    forJoin: {
      type: 'rollup',
      measureReferences: [count],
      dimensionReferences: [visitor_id]
    },
    joined: {
      type: 'rollupJoin',
      measureReferences: [count],
      dimensionReferences: [visitors.source],
      rollupReferences: [visitor_checkins.forJoin, visitors.forJoin],
    },
    joinedPartitioned: {
      type: 'rollupJoin',
      measureReferences: [count],
      dimensionReferences: [visitors.source],
      timeDimensionReference: visitors.createdAt,
      granularity: 'hour',
      rollupReferences: [visitor_checkins.forJoin, visitors.partitionedHourlyForJoin],
    },
    joinedIncCards: {
      type: 'rollupJoin',
      measureReferences: [count],
      dimensionReferences: [visitors.source, cards.visitorId],
      rollupReferences: [visitor_checkins.forJoin, visitors.forJoinIncCards],
    },
    partitioned: {
      type: 'rollup',
      measureReferences: [count],
      timeDimensionReference: EveryHourVisitors.createdAt,
      granularity: 'day',
      partitionGranularity: 'month',
      scheduledRefresh: true,
      refreshRangeStart: {
        sql: "SELECT NOW() - interval '30 day'"
      },
      refreshKey: {
        every: '1 hour',
        incremental: true,
        updateWindow: '1 day'
      }
    },
    emptyPartitioned: {
      type: 'rollup',
      measureReferences: [count],
      timeDimensionReference: EmptyHourVisitors.createdAt,
      granularity: 'hour',
      partitionGranularity: 'month',
      scheduledRefresh: true,
      refreshKey: {
        every: '1 hour',
        incremental: true,
        updateWindow: '1 day'
      }
    }
  }
});

cube('cards', {
  sql: `
      select * from cards
      `,

  measures: {
    count: {
      type: 'count'
    }
  },

  dimensions: {
    id: {
      type: 'number',
      sql: 'id',
      primaryKey: true
    },

    visitorId: {
      type: 'number',
      sql: 'visitor_id'
    }
  },

  preAggregations: {
    forJoin: {
      type: 'rollup',
      measureReferences: [count],
      dimensionReferences: [visitorId]
    },
  }
});

cube('GoogleVisitors', {
  refreshKey: {
    immutable: true,
  },
  extends: visitors,
  sql: `select v.* from ${visitors.sql()} v where v.source = 'google'`
});

cube('EveryHourVisitors', {
  refreshKey: {
    immutable: true,
  },
  extends: visitors,
  sql: `select v.* from ${visitors.sql()} v where v.source = 'google'`,

  preAggregations: {
    default: {
      type: 'originalSql',
      refreshKey: {
        sql: 'select NOW()'
      }
    },
    partitioned: {
      type: 'rollup',
      measureReferences: [checkinsTotal],
      dimensionReferences: [source],
      timeDimensionReference: createdAt,
      granularity: 'day',
      partitionGranularity: 'month',
      refreshKey: {
        every: '1 hour',
        incremental: true,
        updateWindow: '1 day'
      }
    }
  }
});

cube('EmptyHourVisitors', {
  extends: EveryHourVisitors,
  sql: `select v.* from ${visitors.sql()} v where created_at < '2000-01-01'`
});

cube('ReferenceOriginalSql', {
  extends: visitors,
  sql: `select v.* from ${visitors.sql()} v where v.source = 'google'`,

  preAggregations: {
    partitioned: {
      type: 'rollup',
      measureReferences: [count],
      dimensionReferences: [source],
      timeDimensionReference: createdAt,
      granularity: 'day',
      partitionGranularity: 'month',
      refreshKey: {
        every: '1 hour',
        incremental: true,
        updateWindow: '1 day'
      },
      useOriginalSqlPreAggregations: true
    }
  }
});
