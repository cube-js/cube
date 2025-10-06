/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable no-template-curly-in-string */
/* eslint-disable quote-props */
/**
 * @license Apache-2.0
 * @copyright Cube Dev, Inc.
 * @fileoverview transformMetaExtended related helpers unit tests.
 */

/* globals describe,test,expect */

import {
  stringifyMemberSql,
  getMemberPath,
  handleDimensionCaseCondition,
  transformCube,
  transformDimension,
  transformMeasure,
  transformSegment,
  transformJoins,
  transformPreAggregations,
} from '../../src/helpers/transformMetaExtended';

const MOCK_USERS_CUBE = {
  measures: {
    count: {
      sql: () => 'id',
      type: 'count',
    },
  },
  dimensions: {
    id: {
      sql: () => 'id',
      type: 'string',
      primaryKey: true,
      shown: true,
    },
    email: {
      sql: () => 'email',
      type: 'string',
    },
    plan: {
      case: {
        when: {
          '0': {
            // eslint-disable-next-line quotes
            sql: () => `tenantEnterpriseFlag = true`,
            label: 'Enterprise',
          },
          '1': {
            // eslint-disable-next-line quotes
            sql: () => `stripe_customer_id IS NOT NULL`,
            label: 'Standard',
          },
        },
        else: {
          label: 'Free',
        },
      },
      type: 'string',
    },
    sessionsCount: {
      sql: () => '${AweWebSessions.count}',
      type: 'number',
      subQuery: true,
    },
  },
  segments: {
    testSegment: {
      // eslint-disable-next-line quotes
      sql: () => `testName IS NULL`,
    },
  },
  joins: {
    PlaygroundUsers: {
      relationship: 'belongsTo',
      // eslint-disable-next-line quotes
      sql: () => `{CUBE}.id = {PlaygroundUsers.anonymous}`,
    },
    IpEnrich: {
      relationship: 'belongsTo',
      // eslint-disable-next-line quotes
      sql: () => `{CUBE.email} = {IpEnrich.email}`,
    },
  },
  preAggregations: {
    main: {
      granularity: 'day',
      refreshKey: {
        every: '0 5 * * *',
        timezone: 'America/Los_Angeles',
      },
      type: 'rollup',
      scheduledRefresh: true,
    },
    eventsByType: {
      granularity: 'day',
      refreshKey: {
        every: '0 5 * * *',
        timezone: 'America/Los_Angeles',
      },
      type: 'rollup',
      scheduledRefresh: true,
    },
  },
  refreshKey: {
    every: '1 hour',
  },
  // eslint-disable-next-line quotes
  sql: () => `SELECT * FROM MockUsers`,
  name: 'MockUsersCube',
  fileName: 'MockUsersCube.js',
};

const MOCK_DIMENSION_CASE_RESULT = {
  when: [
    {
      sql: '`tenantEnterpriseFlag = true`',
      label: 'Enterprise'
    },
    {
      sql: '`stripe_customer_id IS NOT NULL`',
      label: 'Standard'
    }
  ],
  else: {
    label: 'Free'
  }
};

const MOCK_SEGMENT = {
  name: 'MockUsersCube.testSegment',
  title: 'MockUsersCube Test Segment',
  shortTitle: 'Test Segment',
  isVisible: true,
};

describe('transformMetaExtended helpers', () => {
  test('stringifyMemberSql', () => {
    expect(stringifyMemberSql(undefined)).toBeUndefined();
    expect(stringifyMemberSql(MOCK_USERS_CUBE.sql)).toBeDefined();
    expect(stringifyMemberSql(MOCK_USERS_CUBE.sql)).toBe('`SELECT * FROM MockUsers`');
  });

  test('getMemberPath', () => {
    expect(getMemberPath('Users.count')).toBeDefined();
    expect(getMemberPath('Users.count')).toEqual({ cubeName: 'Users', memberName: 'count' });
  });

  test('handleDimensionCaseCondition', () => {
    expect(handleDimensionCaseCondition(undefined)).toBeUndefined();
    const handledCaseCondition = handleDimensionCaseCondition(MOCK_USERS_CUBE.dimensions.plan.case);
    expect(handledCaseCondition).toBeDefined();
    expect(handledCaseCondition).toEqual(MOCK_DIMENSION_CASE_RESULT);
  });

  test('transformCube', () => {
    const handledCube = transformCube(MOCK_USERS_CUBE, MOCK_USERS_CUBE);
    expect(handledCube).toBeDefined();
    expect(handledCube).toMatchObject({ name: 'MockUsersCube' });
    expect(handledCube).toHaveProperty('measures');
    expect(handledCube).toHaveProperty('dimensions');
  });

  test('transformCube - extends field preservation', () => {
    const mockCubeDefinitions = {
      BaseCube: { extends: () => 'BaseCube' },
      ExtendedCube: { extends: () => 'ExtendedCube' },
      AnotherCube: { extends: () => 'AnotherCube' },
      TestCube: { extends: () => 'TestCube' },
      SampleCube: { extends: () => 'SampleCube' }
    };

    const mockCubes = [
      { name: 'BaseCube' },
      { name: 'ExtendedCube' },
      { name: 'AnotherCube' },
      { name: 'TestCube' },
      { name: 'SampleCube' }
    ];

    mockCubes.forEach(cube => {
      const transformedCube = transformCube(cube, mockCubeDefinitions);
      expect(transformedCube).toBeDefined();
      expect(transformedCube.extends).toBe(`'${cube.name}'`);
    });

    // Specific test cases to verify first letter preservation
    const baseCube = transformCube({ name: 'BaseCube' }, mockCubeDefinitions);
    expect(baseCube.extends).toBe('\'BaseCube\'');

    const extendedCube = transformCube({ name: 'ExtendedCube' }, mockCubeDefinitions);
    expect(extendedCube.extends).toBe('\'ExtendedCube\'');
  });

  test('transformDimension', () => {
    const handledDimension = transformDimension(MOCK_USERS_CUBE.dimensions.id, MOCK_USERS_CUBE);
    expect(handledDimension).toBeDefined();
    expect(handledDimension).toHaveProperty('sql');
  });

  test('transformMeasure', () => {
    const handledMeasure = transformMeasure(MOCK_USERS_CUBE.measures.count, MOCK_USERS_CUBE);
    expect(handledMeasure).toBeDefined();
    expect(handledMeasure).toHaveProperty('sql');
    expect(handledMeasure).toMatchObject({ type: 'count' });
  });

  test('transformSegment', () => {
    const handledSegment = transformSegment(MOCK_SEGMENT, MOCK_USERS_CUBE);
    expect(handledSegment).toBeDefined();
    expect(handledSegment).toHaveProperty('sql');
  });

  test('transformJoins', () => {
    expect(transformJoins(undefined)).toBeUndefined();
    const handledJoins = transformJoins(MOCK_USERS_CUBE.joins);
    expect(handledJoins).toBeDefined();
    expect(handledJoins?.length).toBe(2);
  });

  test('transformJoins - array format', () => {
    // Test new array format (after PR #9800)
    const arrayJoins = [
      {
        name: 'PlaygroundUsers',
        relationship: 'belongsTo',
        sql: () => `{CUBE}.id = {PlaygroundUsers.anonymous}`,
      },
      {
        name: 'IpEnrich',
        relationship: 'belongsTo',
        sql: () => `{CUBE.email} = {IpEnrich.email}`,
      },
    ];
    
    const handledJoins = transformJoins(arrayJoins);
    expect(handledJoins).toBeDefined();
    expect(handledJoins?.length).toBe(2);
    expect(handledJoins?.[0].name).toBe('PlaygroundUsers');
    expect(handledJoins?.[1].name).toBe('IpEnrich');
    expect(handledJoins?.[0].sql).toBe('`{CUBE}.id = {PlaygroundUsers.anonymous}`');
    expect(handledJoins?.[1].sql).toBe('`{CUBE.email} = {IpEnrich.email}`');
  });

  test('transformPreAggregations', () => {
    expect(transformPreAggregations(undefined)).toBeUndefined();
    const handledPreAggregations = transformPreAggregations(MOCK_USERS_CUBE.preAggregations);
    expect(handledPreAggregations).toBeDefined();
    expect(handledPreAggregations?.length).toBe(2);
  });
});
