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
} from '../../src/helpers/transformMetaExtended';

const MockUsersCube = {
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
  },
  refreshKey: {
    every: '1 hour',
  },
  // eslint-disable-next-line quotes
  sql: () => `SELECT * FROM MockUsers`,
  name: 'MockUsersCube',
  fileName: 'MockUsersCube.js',
};

const MockDimensionCaseResult = {
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

describe('transformMetaExtended helpers', () => {
  test('stringifyMemberSql', () => {
    expect(stringifyMemberSql(undefined)).toBeUndefined();
    expect(stringifyMemberSql(MockUsersCube.sql)).toBeDefined();
    expect(stringifyMemberSql(MockUsersCube.sql)).toBe('`SELECT * FROM MockUsers`');
  });

  test('getMemberPath', () => {
    expect(getMemberPath('Users.count')).toBeDefined();
    expect(getMemberPath('Users.count')).toEqual({ cubeName: 'Users', memberName: 'count' });
  });

  test('handleDimensionCaseCondition', () => {
    expect(handleDimensionCaseCondition(undefined)).toBeUndefined();
    const handledCaseCondition = handleDimensionCaseCondition(MockUsersCube.dimensions.plan.case);
    expect(handledCaseCondition).toBeDefined();
    expect(handledCaseCondition).toEqual(MockDimensionCaseResult);
  });

  test('transformCube', () => {
    const handledCube = transformCube(MockUsersCube, MockUsersCube);
    expect(handledCube).toBeDefined();
    expect(handledCube).toMatchObject({ name: 'MockUsersCube' });
    expect(handledCube).toHaveProperty('measures');
    expect(handledCube).toHaveProperty('dimensions');
  });

  test('transformDimension', () => {
    const handledDimension = transformDimension(MockUsersCube.dimensions.id, MockUsersCube);
    expect(handledDimension).toBeDefined();
    expect(handledDimension).toHaveProperty('sql');
  });

  test('transformMeasure', () => {
    const handledMeasure = transformMeasure(MockUsersCube.measures.count, MockUsersCube);
    expect(handledMeasure).toBeDefined();
    expect(handledMeasure).toHaveProperty('sql');
    expect(handledMeasure).toMatchObject({ type: 'count' });
  });
});
