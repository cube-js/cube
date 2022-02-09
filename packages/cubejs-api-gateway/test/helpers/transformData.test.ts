/**
 * @license Apache-2.0
 * @copyright Cube Dev, Inc.
 * @fileoverview transformData related helpers unit tests.
 */

/* globals describe,test,expect,jest */
/* eslint-disable import/no-duplicates */
/* eslint-disable @typescript-eslint/no-duplicate-imports */

import {
  QueryTimeDimension,
  NormalizedQuery,
} from '../../src/types/query';
import {
  QueryType as QueryTypeEnum,
  ResultType as ResultTypeEnum,
} from '../../src/types/enums';
import {
  DBResponsePrimitive,
  DBResponseValue,
  transformValue,
} from '../../src/helpers/transformValue';
import transformDataDefault
  from '../../src/helpers/transformData';
import {
  ConfigItem,
} from '../../src/helpers/prepareAnnotation';
import {
  AliasToMemberMap,
  COMPARE_DATE_RANGE_FIELD,
  COMPARE_DATE_RANGE_SEPARATOR,
  BLENDING_QUERY_KEY_PREFIX,
  BLENDING_QUERY_RES_SEPARATOR,
  MEMBER_SEPARATOR,
  getDateRangeValue,
  getBlendingQueryKey,
  getBlendingResponseKey,
  getMembers,
  getCompactRow,
  getVanilaRow,
  transformData,
} from '../../src/helpers/transformData';

describe('transformData helpers', () => {
  test('export looks as expected', () => {
    expect(transformDataDefault).toBeDefined();
    expect(COMPARE_DATE_RANGE_FIELD).toBeDefined();
    expect(COMPARE_DATE_RANGE_SEPARATOR).toBeDefined();
    expect(BLENDING_QUERY_KEY_PREFIX).toBeDefined();
    expect(BLENDING_QUERY_RES_SEPARATOR).toBeDefined();
    expect(MEMBER_SEPARATOR).toBeDefined();
    expect(getDateRangeValue).toBeDefined();
    expect(getBlendingQueryKey).toBeDefined();
    expect(getBlendingResponseKey).toBeDefined();
    expect(getMembers).toBeDefined();
    expect(getCompactRow).toBeDefined();
    expect(getVanilaRow).toBeDefined();
    expect(transformData).toBeDefined();
    expect(transformData).toEqual(transformDataDefault);
  });
  test('getDateRangeValue helper', () => {
    const d = Date();
    expect(() => { getDateRangeValue(); }).toThrow(
      'QueryTimeDimension should be specified ' +
      'for the compare date range query.'
    );
    expect(() => {
      getDateRangeValue([
        { prop: 'val' } as unknown as QueryTimeDimension
      ]);
    }).toThrow(
      `${'Inconsistent QueryTimeDimension configuration ' +
      'for the compare date range query, dateRange required: '}${
        ({ prop: 'val' }).toString()}`
    );
    expect(() => {
      getDateRangeValue([
        { dateRange: 'val' } as unknown as QueryTimeDimension
      ]);
    }).toThrow(
      'Inconsistent dateRange configuration for the ' +
      'compare date range query: val'
    );
    expect(getDateRangeValue([
      { dateRange: [d, d] } as unknown as QueryTimeDimension
    ])).toEqual(`${d}${COMPARE_DATE_RANGE_SEPARATOR}${d}`);
  });
  test('getBlendingQueryKey helper', () => {
    expect(() => {
      getBlendingQueryKey();
    }).toThrow(
      'QueryTimeDimension should be specified ' +
      'for the blending query.'
    );
    expect(() => {
      getBlendingQueryKey([
        { prop: 'val' } as unknown as QueryTimeDimension
      ]);
    }).toThrow(
      'Inconsistent QueryTimeDimension configuration ' +
      `for the blending query, granularity required: ${
        ({ prop: 'val' }).toString()}`
    );
    expect(
      getBlendingQueryKey([
        { granularity: 'day' } as unknown as QueryTimeDimension
      ])
    ).toEqual(`${BLENDING_QUERY_KEY_PREFIX}day`);
  });
  test('getBlendingResponseKey helper', () => {
    expect(() => {
      getBlendingResponseKey();
    }).toThrow(
      'QueryTimeDimension should be specified ' +
      'for the blending query.'
    );
    expect(() => {
      getBlendingResponseKey([
        { prop: 'val' } as unknown as QueryTimeDimension
      ]);
    }).toThrow(
      'Inconsistent QueryTimeDimension configuration ' +
      `for the blending query, granularity required: ${
        ({ prop: 'val' }).toString()}`
    );
    expect(() => {
      getBlendingResponseKey([
        { granularity: 'day' } as unknown as QueryTimeDimension
      ]);
    }).toThrow(
      'Inconsistent QueryTimeDimension configuration ' +
      `for the blending query, dimension required: ${
        ({ granularity: 'day' }).toString()}`
    );
    expect(
      getBlendingResponseKey([
        {
          granularity: 'day',
          dimension: 'dim',
        } as unknown as QueryTimeDimension
      ])
    ).toEqual(`dim${BLENDING_QUERY_RES_SEPARATOR}day`);
  });
  test('getMembers helper', () => {
    // throw
    expect(() => {
      getMembers(
        QueryTypeEnum.REGULAR_QUERY,
        {} as NormalizedQuery,
        [
          { col_1: 'col1', col_2: 'col2', col_3: 'col3' }
        ] as { [sqlAlias: string]: DBResponseValue }[],
        {
          col_1: 'col1',
          col_2: 'col2',
        },
      );
    }).toThrow(
      'You requested hidden member: \'col_3\'. Please make it ' +
      'visible using `shown: true`. ' +
      'Please note primaryKey fields are `shown: false` by ' +
      'default: https://cube.dev/docs/schema/reference/joins#' +
      'setting-a-primary-key.'
    );

    // regular
    expect(getMembers(
      QueryTypeEnum.REGULAR_QUERY,
      {} as NormalizedQuery,
      [] as { [sqlAlias: string]: DBResponseValue }[],
      {},
    )).toEqual([]);
    expect(getMembers(
      QueryTypeEnum.REGULAR_QUERY,
      {} as NormalizedQuery,
      [
        { col_1: 'col1', col_2: 'col2', col_3: 'col3' }
      ] as { [sqlAlias: string]: DBResponseValue }[],
      {
        col_1: 'col1',
        col_2: 'col2',
        col_3: 'col3',
      },
    )).toEqual(['col1', 'col2', 'col3']);

    // compare
    expect(getMembers(
      QueryTypeEnum.COMPARE_DATE_RANGE_QUERY,
      {} as NormalizedQuery,
      [] as { [sqlAlias: string]: DBResponseValue }[],
      {},
    )).toEqual([]);
    expect(getMembers(
      QueryTypeEnum.COMPARE_DATE_RANGE_QUERY,
      {} as NormalizedQuery,
      [
        { col_1: 'col1', col_2: 'col2', col_3: 'col3' }
      ] as { [sqlAlias: string]: DBResponseValue }[],
      {
        col_1: 'col1',
        col_2: 'col2',
        col_3: 'col3',
      },
    )).toEqual(['col1', 'col2', 'col3', COMPARE_DATE_RANGE_FIELD]);

    // blending
    expect(getMembers(
      QueryTypeEnum.BLENDING_QUERY,
      {} as NormalizedQuery,
      [] as { [sqlAlias: string]: DBResponseValue }[],
      {},
    )).toEqual([]);
    expect(getMembers(
      QueryTypeEnum.BLENDING_QUERY,
      {
        timeDimensions: [{ granularity: 'day' }]
      } as unknown as NormalizedQuery,
      [
        { col_1: 'col1', col_2: 'col2', col_3: 'col3' }
      ] as { [sqlAlias: string]: DBResponseValue }[],
      {
        col_1: 'col1',
        col_2: 'col2',
        col_3: 'col3',
      },
    )).toEqual(['col1', 'col2', 'col3', getBlendingQueryKey([
      { granularity: 'day' } as unknown as QueryTimeDimension
    ])]);
  });
  test('getCompactRow helper', () => {
    // regular
    expect(
      getCompactRow(
        // aliasToMemberNameMap
        {
          member_1: 'member1',
          member_2: 'member2',
        },

        // annotation
        {
          member1: {
            type: 'string',
          },
          member2: {
            type: 'string',
          },
        } as unknown as { [member: string]: ConfigItem },

        // queryType
        QueryTypeEnum.REGULAR_QUERY,

        // members
        [
          'member_1',
          'member_2',
        ],

        // time dimensions
        undefined,

        // db row
        {
          member_1: 'value 1',
          member_2: 'value 2',
        },
      )
    ).toEqual(['value 1', 'value 2']);

    // compare date range
    const d = Date();
    expect(
      getCompactRow(
        // aliasToMemberNameMap
        {
          member_1: 'member1',
          member_2: 'member2',
        },

        // annotation
        {
          member1: {
            type: 'string',
          },
          member2: {
            type: 'string',
          },
        } as unknown as { [member: string]: ConfigItem },

        // queryType
        QueryTypeEnum.COMPARE_DATE_RANGE_QUERY,

        // members
        [],

        // time dimensions
        [
          {
            dateRange: [d, d]
          } as unknown as QueryTimeDimension
        ],

        // db row
        {},
      )
    ).toEqual([
      `${d}${COMPARE_DATE_RANGE_SEPARATOR}${d}`
    ]);
    expect(
      getCompactRow(
        // aliasToMemberNameMap
        {
          member_1: 'member1',
          member_2: 'member2',
        },

        // annotation
        {
          member1: {
            type: 'string',
          },
          member2: {
            type: 'string',
          },
        } as unknown as { [member: string]: ConfigItem },

        // queryType
        QueryTypeEnum.COMPARE_DATE_RANGE_QUERY,

        // members
        [],

        // time dimensions
        [
          {
            dateRange: [d, d]
          } as unknown as QueryTimeDimension
        ],

        // db row
        {
          member_1: 'value 1',
          member_2: 'value 2',
        },
      )
    ).toEqual([
      `${d}${COMPARE_DATE_RANGE_SEPARATOR}${d}`
    ]);
    expect(
      getCompactRow(
        // aliasToMemberNameMap
        {
          member_1: 'member1',
          member_2: 'member2',
        },

        // annotation
        {
          member1: {
            type: 'string',
          },
          member2: {
            type: 'string',
          },
        } as unknown as { [member: string]: ConfigItem },

        // queryType
        QueryTypeEnum.COMPARE_DATE_RANGE_QUERY,

        // members
        [
          'member_1',
        ],

        // time dimensions
        [
          {
            dateRange: [d, d]
          } as unknown as QueryTimeDimension
        ],

        // db row
        {
          member_1: 'value 1',
          member_2: 'value 2',
        },
      )
    ).toEqual([
      'value 1',
      `${d}${COMPARE_DATE_RANGE_SEPARATOR}${d}`
    ]);
    expect(
      getCompactRow(
        // aliasToMemberNameMap
        {
          member_1: 'member1',
          member_2: 'member2',
        },

        // annotation
        {
          member1: {
            type: 'string',
          },
          member2: {
            type: 'string',
          },
        } as unknown as { [member: string]: ConfigItem },

        // queryType
        QueryTypeEnum.COMPARE_DATE_RANGE_QUERY,

        // members
        [
          'member_1',
          'member_2',
        ],

        // time dimensions
        [
          {
            dateRange: [d, d]
          } as unknown as QueryTimeDimension
        ],

        // db row
        {
          member_1: 'value 1',
          member_2: 'value 2',
        },
      )
    ).toEqual([
      'value 1',
      'value 2',
      `${d}${COMPARE_DATE_RANGE_SEPARATOR}${d}`
    ]);

    // blending
    expect(
      getCompactRow(
        // aliasToMemberNameMap
        {
          member_1: 'member1',
          member_2: 'member2',
        },

        // annotation
        {
          member1: {
            type: 'string',
          },
          member2: {
            type: 'string',
          },
        } as unknown as { [member: string]: ConfigItem },

        // queryType
        QueryTypeEnum.BLENDING_QUERY,

        // members
        [],

        // time dimensions
        [
          {
            dimension: 'member_2',
            granularity: 'day',
          } as unknown as QueryTimeDimension
        ],

        // db row
        {
          member_1: 'value 1',
          member_2: 'value 2',
        },
      )
    ).toEqual([
      undefined,
    ]);
    expect(
      getCompactRow(
        // aliasToMemberNameMap
        {
          member_1: 'member1',
          member_2: 'member2',
        },

        // annotation
        {
          member1: {
            type: 'string',
          },
          member2: {
            type: 'string',
          },
        } as unknown as { [member: string]: ConfigItem },

        // queryType
        QueryTypeEnum.BLENDING_QUERY,

        // members
        [],

        // time dimensions
        [
          {
            dimension: 'member_2',
            granularity: 'day',
          } as unknown as QueryTimeDimension
        ],

        // db row
        {
          member_1: 'value 1',
          member_2: 'value 2',
          'member_2.day': 'granular value',
        },
      )
    ).toEqual([
      'granular value'
    ]);
    expect(
      getCompactRow(
        // aliasToMemberNameMap
        {
          member_1: 'member1',
          member_2: 'member2',
        },

        // annotation
        {
          member1: {
            type: 'string',
          },
          member2: {
            type: 'string',
          },
        } as unknown as { [member: string]: ConfigItem },

        // queryType
        QueryTypeEnum.BLENDING_QUERY,

        // members
        [
          'member_1',
        ],

        // time dimensions
        [
          {
            dimension: 'member_2',
            granularity: 'day',
          } as unknown as QueryTimeDimension
        ],

        // db row
        {
          member_1: 'value 1',
          member_2: 'value 2',
          'member_2.day': 'granular value',
        },
      )
    ).toEqual([
      'value 1',
      'granular value'
    ]);
    expect(
      getCompactRow(
        // aliasToMemberNameMap
        {
          member_1: 'member1',
          member_2: 'member2',
        },

        // annotation
        {
          member1: {
            type: 'string',
          },
          member2: {
            type: 'string',
          },
        } as unknown as { [member: string]: ConfigItem },

        // queryType
        QueryTypeEnum.BLENDING_QUERY,

        // members
        [
          'member_1',
          'member_2',
        ],

        // time dimensions
        [
          {
            dimension: 'member_2',
            granularity: 'day',
          } as unknown as QueryTimeDimension
        ],

        // db row
        {
          member_1: 'value 1',
          member_2: 'value 2',
          'member_2.day': 'granular value',
        },
      )
    ).toEqual([
      'value 1',
      'value 2',
      'granular value'
    ]);
  });
  test('getVanilaRow helper', () => {
    // hidden memeber request
    expect(() => {
      getVanilaRow(
        // aliasToMemberNameMap
        {
          member_1: 'member1',
        },

        // annotation
        {
          member1: {
            type: 'string',
          },
          member2: {
            type: 'string',
          },
        } as unknown as { [member: string]: ConfigItem },

        // queryType
        QueryTypeEnum.REGULAR_QUERY,

        // query
        {} as NormalizedQuery,

        // db row
        {
          member_1: 'value 1',
          member_2: 'value 2',
        },
      );
    }).toThrow(
      'You requested hidden member: \'member_2\'. Please make it ' +
      'visible using `shown: true`. ' +
      'Please note primaryKey fields are `shown: false` by ' +
      'default: https://cube.dev/docs/schema/reference/joins#' +
      'setting-a-primary-key.'
    );

    // blending query
    expect(
      getVanilaRow(
        // aliasToMemberNameMap
        {
          member_1: 'member1',
          member_2: 'member2',
          'member_2.day': 'time.day',
        },

        // annotation
        {
          member1: {
            type: 'string',
          },
          member2: {
            type: 'string',
          },
          'time.day': {
            type: 'string',
          },
        } as unknown as { [member: string]: ConfigItem },

        // queryType
        QueryTypeEnum.BLENDING_QUERY,

        // query
        {
          dimensions: [
            'member_1',
            'member_2',
          ],
          timeDimensions: [
            {
              dimension: 'member_2',
              granularity: 'day',
            } as unknown as QueryTimeDimension
          ],
        } as NormalizedQuery,

        // db row
        {
          member_1: 'value 1',
          member_2: 'value 2',
          'member_2.day': 'granular value',
        },
      )
    ).toEqual({
      member1: 'value 1',
      member2: 'value 2',
      'time.day': 'granular value',
    });
  });
  test('transformData helper', () => {
    // regular
    expect(
      transformData(
        // aliasToMemberNameMap
        {
          member_1: 'member1',
          member_2: 'member2',
        },

        // annotation
        {
          member1: {
            type: 'string',
          },
          member2: {
            type: 'string',
          },
        } as unknown as { [member: string]: ConfigItem },

        // data
        [{
          member_1: 'value 1',
          member_2: 'value 2',
        }],

        // query
        {
          dimensions: [
            'member_1',
            'member_2',
          ],
        } as NormalizedQuery,

        // queryType
        QueryTypeEnum.REGULAR_QUERY,

        // resType
        ResultTypeEnum.COMPACT,
      )
    ).toEqual([{
      member1: 'value 1',
      member2: 'value 2',
    }]);
    // expect(
    //   transformData(
    //     // aliasToMemberNameMap
    //     {
    //       member_1: 'member1',
    //       member_2: 'member2',
    //     },

    //     // annotation
    //     {
    //       member1: {
    //         type: 'string',
    //       },
    //       member2: {
    //         type: 'string',
    //       },
    //     } as unknown as { [member: string]: ConfigItem },

    //     // data
    //     [{
    //       member_1: 'value 1',
    //       member_2: 'value 2',
    //     }],

    //     // query
    //     {
    //       dimensions: [
    //         'member1',
    //         'member2',
    //       ],
    //     } as NormalizedQuery,

    //     // queryType
    //     QueryTypeEnum.REGULAR_QUERY,

    //     // resType
    //     ResultTypeEnum.COMPACT,
    //   )
    // ).toEqual({
    //   members: ['member1', 'member2'],
    //   dataset: [['value 1', 'value 2']],
    // });
  });
});
