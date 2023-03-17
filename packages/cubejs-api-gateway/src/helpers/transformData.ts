/**
 * @license Apache-2.0
 * @copyright Cube Dev, Inc.
 * @fileoverview
 * transformData function and related types definition.
 */

import R from 'ramda';
import { UserError } from '../UserError';
import { ConfigItem } from './prepareAnnotation';
import {
  DBResponsePrimitive,
  DBResponseValue,
  transformValue,
} from './transformValue';
import {
  NormalizedQuery,
  QueryTimeDimension
} from '../types/query';
import {
  ResultType,
  QueryType,
} from '../types/strings';
import {
  ResultType as ResultTypeEnum,
  QueryType as QueryTypeEnum,
} from '../types/enums';

const COMPARE_DATE_RANGE_FIELD = 'compareDateRange';
const COMPARE_DATE_RANGE_SEPARATOR = ' - ';
const BLENDING_QUERY_KEY_PREFIX = 'time.';
const BLENDING_QUERY_RES_SEPARATOR = '.';
const MEMBER_SEPARATOR = '.';

/**
 * SQL aliases to cube properties hash map.
 */
type AliasToMemberMap = { [alias: string]: string };

/**
 * Parse date range value from time dimension.
 * @internal
 */
function getDateRangeValue(
  timeDimensions?: QueryTimeDimension[]
): string {
  if (!timeDimensions) {
    throw new UserError(
      'QueryTimeDimension should be specified ' +
      'for the compare date range query.'
    );
  } else {
    const [dim] = timeDimensions;
    if (!dim.dateRange) {
      throw new UserError(
        `${'Inconsistent QueryTimeDimension configuration ' +
        'for the compare date range query, dateRange required: '}${
          dim}`
      );
    } else if (typeof dim.dateRange === 'string') {
      throw new UserError(
        'Inconsistent dateRange configuration for the ' +
        `compare date range query: ${dim.dateRange}`
      );
    } else {
      return dim.dateRange.join(COMPARE_DATE_RANGE_SEPARATOR);
    }
  }
}

/**
 * Parse blending query key from time time dimension.
 * @internal
 */
function getBlendingQueryKey(
  timeDimensions?: QueryTimeDimension[]
): string {
  if (!timeDimensions) {
    throw new UserError(
      'QueryTimeDimension should be specified ' +
      'for the blending query.'
    );
  } else {
    const [dim] = timeDimensions;
    if (!dim.granularity) {
      throw new UserError(
        'Inconsistent QueryTimeDimension configuration ' +
        `for the blending query, granularity required: ${dim}`
      );
    } else {
      return BLENDING_QUERY_KEY_PREFIX + dim.granularity;
    }
  }
}

/**
 * Parse blending response key from time time dimension.
 * @internal
 */
function getBlendingResponseKey(
  timeDimensions?: QueryTimeDimension[]
): string {
  if (!timeDimensions) {
    throw new UserError(
      'QueryTimeDimension should be specified ' +
      'for the blending query.'
    );
  } else {
    const [dim] = timeDimensions;
    if (!dim.granularity) {
      throw new UserError(
        'Inconsistent QueryTimeDimension configuration ' +
        `for the blending query, granularity required: ${dim}`
      );
    } else if (!dim.dimension) {
      throw new UserError(
        'Inconsistent QueryTimeDimension configuration ' +
        `for the blending query, dimension required: ${dim}`
      );
    } else {
      return dim.dimension +
        BLENDING_QUERY_RES_SEPARATOR +
        dim.granularity;
    }
  }
}

/**
 * Parse members names from request/response.
 * @internal
 */
function getMembers(
  queryType: QueryType,
  query: NormalizedQuery,
  dbData: { [sqlAlias: string]: DBResponseValue }[],
  aliasToMemberNameMap: AliasToMemberMap,
  annotation: { [member: string]: ConfigItem },
): { [member: string]: string } {
  const members: { [member: string]: string } = {};
  if (!dbData.length) {
    return members;
  }
  const columns = Object.keys(dbData[0]);
  columns.forEach((column) => {
    if (!aliasToMemberNameMap[column] || !annotation[aliasToMemberNameMap[column]]) {
      throw new UserError(
        `You requested hidden member: '${
          column
        }'. Please make it visible using \`shown: true\`. ` +
        'Please note primaryKey fields are `shown: false` by ' +
        'default: https://cube.dev/docs/schema/reference/joins#' +
        'setting-a-primary-key.'
      );
    }
    members[aliasToMemberNameMap[column]] = column;
    const path = aliasToMemberNameMap[column]
      .split(MEMBER_SEPARATOR);
    const calcMember =
      [path[0], path[1]].join(MEMBER_SEPARATOR);
    if (
      path.length === 3 &&
      query.dimensions?.indexOf(calcMember) === -1
    ) {
      members[calcMember] = column;
    }
  });
  if (queryType === QueryTypeEnum.COMPARE_DATE_RANGE_QUERY) {
    members[COMPARE_DATE_RANGE_FIELD] =
      QueryTypeEnum.COMPARE_DATE_RANGE_QUERY;
  } else if (queryType === QueryTypeEnum.BLENDING_QUERY) {
    members[getBlendingQueryKey(query.timeDimensions)] =
      // @ts-ignore
      members[query.timeDimensions[0].dimension];
  }
  return members;
}

/**
 * Convert DB response object to the compact output format.
 * @internal
 * @todo should we use transformValue for blending query?
 */
function getCompactRow(
  membersToAliasMap: { [member: string]: string },
  annotation: { [member: string]: ConfigItem },
  queryType: QueryType,
  members: string[],
  timeDimensions: QueryTimeDimension[] | undefined,
  dbRow: { [sqlAlias: string]: DBResponseValue },
): DBResponsePrimitive[] {
  const row: DBResponsePrimitive[] = [];
  members.forEach((m: string) => {
    if (annotation[m]) {
      row.push(
        transformValue(
          dbRow[membersToAliasMap[m]],
          annotation[m].type
        ),
      );
    }
  });
  if (queryType === QueryTypeEnum.COMPARE_DATE_RANGE_QUERY) {
    row.push(
      getDateRangeValue(timeDimensions)
    );
  } else if (queryType === QueryTypeEnum.BLENDING_QUERY) {
    console.log(getBlendingResponseKey(timeDimensions));
    console.log(dbRow[
      getBlendingResponseKey(timeDimensions)
    ]);
    row.push(
      dbRow[
        membersToAliasMap[
          getBlendingResponseKey(timeDimensions)
        ]
      ] as DBResponsePrimitive
    );
  }
  return row;
}

/**
 * Convert DB response object to the vanila output format.
 * @todo rewrite me please!
 * @internal
 */
function getVanilaRow(
  aliasToMemberNameMap: AliasToMemberMap,
  annotation: { [member: string]: ConfigItem },
  queryType: QueryType,
  query: NormalizedQuery,
  dbRow: { [sqlAlias: string]: DBResponseValue },
): { [member: string]: DBResponsePrimitive } {
  const row = R.pipe(
    R.toPairs,
    R.map(p => {
      const memberName = aliasToMemberNameMap[p[0]];
      const annotationForMember = annotation[memberName];
      if (!annotationForMember) {
        throw new UserError(
          `You requested hidden member: '${
            p[0]
          }'. Please make it visible using \`shown: true\`. ` +
          'Please note primaryKey fields are `shown: false` by ' +
          'default: https://cube.dev/docs/schema/reference/joins#' +
          'setting-a-primary-key.'
        );
      }
      const transformResult = [
        memberName,
        transformValue(
          p[1] as DBResponseValue,
          annotationForMember.type
        )
      ];
      const path = memberName.split(MEMBER_SEPARATOR);

      /**
       * Time dimensions without granularity.
       * @deprecated
       * @todo backward compatibility for referencing
       */
      const memberNameWithoutGranularity =
        [path[0], path[1]].join(MEMBER_SEPARATOR);
      if (
        path.length === 3 &&
        (query.dimensions || [])
          .indexOf(memberNameWithoutGranularity) === -1
      ) {
        return [
          transformResult,
          [
            memberNameWithoutGranularity,
            transformResult[1]
          ]
        ];
      }

      return [transformResult];
    }),
    // @ts-ignore
    R.unnest,
    R.fromPairs
  // @ts-ignore
  )(dbRow);
  if (queryType === QueryTypeEnum.COMPARE_DATE_RANGE_QUERY) {
    return {
      ...row,
      compareDateRange: getDateRangeValue(query.timeDimensions)
    };
  } else if (queryType === QueryTypeEnum.BLENDING_QUERY) {
    return {
      ...row,
      [getBlendingQueryKey(query.timeDimensions)]:
        row[getBlendingResponseKey(query.timeDimensions)]
    };
  }
  return row as { [member: string]: DBResponsePrimitive; };
}

/**
 * Transforms queried data array to the output format.
 */
function transformData(
  aliasToMemberNameMap: AliasToMemberMap,
  annotation: { [member: string]: ConfigItem },
  data: { [sqlAlias: string]: unknown }[],
  query: NormalizedQuery,
  queryType: QueryType,
  resType?: ResultType
): {
  members: string[],
  dataset: DBResponsePrimitive[][]
} | {
  [member: string]: DBResponsePrimitive
}[] {
  const d = data as { [sqlAlias: string]: DBResponseValue }[];
  const membersToAliasMap = getMembers(
    queryType,
    query,
    d,
    aliasToMemberNameMap,
    annotation,
  );
  const members: string[] = Object.keys(membersToAliasMap);
  const dataset: DBResponsePrimitive[][] | {
    [member: string]: DBResponsePrimitive
  }[] = d.map((r) => {
    const row: DBResponsePrimitive[] | {
      [member: string]: DBResponsePrimitive
    } = resType === ResultTypeEnum.COMPACT
      ? getCompactRow(
        membersToAliasMap,
        annotation,
        queryType,
        members,
        query.timeDimensions,
        r,
      )
      : getVanilaRow(
        aliasToMemberNameMap,
        annotation,
        queryType,
        query,
        r,
      );
    return row;
  }) as DBResponsePrimitive[][] | {
    [member: string]: DBResponsePrimitive
  }[];
  return (resType === ResultTypeEnum.COMPACT
    ? { members, dataset }
    : dataset
  ) as {
    members: string[],
    dataset: DBResponsePrimitive[][]
  } | {
    [member: string]: DBResponsePrimitive
  }[];
}

export default transformData;
export {
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
};
