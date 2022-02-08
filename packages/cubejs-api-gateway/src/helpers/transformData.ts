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
import { NormalizedQuery, QueryTimeDimension } from '../types/query';
import {
  ResultType,
  QueryType,
} from '../types/strings';
import {
  ResultType as ResultTypeEnum,
  QueryType as QueryTypeEnum,
} from '../types/enums';

const COMPARE_DATE_RANGE_FIELD = 'compareDateRange';

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
      return dim.dateRange.join(' - ');
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
      return `time.${dim.granularity}`;
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
      return `${dim.dimension}.${dim.granularity}`;
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
  data: { [sqlAlias: string]: DBResponseValue }[]
): string[] {
  const members = Object.keys(data[0]);
  if (queryType === QueryTypeEnum.COMPARE_DATE_RANGE_QUERY) {
    members.push(COMPARE_DATE_RANGE_FIELD);
  } else if (queryType === QueryTypeEnum.BLENDING_QUERY) {
    members.push(getBlendingQueryKey(query.timeDimensions));
  }
  return members;
}

/**
 * Convert DB response object to the compact output format.
 * @internal
 */
function getCompactRow(
  aliasToMemberNameMap: AliasToMemberMap,
  annotation: { [member: string]: ConfigItem },
  queryType: QueryType,
  members: string[],
  timeDimensions: QueryTimeDimension[] | undefined,
  item: { [sqlAlias: string]: DBResponseValue },
): DBResponsePrimitive[] {
  const row: DBResponsePrimitive[] = [];
  members.forEach((col: string) => {
    row.push(
      transformValue(
        item[col],
        annotation[aliasToMemberNameMap[col]].type
      ),
    );
  });
  if (queryType === QueryTypeEnum.COMPARE_DATE_RANGE_QUERY) {
    row.push(getDateRangeValue(timeDimensions));
  } else if (queryType === QueryTypeEnum.BLENDING_QUERY) {
    row.push(row[getBlendingResponseKey(timeDimensions)]);
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
  item: { [sqlAlias: string]: DBResponseValue },
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
      const path = memberName.split('.');

      /**
       * Time dimensions without granularity.
       * @deprecated
       * @todo backward compatibility for referencing
       */
      const memberNameWithoutGranularity =
        [path[0], path[1]].join('.');
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
  )(item);

  // @ts-ignore
  const [{ dimension, granularity, dateRange } = {}]
    = query.timeDimensions;

  if (queryType === QueryTypeEnum.COMPARE_DATE_RANGE_QUERY) {
    return {
      ...row,
      compareDateRange: dateRange.join(' - ')
    };
  } else if (queryType === QueryTypeEnum.BLENDING_QUERY) {
    return {
      ...row,
      [['time', granularity].join('.')]:
        row[[dimension, granularity].join('.')]
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
  resType: ResultType
): {
  members: string[],
  dataset: DBResponsePrimitive[][]
} | {
  [member: string]: DBResponsePrimitive
}[] {
  const d = data as { [sqlAlias: string]: DBResponseValue }[];
  const members: string[] = getMembers(queryType, query, d);
  const dataset: DBResponsePrimitive[][] | {
    [member: string]: DBResponsePrimitive
  }[] = d.map((r) => {
    const row: DBResponsePrimitive[] | {
      [member: string]: DBResponsePrimitive
    } = resType === ResultTypeEnum.COMPACT
      ? getCompactRow(
        aliasToMemberNameMap,
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
  getDateRangeValue,
  getBlendingQueryKey,
  getBlendingResponseKey,
  getMembers,
  getCompactRow,
  getVanilaRow,
  transformData,
};
