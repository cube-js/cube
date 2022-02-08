/**
 * @license Apache-2.0
 * @copyright Cube Dev, Inc.
 * @fileoverview
 * transformData function and related types definition.
 */

import R from 'ramda';
import { UserError } from '../UserError';
import { ConfigItem } from './prepareAnnotation';
import { transformValue } from './transformValue';
import { NormalizedQuery, QueryTimeDimension } from '../types/query';
import {
  ResultType,
  QueryType,
  QueryTimeDimensionGranularity,
} from '../types/strings';
import {
  ResultType as ResultTypeEnum,
  QueryType as QueryTypeEnum,
} from '../types/enums';

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
 * Transforms queried data set to the output network format.
 */
function transformData(
  aliasToMemberNameMap: AliasToMemberMap,
  annotation: { [member: string]: ConfigItem },
  data: { [sqlAlias: string]: unknown }[],
  query: NormalizedQuery,
  queryType: QueryType,
  resType: ResultType
) {
  const members: string[] = [];
  const dataset = data.map((r, i) => {
    let row;
    if (resType === ResultTypeEnum.COMPACT) {
      row = R.pipe(
        R.toPairs,
        R.map(p => {
          const memberName = aliasToMemberNameMap[p[0]];
          const annotationForMember = annotation[memberName];
          if (i === 0) members.push(memberName);
          return transformValue(p[1], annotationForMember.type);
        })
      )(r);

      if (queryType === QueryTypeEnum.COMPARE_DATE_RANGE_QUERY) {
        if (i === 0) members.push('compareDateRange');
        row.push(getDateRangeValue(query.timeDimensions));
      } else if (queryType === QueryTypeEnum.BLENDING_QUERY) {
        if (i === 0) {
          members.push(getBlendingQueryKey(query.timeDimensions));
        }
        row.push(row[getBlendingResponseKey(query.timeDimensions)]);
      }
    } else {
      row = R.pipe(
        R.toPairs,
        R.map(p => {
          const memberName = aliasToMemberNameMap[p[0]];
          const annotationForMember = annotation[memberName];
          if (!annotationForMember) {
            throw new UserError(
              `You requested hidden member: '${p[0]}'. Please make it visible using \`shown: true\`. Please note primaryKey fields are \`shown: false\` by default: https://cube.dev/docs/schema/reference/joins#setting-a-primary-key.`
            );
          }
          const transformResult = [
            memberName,
            transformValue(p[1], annotationForMember.type)
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
      )(r);

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
    }
    return row;
  });
  return resType === ResultTypeEnum.COMPACT
    ? { members, dataset }
    : dataset;
}

export default transformData;
export {
  AliasToMemberMap,
  transformData,
};
