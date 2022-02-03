import R from 'ramda';
import { UserError } from '../UserError';
import { QUERY_TYPE } from '../query';
import { ConfigItem } from './prepareAnnotation';
import { transformValue } from './transformValue';

/**
 * User's query decorated by ./../query.js:normalizeQuery().
 * TODO: Provide type definition.
 */
type NormalizedQuery = any;

/**
 * SQL aliases to cube properties hash map.
 */
type AliasToMemberMap = { [alias: string]: string };

export
/**
 * String that represent query type.
 */
enum QueryType {
  REGULAR_QUERY = 'regularQuery',
  COMPARE_DATE_RANGE_QUERY = 'compareDateRangeQuery',
  BLENDING_QUERY = 'blendingQuery',
}

export
/**
 * String that represent required dataset format.
 */
enum ResultType {
  DEFAULT = 'default',
  COMPACT = 'compact',
  ARROW = 'arrow'
}

export
/**
 * Transforms queried data set to the output network format.
 */
function transformData(
  aliasToMemberNameMap: AliasToMemberMap,
  annotation: { [member: string]: ConfigItem },
  data: { [alias: string]: unknown }[],
  query: NormalizedQuery,
  queryType: QueryType,
  resType: ResultType
) {
  return data.map(r => {
    let row;
    if (resType === ResultType.COMPACT) {
      row = R.pipe(
        // @ts-ignore
        R.toPairs,
        R.map(p => {
          const memberName = aliasToMemberNameMap[p[0]];
          const annotationForMember = annotation[memberName];
          return transformValue(p[1], annotationForMember.type);
        })
      )(r);
    } else {
      row = R.pipe(
        // @ts-ignore
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
    
          // TODO: deprecated: backward compatibility for referencing
          // time dimensions without granularity
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
    
      if (queryType === QUERY_TYPE.COMPARE_DATE_RANGE_QUERY) {
        return {
          ...row,
          compareDateRange: dateRange.join(' - ')
        };
      } else if (queryType === QUERY_TYPE.BLENDING_QUERY) {
        return {
          ...row,
          [['time', granularity].join('.')]:
            row[[dimension, granularity].join('.')]
        };
      }
    }
    return row;
  });
}
