import R from 'ramda';
import { UserError } from '../UserError';
import { ConfigItem } from './prepareAnnotation';
import { transformValue } from './transformValue';
import { NormalizedQuery } from '../type/query';
import QueryType from '../enum/QueryType';
import { ResultType } from '../type/strings';

/**
 * SQL aliases to cube properties hash map.
 */
type AliasToMemberMap = { [alias: string]: string };

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
    if (resType === 'compact') {
      row = R.pipe(
        R.toPairs,
        R.map(p => {
          const memberName = aliasToMemberNameMap[p[0]];
          const annotationForMember = annotation[memberName];
          return transformValue(p[1], annotationForMember.type);
        })
      )(r);
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
    
      if (queryType === QueryType.COMPARE_DATE_RANGE_QUERY) {
        return {
          ...row,
          compareDateRange: dateRange.join(' - ')
        };
      } else if (queryType === QueryType.BLENDING_QUERY) {
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

export default transformData;
