/**
 * @license Apache-2.0
 * @copyright Cube Dev, Inc.
 * @fileoverview
 * prepareAnnotation function and related types definition.
 */

import R from 'ramda';
import { MetaConfig, MetaConfigMap, toConfigMap } from './toConfigMap';
import { MemberType } from '../types/strings';
import { MemberType as MemberTypeEnum } from '../types/enums';
import { MemberExpression } from '../types/query';

type GranularityMeta = {
  name: string;
  title: string;
  interval: string;
  offset?: string;
  origin?: string;
};

/**
 * Annotation item for cube's member.
 */
type ConfigItem = {
  title: string;
  shortTitle: string;
  description: string;
  type: string;
  format: string;
  meta: any;
  drillMembers?: any[];
  drillMembersGrouped?: any;
  granularities?: GranularityMeta[];
};

/**
 * Returns annotations by MetaConfigMap and cube's member type.
 */
const annotation = (
  configMap: MetaConfigMap,
  memberType: MemberType,
) => (member: string | MemberExpression): undefined | [string, ConfigItem] => {
  const [cubeName, fieldName] = (<MemberExpression>member).expression ? [(<MemberExpression>member).cubeName, (<MemberExpression>member).name] : (<string>member).split('.');
  const memberWithoutGranularity = [cubeName, fieldName].join('.');
  const config: ConfigItem = configMap[cubeName][memberType]
    .find(m => m.name === memberWithoutGranularity);

  if (!config) {
    return undefined;
  }
  return [typeof member === 'string' ? member : memberWithoutGranularity, {
    title: config.title,
    shortTitle: config.shortTitle,
    description: config.description,
    type: config.type,
    format: config.format,
    meta: config.meta,
    ...(memberType === MemberTypeEnum.MEASURES ? {
      drillMembers: config.drillMembers,
      drillMembersGrouped: config.drillMembersGrouped
    } : {}),
    ...(memberType === MemberTypeEnum.DIMENSIONS && config.granularities ? {
      granularities: config.granularities || [],
    } : {}),
  }];
};

/**
 * Returns annotations object by MetaConfigs and query.
 */
function prepareAnnotation(metaConfig: MetaConfig[], query: any) {
  const configMap = toConfigMap(metaConfig);
  const dimensions = (query.dimensions || []);
  return {
    measures: R.fromPairs(
      (query.measures || []).map(
        annotation(configMap, MemberTypeEnum.MEASURES)
      ).filter(a => !!a)
    ),
    dimensions: R.fromPairs(
      dimensions
        .map(annotation(configMap, MemberTypeEnum.DIMENSIONS))
        .filter(a => !!a)
    ),
    segments: R.fromPairs(
      (query.segments || [])
        .map(annotation(configMap, MemberTypeEnum.SEGMENTS))
        .filter(a => !!a)
    ),
    timeDimensions: R.fromPairs(
      R.unnest(
        (query.timeDimensions || [])
          .filter(td => !!td.granularity)
          .map(
            td => {
              const an = annotation(
                configMap,
                MemberTypeEnum.DIMENSIONS,
              )(
                `${td.dimension}.${td.granularity}`
              );

              if (an && an[1].granularities) {
                // No need to send all the granularities defined, only those make sense for this query
                an[1].granularities = an[1].granularities.filter(g => g.name === td.granularity);
              }

              // TODO: deprecated: backward compatibility for
              // referencing time dimensions without granularity
              if (dimensions.indexOf(td.dimension) !== -1) {
                return [an].filter(a => !!a);
              }

              const dimWithoutGranularity = annotation(
                configMap,
                MemberTypeEnum.DIMENSIONS
              )(td.dimension);

              if (dimWithoutGranularity && dimWithoutGranularity[1].granularities) {
                // no need to populate granularities here
                dimWithoutGranularity[1].granularities = undefined;
              }

              return [an].concat([dimWithoutGranularity])
                .filter(a => !!a);
            }
          )
      )
    ),
  };
}

export default prepareAnnotation;
export {
  ConfigItem,
  annotation,
  prepareAnnotation,
};
