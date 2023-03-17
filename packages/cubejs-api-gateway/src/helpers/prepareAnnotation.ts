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
};

/**
 * Returns annotations by MetaConfigMap and cube's member type.
 */
const annotation = (
  configMap: MetaConfigMap,
  memberType: MemberType,
) => (member: string): undefined | [string, ConfigItem] => {
  const [cubeName, fieldName] = member.split('.');
  const memberWithoutGranularity = [cubeName, fieldName].join('.');
  const config: ConfigItem = configMap[cubeName][memberType]
    .find(m => m.name === memberWithoutGranularity);

  if (!config) {
    return undefined;
  }
  return [member, {
    title: config.title,
    shortTitle: config.shortTitle,
    description: config.description,
    type: config.type,
    format: config.format,
    meta: config.meta,
    ...(memberType === MemberTypeEnum.MEASURES ? {
      drillMembers: config.drillMembers,
      drillMembersGrouped: config.drillMembersGrouped
    } : {})
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
            td => [
              annotation(
                configMap,
                MemberTypeEnum.DIMENSIONS,
              )(
                `${td.dimension}.${td.granularity}`
              )
            ].concat(
              // TODO: deprecated: backward compatibility for
              // referencing time dimensions without granularity
              dimensions.indexOf(td.dimension) === -1
                ? [
                  annotation(
                    configMap,
                    MemberTypeEnum.DIMENSIONS
                  )(td.dimension)
                ]
                : []
            ).filter(a => !!a)
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
