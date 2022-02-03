import R from 'ramda';
import { MetaConfig, MetaConfigMap, toConfigMap } from './toConfigMap';
import MemberType from '../enum/MemberType';

export

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

export

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
    ...(memberType === MemberType.MEASURES ? {
      drillMembers: config.drillMembers,
      drillMembersGrouped: config.drillMembersGrouped
    } : {})
  }];
};

export

/**
 * Returns annotations object by MetaConfigs and query.
 */
function prepareAnnotation(metaConfig: MetaConfig[], query: any) {
  const configMap = toConfigMap(metaConfig);
  const dimensions = (query.dimensions || []);
  return {
    measures: R.fromPairs(
      (query.measures || []).map(
        annotation(configMap, MemberType.MEASURES)
      ).filter(a => !!a)
    ),
    dimensions: R.fromPairs(
      dimensions
        .map(annotation(configMap, MemberType.DIMENSIONS))
        .filter(a => !!a)
    ),
    segments: R.fromPairs(
      (query.segments || [])
        .map(annotation(configMap, MemberType.SEGMENTS))
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
                MemberType.DIMENSIONS
              )(
                `${td.dimension}.${td.granularity}`
              )
            ].concat(
              // TODO: deprecated: backward compatibility for
              // referencing time dimensions without granularity
              dimensions.indexOf(td.dimension) === -1
                ? [
                  annotation(configMap, MemberType.DIMENSIONS)(td.dimension)
                ]
                : []
            ).filter(a => !!a)
          )
      )
    ),
  };
}
