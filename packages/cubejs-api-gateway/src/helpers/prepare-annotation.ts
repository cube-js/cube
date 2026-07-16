/**
 * @license Apache-2.0
 * @copyright Cube Dev, Inc.
 * @fileoverview
 * prepareAnnotation function and related types definition.
 */

import R from 'ramda';
import { MetaConfig, MetaConfigMap, toConfigMap } from './to-config-map';
import { MemberType } from '../types/strings';
import { MemberType as MemberTypeEnum } from '../types/enums';
import { MemberExpression } from '../types/query';

type GranularityMeta = {
  name: string;
  type?: 'built-in' | 'custom';
  title: string;
  /** d3-time-format string for displaying bucketed timestamps. */
  format?: string;
  interval: string;
  offset?: string;
  origin?: string;
};

// Resolves the effective granularity for a queried time dimension against the request's global
// config, so the load path doesn't have to enrich the whole meta. `dimension` is `cube.member`.
export type GranularityResolverFn = (dimension: string, granularity: string) => GranularityMeta | undefined;

/**
 * Annotation item for cube's member.
 */
type ConfigItem = {
  title: string;
  shortTitle: string;
  description: string;
  type: string;
  format: string;
  /** ISO 4217 currency code in uppercase (e.g. USD, EUR) */
  currency?: string;
  meta: any;
  drillMembers?: any[];
  drillMembersGrouped?: any;
  granularities?: GranularityMeta[];
};

type AnnotatedConfigItem = Omit<ConfigItem, 'granularities'> & {
  granularity?: GranularityMeta;
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
  const cubeConfig = configMap[cubeName];
  const config: ConfigItem = cubeConfig && cubeConfig[memberType]
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
    currency: config.currency,
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
 * `resolveGranularity` computes the effective granularity meta for a queried time dimension.
 */
function prepareAnnotation(metaConfig: MetaConfig[], query: any, resolveGranularity?: GranularityResolverFn) {
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

              let dimAnnotation: [string, AnnotatedConfigItem] | undefined;

              if (an) {
                const granularityMeta = resolveGranularity?.(td.dimension, td.granularity);
                const { granularities: _, ...rest } = an[1];
                dimAnnotation = [an[0], { ...rest, granularity: granularityMeta }];
              }

              // TODO: deprecated: backward compatibility for
              // referencing time dimensions without granularity
              if (dimensions.indexOf(td.dimension) !== -1) {
                return [dimAnnotation].filter(a => !!a);
              }

              const dimWithoutGranularity = annotation(
                configMap,
                MemberTypeEnum.DIMENSIONS
              )(td.dimension);

              if (dimWithoutGranularity && dimWithoutGranularity[1].granularities) {
                // no need to populate granularities here
                dimWithoutGranularity[1].granularities = undefined;
              }

              return [dimAnnotation].concat([dimWithoutGranularity])
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
  GranularityMeta,
  annotation,
  prepareAnnotation,
};
