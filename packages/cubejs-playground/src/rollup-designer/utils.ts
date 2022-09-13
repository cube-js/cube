import {
  Query,
  TimeDimensionBase,
  TimeDimensionGranularity,
  TransformedQuery,
} from '@cubejs-client/core';
import { camelCase } from 'camel-case';

import { QueryMemberKey } from '../types';
import { RollupSettings } from './components/Settings';

export type PreAggregationReferences = {
  measures: string[];
  dimensions: string[];
  segments: string[];
  timeDimensions: TimeDimensionBase[];
  timeDimension?: string;
  granularity?: TimeDimensionGranularity;
};

export type PreAggregationDefinition = {
  preAggregationName: string;
  cubeName: string;
  code: Object;
};

export function getPreAggregationReferences(
  transformedQuery: TransformedQuery | null,
  segments: Set<string>
): PreAggregationReferences {
  const references: PreAggregationReferences = {
    measures: [],
    dimensions: [],
    segments: [],
    timeDimensions: [],
  };

  if (!transformedQuery) {
    return references;
  }

  if (transformedQuery.leafMeasures.length) {
    references.measures = [...transformedQuery.leafMeasures];
  }

  if (transformedQuery.sortedDimensions.length) {
    references.dimensions = [
      ...transformedQuery.sortedDimensions.filter(
        (name) => !segments.has(name)
      ),
    ];
    references.segments = [
      ...transformedQuery.sortedDimensions.filter((name) => segments.has(name)),
    ];
  }

  if (transformedQuery.sortedTimeDimensions?.[0]?.[0]) {
    const [dimension, granularity] = transformedQuery.sortedTimeDimensions[0];
    references.timeDimensions = [
      {
        dimension,
        granularity: <TimeDimensionGranularity>granularity || 'day',
      },
    ];
  }

  return references;
}

export function areReferencesEmpty(
  references: PreAggregationReferences
): boolean {
  for (const [, value] of Object.entries(references)) {
    if (Array.isArray(value) && value.length > 0) {
      return false;
    }
  }

  return true;
}

type PreAggregationDefinitionResult = {
  code: string;
  value: Object;
};

export function getRollupDefinitionFromReferences(
  references: PreAggregationReferences,
  name: string = 'main',
  settings: RollupSettings
): PreAggregationDefinitionResult {
  const { timeDimensions, ...otherReferences } = references;
  const code: Record<string, any> = {
    ...Object.entries(otherReferences).reduce(
      (memo, [key, value]) => ({
        ...memo,
        ...(Array.isArray(value) && value.length ? { [key]: value } : null),
      }),
      {}
    ),
    ...settings,
  };

  if (timeDimensions.length) {
    const { dimension, granularity } = references.timeDimensions[0];

    code.timeDimension = dimension;

    if (granularity) {
      code.granularity = `\`${granularity}\``;
    }
  }

  const value = JSON.stringify(code, null, 2).replaceAll('"', '');

  return {
    code: `${camelCase(name)}: ${value}`,
    value,
  };
}

export function updateQuery(
  query: Query,
  memberType: QueryMemberKey,
  key: string
) {
  const updatedQuery: Query = JSON.parse(JSON.stringify(query));

  if (memberType === 'timeDimensions') {
    if (updatedQuery.timeDimensions?.[0]?.dimension === key) {
      updatedQuery.timeDimensions = [];
    } else {
      updatedQuery.timeDimensions = [
        {
          // default granularity
          granularity: 'day',
          ...updatedQuery.timeDimensions?.[0],
          dimension: key,
        },
      ];
    }
  } else {
    if (updatedQuery[memberType]?.includes(key)) {
      updatedQuery[memberType] = updatedQuery[memberType]!.filter(
        (k) => key !== k
      );
    } else {
      updatedQuery[memberType] = [...(updatedQuery[memberType] || []), key];
    }
  }

  return updatedQuery;
}

function isBuffer(obj) {
  return (
    typeof obj?.constructor?.isBuffer === 'function' &&
    obj.constructor.isBuffer(obj)
  );
}

function keyIdentity(key) {
  return key;
}

type FlattenOptions = {
  safe?: boolean;
  maxDepth?: number;
  delimiter?: string;
  transformKey?: (key: string) => string;
};

export function flatten(target: Object, opts: FlattenOptions = {}) {
  const delimiter = opts.delimiter || '.';
  const maxDepth = opts.maxDepth || 100;
  const transformKey = opts.transformKey || keyIdentity;
  const output = {};

  function step(object, prev = null, keyDepth = 1) {
    Object.keys(object).forEach(function (key) {
      const value = object[key];
      const isArray = opts.safe && Array.isArray(value);
      const type = Object.prototype.toString.call(value);
      const isbuffer = isBuffer(value);
      const isobject = type === '[object Object]' || type === '[object Array]';

      const newKey = prev
        ? prev + delimiter + transformKey(key)
        : transformKey(key);

      if (
        !isArray &&
        !isbuffer &&
        isobject &&
        Object.keys(value).length &&
        (!opts.maxDepth || keyDepth < maxDepth)
      ) {
        return step(value, newKey, keyDepth + 1);
      }

      output[newKey] = value;
    });
  }

  step(target);

  return output;
}

export function buildSettings(
  values: Record<string, string | boolean | number>
) {
  const nextSettings: RollupSettings = {};

  if (values['refreshKey.checked.every']) {
    if (
      values['refreshKey.isCron'] &&
      (values['refreshKey.cron'] || values['refreshKey.timeZone'])
    ) {
      nextSettings.refreshKey = {};

      if (values['refreshKey.cron']) {
        nextSettings.refreshKey.every = `\`${values['refreshKey.cron']}\``;
      }

      if (values['refreshKey.timeZone']) {
        nextSettings.refreshKey.timezone = `\`${values['refreshKey.timeZone']}\``;
      }
    } else {
      nextSettings.refreshKey = {
        every: `\`${values['refreshKey.value']} ${values['refreshKey.granularity']}\``,
      };
    }
  }

  if (values['refreshKey.checked.sql'] && values['refreshKey.sql']) {
    nextSettings.refreshKey = {
      ...nextSettings.refreshKey,
      sql: `\`${values['refreshKey.sql']}\``,
    };
  }

  if (values.partitionGranularity) {
    nextSettings.partitionGranularity = `\`${values.partitionGranularity}\``;

    if (values['updateWindow.value']) {
      const value = [
        values['updateWindow.value'],
        values['updateWindow.granularity'],
      ].join(' ');

      nextSettings.refreshKey = {
        ...nextSettings.refreshKey,
        updateWindow: `\`${value}\``,
      };
    }

    nextSettings.refreshKey = {
      ...nextSettings.refreshKey,
      incremental: Boolean(values['incrementalRefresh']),
    };
  }

  if (Array.isArray(values.indexes) && values.indexes.length > 0) {
    nextSettings.indexes = {
      indexName: {
        columns: values.indexes,
      },
    };
  }

  return nextSettings;
}
