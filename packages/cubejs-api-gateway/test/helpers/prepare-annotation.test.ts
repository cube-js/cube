/**
 * @license Apache-2.0
 * @copyright Cube Dev, Inc.
 * @fileoverview prepareAnnotation related helpers unit tests.
 */

/* globals describe,test,expect */
/* eslint-disable import/no-duplicates */

import { MemberType } from '../../src/types/enums';
import prepareAnnotationDef
  from '../../src/helpers/prepare-annotation';
import {
  annotation,
  prepareAnnotation,
} from '../../src/helpers/prepare-annotation';

describe('prepareAnnotation helpers', () => {
  test('export looks as expected', () => {
    expect(prepareAnnotationDef).toBeDefined();
    expect(annotation).toBeDefined();
    expect(prepareAnnotation).toBeDefined();
    expect(prepareAnnotation).toEqual(prepareAnnotationDef);
  });
  test('annotation without config returns void', () => {
    // for measures
    expect(annotation({
      cube_name: ({
        name: 'cube_name',
        title: 'cube name',
        measures: [{
          name: 'cube_name.measures'
        }],
      }) as { name: string; title: string; }
    }, MemberType.MEASURES)('cube_name.measures')).toBeDefined();
    expect(annotation({
      cube_name: ({
        name: 'cube_name',
        title: 'cube name',
        measures: [{
          name: 'cube_name.measures'
        }],
      }) as { name: string; title: string; }
    }, MemberType.MEASURES)('cube_name.undefined')).toBeUndefined();
    // for dimensions
    expect(annotation({
      cube_name: ({
        name: 'cube_name',
        title: 'cube name',
        dimensions: [{
          name: 'cube_name.dimensions'
        }],
      }) as { name: string; title: string; }
    }, MemberType.DIMENSIONS)('cube_name.dimensions')).toBeDefined();
    expect(annotation({
      cube_name: ({
        name: 'cube_name',
        title: 'cube name',
        dimensions: [{
          name: 'cube_name.dimensions'
        }],
      }) as { name: string; title: string; }
    }, MemberType.DIMENSIONS)('cube_name.undefined')).toBeUndefined();
    // for segments
    expect(annotation({
      cube_name: ({
        name: 'cube_name',
        title: 'cube name',
        segments: [{
          name: 'cube_name.segments'
        }],
      }) as { name: string; title: string; }
    }, MemberType.SEGMENTS)('cube_name.segments')).toBeDefined();
    expect(annotation({
      cube_name: ({
        name: 'cube_name',
        title: 'cube name',
        segments: [{
          name: 'cube_name.segments'
        }],
      }) as { name: string; title: string; }
    }, MemberType.SEGMENTS)('cube_name.undefined')).toBeUndefined();
  });
  test('prepareAnnotation with empty parameters', () => {
    expect(
      Object.keys(prepareAnnotation([], {}).dimensions)
    ).toHaveLength(0);
    expect(
      Object.keys(prepareAnnotation([], {}).measures)
    ).toHaveLength(0);
    expect(
      Object.keys(prepareAnnotation([], {}).segments)
    ).toHaveLength(0);
    expect(
      Object.keys(prepareAnnotation([], {}).timeDimensions)
    ).toHaveLength(0);
  });
  test('prepareAnnotation with unmapped parameters', () => {
    // dimensions
    expect(
      prepareAnnotation([{
        config: ({
          name: 'cube_name',
          title: 'cube name',
          dimensions: [{
            name: 'cube_name.member',
          }],
        }) as { name: string; title: string; },
      }], {
        dimensions: ['cube_name.undefined'],
      }).dimensions
    ).toEqual({});
    // measures
    expect(
      prepareAnnotation([{
        config: ({
          name: 'cube_name',
          title: 'cube name',
          measures: [{
            name: 'cube_name.member',
          }],
        }) as { name: string; title: string; },
      }], {
        measures: ['cube_name.undefined'],
      }).measures
    ).toEqual({});
    // segments
    expect(
      prepareAnnotation([{
        config: ({
          name: 'cube_name',
          title: 'cube name',
          segments: [{
            name: 'cube_name.member',
          }],
        }) as { name: string; title: string; },
      }], {
        segments: ['cube_name.undefined'],
      }).segments
    ).toEqual({});
  });
  test('prepareAnnotation with mapped parameters', () => {
    // query segments
    expect(
      prepareAnnotation([{
        config: ({
          name: 'cube_name',
          title: 'cube name',
          segments: [{
            name: 'cube_name.member',
          }],
        }) as { name: string; title: string; },
      }], {
        segments: ['cube_name.member'],
      }).segments
    ).toEqual({
      'cube_name.member': {
        description: undefined,
        format: undefined,
        meta: undefined,
        shortTitle: undefined,
        title: undefined,
        type: undefined,
      }
    });

    // query timeDimensions
    expect(
      prepareAnnotation([{
        config: ({
          name: 'cube_name',
          title: 'cube name',
          dimensions: [{
            name: 'cube_name.member',
          }],
        }) as { name: string; title: string; },
      }], {
        timeDimensions: [{
          dimension: 'cube_name.member',
          granularity: 'day',
        }],
      }).timeDimensions
    ).toEqual({
      'cube_name.member': {
        currency: undefined,
        description: undefined,
        format: undefined,
        meta: undefined,
        shortTitle: undefined,
        title: undefined,
        type: undefined,
      },
      'cube_name.member.day': {
        currency: undefined,
        description: undefined,
        format: undefined,
        meta: undefined,
        shortTitle: undefined,
        title: undefined,
        type: undefined,
        granularity: {
          name: 'day',
          type: 'built-in',
          title: 'Day',
          interval: '1 day',
          format: '%Y-%m-%d',
        }
      },
    });

    // query dimensions and timeDimensions without granularity
    expect(
      prepareAnnotation([{
        config: ({
          name: 'cube_name',
          title: 'cube name',
          dimensions: [{
            name: 'cube_name.member',
          }],
        }) as { name: string; title: string; },
      }], {
        dimensions: ['cube_name.member'],
        timeDimensions: [{
          dimension: 'cube_name.member',
        }],
      }).timeDimensions
    ).toEqual({});

    // query timeDimensions without granularity
    expect(
      prepareAnnotation([{
        config: ({
          name: 'cube_name',
          title: 'cube name',
          dimensions: [{
            name: 'cube_name.member',
          }],
        }) as { name: string; title: string; },
      }], {
        timeDimensions: [{
          dimension: 'cube_name.member',
        }],
      }).timeDimensions
    ).toEqual({});
  });

  describe('granularity resolution from effectiveGranularities', () => {
    const metaConfig = (effectiveGranularities?: any[]) => [{
      config: ({
        name: 'cube_name',
        title: 'cube name',
        dimensions: [{
          name: 'cube_name.member',
          type: 'time',
          ...(effectiveGranularities ? { effectiveGranularities } : {}),
        }],
      }) as { name: string; title: string; },
    }];

    const tdQuery = (granularity: string) => ({
      dimensions: ['cube_name.member'],
      timeDimensions: [{ dimension: 'cube_name.member', granularity }],
    });

    test('reads the queried granularity from the effective set (global override honored)', () => {
      const result = prepareAnnotation(
        metaConfig([
          { name: 'day', type: 'built-in', title: 'Tag', interval: '1 day', format: '%d.%m.%Y' },
          { name: 'fiscal_year', type: 'custom', title: 'Fiscal Year', interval: '1 year', origin: '2024-02-01' },
        ]),
        tdQuery('day'),
      );
      expect((result.timeDimensions['cube_name.member.day'] as any).granularity).toEqual({
        name: 'day', type: 'built-in', title: 'Tag', interval: '1 day', format: '%d.%m.%Y',
      });
    });

    test('resolves a custom granularity from the effective set', () => {
      const result = prepareAnnotation(
        metaConfig([
          { name: 'fiscal_year', type: 'custom', title: 'Fiscal Year', interval: '1 year', origin: '2024-02-01' },
        ]),
        tdQuery('fiscal_year'),
      );
      expect((result.timeDimensions['cube_name.member.fiscal_year'] as any).granularity).toEqual({
        name: 'fiscal_year', type: 'custom', title: 'Fiscal Year', interval: '1 year', origin: '2024-02-01',
      });
    });

    test('synthesizes a config-disabled built-in from defaults', () => {
      const result = prepareAnnotation(
        metaConfig([
          { name: 'year', type: 'built-in', title: 'Year', interval: '1 year', format: '%Y' },
        ]),
        tdQuery('day'),
      );
      expect((result.timeDimensions['cube_name.member.day'] as any).granularity).toEqual({
        name: 'day', type: 'built-in', title: 'Day', interval: '1 day', format: '%Y-%m-%d',
      });
    });

    test('unknown custom granularity yields undefined, never the legacy array', () => {
      const result = prepareAnnotation(
        metaConfig([
          { name: 'day', type: 'built-in', title: 'Day', interval: '1 day', format: '%Y-%m-%d' },
        ]),
        tdQuery('some_custom'),
      );
      expect((result.timeDimensions['cube_name.member.some_custom'] as any).granularity).toBeUndefined();
    });
  });
});
