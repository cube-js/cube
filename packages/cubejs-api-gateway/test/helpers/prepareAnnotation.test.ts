/**
 * @license Apache-2.0
 * @copyright Cube Dev, Inc.
 * @fileoverview prepareAnnotation related helpers unit tests.
 */

/* globals describe,test,expect */
/* eslint-disable import/no-duplicates */

import { MemberType } from '../../src/types/enums';
import prepareAnnotationDef
  from '../../src/helpers/prepareAnnotation';
import {
  annotation,
  prepareAnnotation,
} from '../../src/helpers/prepareAnnotation';

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
        description: undefined,
        format: undefined,
        meta: undefined,
        shortTitle: undefined,
        title: undefined,
        type: undefined,
      },
      'cube_name.member.day': {
        description: undefined,
        format: undefined,
        meta: undefined,
        shortTitle: undefined,
        title: undefined,
        type: undefined,
        granularity: {
          name: 'day',
          title: 'day',
          interval: '1 day',
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
});
