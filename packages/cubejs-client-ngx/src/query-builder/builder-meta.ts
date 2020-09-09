import { Meta } from '@cubejs-client/core';

export class BuilderMeta {
  // todo: types
  measures: any[];
  dimensions: any[];
  segments: any[];
  timeDimensions: any[];

  constructor(private meta: Meta) {
    this.mapMeta();
  }

  private mapMeta() {
    const allDimensions = this.meta.membersForQuery(null, 'dimensions');
    
    this.measures = this.meta.membersForQuery(null, 'measures');
    this.segments = this.meta.membersForQuery(null, 'segments');
    this.dimensions = allDimensions.filter(({ type }) => type !== 'time');
    this.timeDimensions = allDimensions.filter(({ type }) => type === 'time');
  }
}