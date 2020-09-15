import { Meta, TCubeDimension, TCubeMeasure, TCubeMember } from '@cubejs-client/core';

export class BuilderMeta {
  measures: TCubeMeasure[];
  dimensions: TCubeDimension[];
  segments: TCubeMember[];
  timeDimensions: TCubeDimension[];

  constructor(private meta: Meta) {
    this.mapMeta();
  }

  private mapMeta() {
    const allDimensions = this.meta.membersForQuery(null, 'dimensions') as TCubeDimension[];
    
    this.measures = this.meta.membersForQuery(null, 'measures') as TCubeMeasure[];
    this.segments = this.meta.membersForQuery(null, 'segments');
    this.dimensions = allDimensions.filter(({ type }) => type !== 'time');
    this.timeDimensions = allDimensions.filter(({ type }) => type === 'time');
  }
}