import {
  Meta,
  TCubeDimension,
  TCubeMeasure,
  TCubeSegment,
} from '@cubejs-client/core';

export class BuilderMeta {
  public measures: TCubeMeasure[];

  public dimensions: TCubeDimension[];

  public segments: TCubeSegment[];

  public timeDimensions: TCubeDimension[];

  public filters: Array<TCubeMeasure | TCubeDimension>;

  public constructor(public readonly meta: Meta) {
    this.mapMeta();
  }

  private mapMeta() {
    const allDimensions = this.meta.membersForQuery(
      null,
      'dimensions'
    ) as TCubeDimension[];

    this.measures = this.meta.membersForQuery(null, 'measures') as TCubeMeasure[];
    this.segments = this.meta.membersForQuery(null, 'segments');
    this.dimensions = allDimensions.filter(({ type }) => type !== 'time');
    this.timeDimensions = allDimensions.filter(({ type }) => type === 'time');
    this.filters = [...allDimensions, ...this.measures].map((member) => ({
      ...member,
      operators: this.meta.filterOperatorsForMember(member.name, [
        'dimensions',
        'measures',
      ]),
    }));
  }
}
