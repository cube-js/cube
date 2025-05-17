import { unnest, fromPairs } from 'ramda';
import {
  Cube,
  CubesMap,
  MemberType,
  MetaResponse,
  TCubeMeasure,
  TCubeDimension,
  TCubeMember,
  TCubeMemberByType,
  Query,
  FilterOperator,
  TCubeSegment,
  NotFoundMember,
} from './types';
import { DeeplyReadonly } from './index';

export interface CubeMemberWrapper<T> {
  cubeName: string;
  cubeTitle: string;
  type: 'view' | 'cube';
  public: boolean;
  members: T[];
}

export type AggregatedMembers = {
  measures: CubeMemberWrapper<TCubeMeasure>[];
  dimensions: CubeMemberWrapper<TCubeDimension>[];
  segments: CubeMemberWrapper<TCubeSegment>[];
  timeDimensions: CubeMemberWrapper<TCubeDimension>[];
};

const memberMap = (memberArray: any[]) => fromPairs(
  memberArray.map((m) => [m.name, m])
);

const operators = {
  string: [
    { name: 'contains', title: 'contains' },
    { name: 'notContains', title: 'does not contain' },
    { name: 'equals', title: 'equals' },
    { name: 'notEquals', title: 'does not equal' },
    { name: 'set', title: 'is set' },
    { name: 'notSet', title: 'is not set' },
    { name: 'startsWith', title: 'starts with' },
    { name: 'notStartsWith', title: 'does not start with' },
    { name: 'endsWith', title: 'ends with' },
    { name: 'notEndsWith', title: 'does not end with' },
  ],
  number: [
    { name: 'equals', title: 'equals' },
    { name: 'notEquals', title: 'does not equal' },
    { name: 'set', title: 'is set' },
    { name: 'notSet', title: 'is not set' },
    { name: 'gt', title: '>' },
    { name: 'gte', title: '>=' },
    { name: 'lt', title: '<' },
    { name: 'lte', title: '<=' },
  ],
  time: [
    { name: 'equals', title: 'equals' },
    { name: 'notEquals', title: 'does not equal' },
    { name: 'inDateRange', title: 'in date range' },
    { name: 'notInDateRange', title: 'not in date range' },
    { name: 'afterDate', title: 'after date' },
    { name: 'afterOrOnDate', title: 'after or on date' },
    { name: 'beforeDate', title: 'before date' },
    { name: 'beforeOrOnDate', title: 'before or on date' },
  ],
};

/**
 * Contains information about available cubes and it's members.
 */
export default class Meta {
  /**
   * Raw meta response
   */
  public readonly meta: MetaResponse;

  /**
   * An array of all available cubes with their members
   */
  public readonly cubes: Cube[];

  /**
   * A map of all cubes where the key is a cube name
   */
  public readonly cubesMap: CubesMap;

  public constructor(metaResponse: MetaResponse) {
    this.meta = metaResponse;
    const { cubes } = this.meta;
    this.cubes = cubes;
    this.cubesMap = fromPairs(
      cubes.map((c) => [
        c.name,
        {
          measures: memberMap(c.measures),
          dimensions: memberMap(c.dimensions),
          segments: memberMap(c.segments),
        },
      ])
    );
  }

  /**
   * Get all members of a specific type for a given query.
   * If empty query is provided no filtering is done based on query context and all available members are retrieved.
   * @param _query - context query to provide filtering of members available to add to this query
   * @param memberType
   */
  public membersForQuery(_query: DeeplyReadonly<Query> | null, memberType: MemberType): (TCubeMeasure | TCubeDimension | TCubeMember | TCubeSegment)[] {
    return unnest(this.cubes.map((c) => c[memberType]))
      .sort((a, b) => (a.title > b.title ? 1 : -1));
  }

  public membersGroupedByCube() {
    const memberKeys = ['measures', 'dimensions', 'segments', 'timeDimensions'];

    return this.cubes.reduce<AggregatedMembers>(
      (memo, cube) => {
        memberKeys.forEach((key) => {
          let members: TCubeMeasure[] | TCubeDimension[] | TCubeSegment[] = [];

          // eslint-disable-next-line default-case
          switch (key) {
            case 'measures':
              members = cube.measures || [];
              break;
            case 'dimensions':
              members = cube.dimensions || [];
              break;
            case 'segments':
              members = cube.segments || [];
              break;
            case 'timeDimensions':
              members = cube.dimensions.filter((m) => m.type === 'time') || [];
              break;
          }

          // TODO: Convince TS this is working
          // @ts-ignore
          memo[key].push({
            cubeName: cube.name,
            cubeTitle: cube.title,
            type: cube.type,
            public: cube.public,
            members,
          });
        });

        return memo;
      },
      {
        measures: [],
        dimensions: [],
        segments: [],
        timeDimensions: [],
      } as AggregatedMembers
    );
  }

  /**
   * Get meta information for a cube member
   * meta information contains:
   * ```javascript
   * {
   *   name,
   *   title,
   *   shortTitle,
   *   type,
   *   description,
   *   format
   * }
   * ```
   * @param memberName - Fully qualified member name in a form `Cube.memberName`
   * @param memberType
   * @return An object containing meta information about member
   */
  public resolveMember<T extends MemberType>(
    memberName: string,
    memberType: T | T[]
  ): NotFoundMember | TCubeMemberByType<T> {
    const [cube] = memberName.split('.');

    if (!this.cubesMap[cube]) {
      return { title: memberName, error: `Cube not found ${cube} for path '${memberName}'` };
    }

    const memberTypes = Array.isArray(memberType) ? memberType : [memberType];
    const member = memberTypes
      .map((type) => this.cubesMap[cube][type] && this.cubesMap[cube][type][memberName])
      .find((m) => m);

    if (!member) {
      return {
        title: memberName,
        error: `Path not found '${memberName}'`,
      };
    }

    return member as TCubeMemberByType<T>;
  }

  public defaultTimeDimensionNameFor(memberName: string): string | null | undefined {
    const [cube] = memberName.split('.');
    if (!this.cubesMap[cube]) {
      return null;
    }
    return Object.keys(this.cubesMap[cube].dimensions || {}).find(
      (d) => this.cubesMap[cube].dimensions[d].type === 'time'
    );
  }

  public filterOperatorsForMember(memberName: string, memberType: MemberType | MemberType[]): FilterOperator[] {
    const member = this.resolveMember(memberName, memberType);

    if ('error' in member || !('type' in member) || member.type === 'boolean') {
      return operators.string;
    }

    return operators[member.type] || operators.string;
  }
}
