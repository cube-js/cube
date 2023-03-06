/**
 * @module @cubejs-client/core
 */

import { unnest, fromPairs } from 'ramda';

const memberMap = (memberArray) => fromPairs(memberArray.map((m) => [m.name, m]));

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
    { name: 'beforeDate', title: 'before date' },
  ],
};

/**
 * Contains information about available cubes and it's members.
 */
class Meta {
  constructor(metaResponse) {
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

  membersForQuery(query, memberType) {
    return unnest(this.cubes.map((c) => c[memberType])).sort((a, b) => (a.title > b.title ? 1 : -1));
  }

  membersGroupedByCube() {
    const memberKeys = ['measures', 'dimensions', 'segments', 'timeDimensions'];

    return this.cubes.reduce(
      (memo, cube) => {
        memberKeys.forEach((key) => {
          let members = cube[key];

          if (key === 'timeDimensions') {
            members = cube.dimensions.filter((m) => m.type === 'time');
          }

          memo[key] = [
            ...memo[key],
            {
              cubeName: cube.name,
              cubeTitle: cube.title,
              members
            },
          ];
        });

        return memo;
      },
      {
        measures: [],
        dimensions: [],
        segments: [],
        timeDimensions: [],
      }
    );
  }

  resolveMember(memberName, memberType) {
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

    return member;
  }

  defaultTimeDimensionNameFor(memberName) {
    const [cube] = memberName.split('.');
    if (!this.cubesMap[cube]) {
      return null;
    }
    return Object.keys(this.cubesMap[cube].dimensions || {}).find(
      (d) => this.cubesMap[cube].dimensions[d].type === 'time'
    );
  }

  filterOperatorsForMember(memberName, memberType) {
    const member = this.resolveMember(memberName, memberType);

    return operators[member.type] || operators.string;
  }
}

export default Meta;
