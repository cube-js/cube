import { unnest, fromPairs } from 'ramda';

const memberMap = (memberArray) => fromPairs(memberArray.map(m => [m.name, m]));

const operators = {
  string: [
    { name: 'contains', title: 'contains' },
    { name: 'notContains', title: 'does not contain' },
    { name: 'equals', title: 'equals' },
    { name: 'notEquals', title: 'does not equal' },
    { name: 'set', title: 'is set' },
    { name: 'notSet', title: 'is not set' }
  ],
  number: [
    { name: 'equals', title: 'equals' },
    { name: 'notEquals', title: 'does not equal' },
    { name: 'set', title: 'is set' },
    { name: 'notSet', title: 'is not set' },
    { name: 'gt', title: '>' },
    { name: 'gte', title: '>=' },
    { name: 'lt', title: '<' },
    { name: 'lte', title: '<=' }
  ]
};

export default class Meta {
  constructor(metaResponse) {
    this.meta = metaResponse;
    const { cubes } = this.meta;
    this.cubes = cubes;
    this.cubesMap = fromPairs(cubes.map(c => [
      c.name,
      { measures: memberMap(c.measures), dimensions: memberMap(c.dimensions), segments: memberMap(c.segments) }
    ]));
  }

  membersForQuery(query, memberType) {
    return unnest(this.cubes.map(c => c[memberType]));
  }

  resolveMember(memberName, memberType) {
    const [cube] = memberName.split('.');
    if (!this.cubesMap[cube]) {
      return { title: memberName, error: `Cube not found ${cube} for path '${memberName}'` };
    }
    const memberTypes = Array.isArray(memberType) ? memberType : [memberType];
    const member = memberTypes
      .map(type => this.cubesMap[cube][type] && this.cubesMap[cube][type][memberName])
      .find(m => m);
    if (!member) {
      return { title: memberName, error: `Path not found '${memberName}'` };
    }
    return member;
  }

  defaultTimeDimensionNameFor(memberName) {
    const [cube] = memberName.split('.');
    if (!this.cubesMap[cube]) {
      return null;
    }
    return Object.keys(this.cubesMap[cube].dimensions || {})
      .find(d => this.cubesMap[cube].dimensions[d].type === 'time');
  }

  filterOperatorsForMember(memberName, memberType) {
    const member = this.resolveMember(memberName, memberType);
    return operators[member.type] || operators.string;
  }
}
