import {unnest, fromPairs} from 'ramda';

const memberMap = (memberArray) => fromPairs(memberArray.map(m => [m.name, m]));

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
    const member = this.cubesMap[cube][memberType][memberName];
    if (!member) {
      return { title: memberName, error: `Path not found '${memberName}'` };
    }
    return member;
  }
}
