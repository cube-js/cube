import { AvailableCube, AvailableMembers } from '@cubejs-client/react';
import { MemberType, TCubeMember } from '@cubejs-client/core';

export function ucfirst(s: string): string {
  return s[0].toUpperCase() + s.slice(1);
}

export function getNameMemberPairs(members: AvailableCube[]) {
  const items: [string, TCubeMember & MemberType][] = [];

  members.forEach((cube) =>
    cube.members.forEach((member) => {
      items.push([member.name, member]);
    })
  );

  return items;
}

export type MembersByCube = {
  cubeName: string;
  cubeTitle: string;
  measures: TCubeMember[];
  dimensions: TCubeMember[];
  timeDimensions: TCubeMember[];
}

export function getMembersByCube(availableMembers: AvailableMembers): MembersByCube[] {
  const membersByCube: Record<string, MembersByCube> = {};

  Object.entries(availableMembers).forEach(([memberType, cubes]) => {
    cubes.forEach((cube) => {
      if (!membersByCube[cube.cubeName]) {
        membersByCube[cube.cubeName] = {
          cubeName: cube.cubeName,
          cubeTitle: cube.cubeTitle,
          measures: [],
          dimensions: [],
          timeDimensions: [],
        }
      }

      cube.members.forEach((member) => {
        membersByCube[cube.cubeName] = {
          ...membersByCube[cube.cubeName],
          [memberType]: [
            ...membersByCube[cube.cubeName][memberType],
            member
          ]
        }
      })
    })
  });

  return Object.values(membersByCube);
}
