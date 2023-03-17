import { BaseCubeMember, MemberType } from '@cubejs-client/core';
import { AvailableCube, AvailableMembers } from '@cubejs-client/react';

export function getNameMemberPairs(members: AvailableCube[]) {
  const items: [memberName: string, member: BaseCubeMember & MemberType][] = [];

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
  measures: BaseCubeMember[];
  dimensions: BaseCubeMember[];
  segments: BaseCubeMember[];
  timeDimensions: BaseCubeMember[];
};

export function getMembersByCube(
  availableMembers: AvailableMembers
): MembersByCube[] {
  const membersByCube: Record<string, MembersByCube> = {};

  Object.entries(availableMembers).forEach(([memberType, cubes]) => {
    cubes.forEach((cube) => {
      if (!membersByCube[cube.cubeName]) {
        membersByCube[cube.cubeName] = {
          cubeName: cube.cubeName,
          cubeTitle: cube.cubeTitle,
          measures: [],
          dimensions: [],
          segments: [],
          timeDimensions: [],
        };
      }

      cube.members.forEach((member) => {
        membersByCube[cube.cubeName] = {
          ...membersByCube[cube.cubeName],
          [memberType]: [...membersByCube[cube.cubeName][memberType], member],
        };
      });
    });
  });

  return Object.values(membersByCube);
}