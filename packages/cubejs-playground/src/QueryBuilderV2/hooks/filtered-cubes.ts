import { TCubeDimension, TCubeMeasure, TCubeSegment } from '@cubejs-client/core';

import { MemberViewType, TCubeFolder, TCubeHierarchy } from '../types';
import { getMemberSearchName } from '../utils';
import { Cube } from '../types';

import { useRawFilter } from './raw-filter';

export function useFilteredCubes(
  filterString: string,
  cubes: Cube[],
  memberViewType: MemberViewType
) {
  // Search filters logic
  const rawFilterFn = useRawFilter();
  const cubesMap = cubes.reduce(
    (acc, cube) => {
      acc[cube.name] = cube;

      return acc;
    },
    {} as Record<string, Cube>
  );

  const filterMemberFn = <
    T extends TCubeMeasure | TCubeDimension | TCubeSegment | TCubeFolder | TCubeHierarchy,
  >(
    items: T[]
  ): T[] => {
    return items.filter((item: T) => {
      const cubeName = item.name.split('.')[0];
      const cube = cubesMap[cubeName];
      const cubeId = cube ? getMemberSearchName(cube, memberViewType) : cubeName;
      const itemId = getMemberSearchName(item, memberViewType);

      return (
        rawFilterFn(cubeId ?? item.name, filterString) ||
        (itemId && rawFilterFn(itemId, filterString))
      );
    });
  };

  const members: string[] = [];

  const membersByCube = cubes.reduce(
    (acc, cube) => {
      acc[cube.name] = {
        // search by member names
        measures: filterMemberFn(cube.measures ?? []).map((m) => m.name),
        dimensions: filterMemberFn(cube.dimensions ?? []).map((m) => m.name),
        segments: filterMemberFn(cube.segments ?? []).map((m) => m.name),
        // search by folder names
        folders: filterMemberFn(cube.folders ?? []).map((m) => m.name),
        // search by hierarchy names
        hierarchies: filterMemberFn(cube.hierarchies ?? []).map((m) => m.name),
      };

      members.push(
        ...acc[cube.name].dimensions,
        ...acc[cube.name].measures,
        ...acc[cube.name].segments
      );

      return acc;
    },
    {} as Record<
      string,
      {
        dimensions: string[];
        measures: string[];
        segments: string[];
        folders: string[];
        hierarchies: string[];
      }
    >
  );

  return {
    isFiltered: !!filterString,
    cubes: cubes.filter((item) => {
      return (
        rawFilterFn(getMemberSearchName(item, memberViewType), filterString) ||
        membersByCube[item.name].dimensions.length ||
        membersByCube[item.name].measures.length ||
        membersByCube[item.name].segments.length ||
        membersByCube[item.name].folders.length ||
        membersByCube[item.name].hierarchies.length
      );
    }),
    membersByCube,
    members,
  };
}
