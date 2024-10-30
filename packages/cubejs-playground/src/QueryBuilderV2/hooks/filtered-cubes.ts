import { Cube, TCubeDimension, TCubeMeasure, TCubeSegment } from '@cubejs-client/core';

import { useRawFilter } from './raw-filter';

export function useFilteredCubes(filterString: string, cubes: Cube[]) {
  // Search filters logic
  const rawFilterFn = useRawFilter();

  const filterMemberFn = <T extends TCubeMeasure | TCubeDimension | TCubeSegment>(
    items: T[]
  ): T[] => {
    return items.filter((item: T) => {
      return rawFilterFn(item.name.split('.')[1], filterString);
    });
  };

  const members: string[] = [];

  const membersByCube = cubes.reduce(
    (acc, cube) => {
      acc[cube.name] = {
        measures: filterMemberFn(cube.measures).map((m) => m.name),
        dimensions: filterMemberFn(cube.dimensions).map((m) => m.name),
      };

      members.push(...acc[cube.name].dimensions, ...acc[cube.name].measures);

      return acc;
    },
    {} as Record<string, { dimensions: string[]; measures: string[] }>
  );

  return {
    isFiltered: !!filterString,
    cubes: cubes.filter((item) => {
      return (
        rawFilterFn(item.title, filterString) ||
        rawFilterFn(item.name, filterString) ||
        membersByCube[item.name].dimensions.length ||
        membersByCube[item.name].measures.length
      );
    }),
    membersByCube,
    members,
  };
}
