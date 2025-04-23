import { Filter, Query } from '@cubejs-client/core';

export function getUsedCubesAndMembers(query: Query, additionalMembers: string[] = []) {
  let usedMembers: string[] = [];

  let usedMembersInFilters = [
    ...extractMembersFromFilters(query?.filters || []),
    ...(query?.timeDimensions?.filter((td) => td.dateRange).map((td) => td.dimension) || []),
  ];

  let usedMembersInGrouping =
    query?.timeDimensions?.filter((td) => td.granularity).map((td) => td.dimension) || [];

  usedMembersInGrouping = usedMembersInGrouping.filter(
    (member, i) => usedMembersInGrouping.indexOf(member) === i
  );

  usedMembers.push(...(query?.dimensions || []));
  usedMembers.push(...(query?.measures || []));
  usedMembers.push(...(query?.segments || []));
  usedMembers.push(...usedMembersInFilters);
  usedMembers.push(...usedMembersInGrouping);

  const usedGranularities = [] as { dimension: string; granularities: string[] }[];

  query?.timeDimensions?.forEach((td) => {
    const dimension = td.dimension;

    if (!td.granularity) {
      return;
    }

    const timeDimension = usedGranularities.find((td) => td.dimension === dimension);

    if (timeDimension) {
      timeDimension.granularities.push(td.granularity);
    } else {
      usedGranularities.push({ dimension, granularities: [td.granularity] });
    }
  });

  let usedCubes: string[] = [];

  usedMembers.forEach((member) => {
    const cubeName = member.split('.')[0];
    if (!usedCubes.includes(cubeName)) {
      usedCubes.push(cubeName);
    }
  });

  const usedCubesInAdditionalMembers = additionalMembers.map((name) => name.split('.')[0]);

  usedCubes.push(...usedCubesInAdditionalMembers);

  const usedMembersInAdditionalMembers = additionalMembers.map((name) => name);

  usedMembersInFilters.push(...usedMembersInAdditionalMembers);
  usedMembers.push(...usedMembersInAdditionalMembers);

  // filter out duplicates of cubes
  usedCubes = usedCubes.filter((cube, i) => usedCubes.indexOf(cube) === i);
  // filter out duplicates of usedMembers
  usedMembers = usedMembers.filter((member, i) => usedMembers.indexOf(member) === i);
  // filter out duplicates of usedMembersInFilters
  usedMembersInFilters = usedMembersInFilters.filter(
    (member, i) => usedMembersInFilters.indexOf(member) === i
  );
  // filter out duplicates of usedMembersInGrouping
  usedMembersInGrouping = usedMembersInGrouping.filter(
    (member, i) => usedMembersInGrouping.indexOf(member) === i
  );

  return {
    usedCubes,
    usedMembers,
    usedMembersInGrouping,
    usedMembersInFilters,
    usedGranularities,
  };
}

export function extractMembersFromFilters(filters: Filter[]) {
  return filters.reduce((members, filter) => {
    if ('and' in filter) {
      members.push(...extractMembersFromFilters(filter.and));
    } else if ('or' in filter) {
      members.push(...extractMembersFromFilters(filter.or));
    }

    if ('member' in filter && filter.member) {
      members.push(filter.member);
    }

    return members;
  }, [] as string[]);
}

export function extractCubesFromFilters(filters: Filter[]) {
  return filters.reduce((members, filter) => {
    if ('and' in filter) {
      members.push(...extractCubesFromFilters(filter.and));
    } else if ('or' in filter) {
      members.push(...extractCubesFromFilters(filter.or));
    }

    if ('member' in filter && filter.member) {
      members.push(filter.member.split('.')[0]);
    }

    return members;
  }, [] as string[]);
}
