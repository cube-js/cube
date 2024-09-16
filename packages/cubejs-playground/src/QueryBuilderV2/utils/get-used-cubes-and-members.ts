import { Filter, Query } from '@cubejs-client/core';

export function getUsedCubesAndMembers(query: Query) {
  const usedMembers: string[] = [];

  usedMembers.push(...(query?.dimensions || []));
  usedMembers.push(...(query?.measures || []));
  usedMembers.push(...((query?.timeDimensions || []).map((td) => td.dimension) || []));
  usedMembers.push(...(query?.segments || []));
  usedMembers.push(...extractMembersFromFilters(query?.filters || []));

  const usedCubes: string[] = [];

  usedMembers.forEach((member) => {
    if (!usedCubes.includes(member.split('.')[0])) {
      usedCubes.push(member.split('.')[0]);
    }
  });

  return { usedCubes, usedMembers };
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
