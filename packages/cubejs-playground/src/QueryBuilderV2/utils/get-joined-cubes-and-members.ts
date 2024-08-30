import { Filter, Query } from '@cubejs-client/core';

export function getJoinedCubesAndMembers(query: Query) {
  const joinedMembers: string[] = [];

  joinedMembers.push(...(query?.dimensions || []));
  joinedMembers.push(...(query?.measures || []));
  joinedMembers.push(...((query?.timeDimensions || []).map((td) => td.dimension) || []));

  const joinedCubes: string[] = [];

  joinedMembers.forEach((member) => {
    if (!joinedCubes.includes(member.split('.')[0])) {
      joinedCubes.push(member.split('.')[0]);
    }
  });

  const usedCubes = joinedCubes.slice();
  const usedMembers = joinedMembers.slice();

  usedCubes.push(...extractCubesFromFilters(query?.filters || []));

  usedMembers.push(...extractMembersFromFilters(query?.filters || []));

  query?.segments?.forEach((segment) => {
    if (!usedCubes.includes(segment.split('.')[0])) {
      usedCubes.push(segment.split('.')[0]);
    }
  });

  return { joinedCubes, joinedMembers, usedCubes, usedMembers };
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
