import { TCubeDimension, TCubeMeasure, TCubeSegment } from '@cubejs-client/core';

import { Cube, MemberViewType, TCubeFolder, TCubeHierarchy } from '../types';

export function getMemberSearchName(
  member: TCubeMeasure | TCubeDimension | TCubeSegment | TCubeFolder | TCubeHierarchy | Cube,
  memberViewType: MemberViewType
) {
  const name = member.name.split('.')[1] ?? member.name;

  if (memberViewType === 'name') {
    return name;
  }

  if ('shortTitle' in member) {
    return member.shortTitle ?? name;
  }

  if ('title' in member) {
    return member.title ?? name;
  }

  return name;
}
