import { TCubeDimension, TCubeMeasure, TCubeSegment } from '@cubejs-client/core';

import { Cube, MemberViewType, TCubeFolder, TCubeHierarchy } from '../types';

export function getMemberSearchName(
  member: TCubeMeasure | TCubeDimension | TCubeSegment | TCubeFolder | TCubeHierarchy | Cube,
  memberViewType: MemberViewType
) {
  const name = member.name.split('.')[1] ?? member.name;

  return (
    (memberViewType === 'name'
      ? name
      : 'shortTitle' in member
        ? member.shortTitle
        : 'title' in member
          ? member.title
          : name) ?? name
  );
}
