import { TCubeDimension, TCubeMeasure, TCubeSegment } from '@cubejs-client/core';

import { MemberViewType, TCubeFolder, TCubeHierarchy } from '../types';
import { getMemberSearchName } from '../utils/get-member-search-name';

import { useEvent } from './event';
import { useRawFilter } from './raw-filter';

export function useFilteredMembers(
  filterString: string,
  members: {
    dimensions: TCubeDimension[];
    measures: TCubeMeasure[];
    segments: TCubeSegment[];
    folders: TCubeFolder[];
    hierarchies: TCubeHierarchy[];
  },
  memberViewType: MemberViewType = 'name'
) {
  // Search filters logic
  const rawFilterFn = useRawFilter();

  const { dimensions, measures, segments, folders, hierarchies } = members;

  const sortFn = (
    a: TCubeMeasure | TCubeDimension | TCubeSegment | TCubeFolder | TCubeHierarchy,
    b: TCubeMeasure | TCubeDimension | TCubeSegment | TCubeFolder | TCubeHierarchy
  ) => {
    const aName = getMemberSearchName(a, memberViewType);
    const bName = getMemberSearchName(b, memberViewType);

    return aName.localeCompare(bName);
  };

  const filterMemberFn = useEvent(
    <T extends TCubeMeasure | TCubeDimension | TCubeSegment | TCubeFolder | TCubeHierarchy>(
      items: T[]
    ): T[] => {
      return items.filter((item: T) => {
        const itemId = getMemberSearchName(item, memberViewType);

        return itemId && rawFilterFn(itemId, filterString);
      });
    }
  );

  if (!filterString) {
    return {
      measures: measures.sort(sortFn),
      dimensions: dimensions.sort(sortFn),
      segments: segments.sort(sortFn),
      folders: folders.sort(sortFn),
      hierarchies: hierarchies.sort(sortFn),
      members: [...dimensions, ...measures, ...segments],
    };
  }

  // Filtered members
  const filteredMeasures = measures.length ? filterMemberFn(measures).sort(sortFn) : [];
  const filteredDimensions = dimensions.length ? filterMemberFn(dimensions).sort(sortFn) : [];
  const filteredSegments = segments.length ? filterMemberFn(segments).sort(sortFn) : [];
  const filteredFolders = folders.length ? filterMemberFn(folders).sort(sortFn) : [];
  const filteredHierarchies = (
    hierarchies.length ? filterMemberFn(hierarchies).sort(sortFn) : []
  ) as TCubeHierarchy[];

  const filteredMeasuresNames = filteredMeasures.map((measure) => measure.name);
  const filteredSegmentNames = filteredSegments.map((segment) => segment.name);
  const filteredFolderNames = filteredFolders.map((folder) => folder.name);
  const filteredDimensionNames = filteredDimensions.map((dimension) => dimension.name);
  const filteredHierarchyNames = filteredHierarchies.map((hierarchy) => hierarchy.name);

  // Include hierarchies with filtered members inside.
  // Hierarchy can only include dimensions.
  hierarchies.forEach((hierarchy) => {
    if (filteredHierarchyNames.includes(hierarchy.name)) {
      return;
    }

    if (hierarchy.levels.find((memberName) => filteredDimensionNames.includes(memberName))) {
      filteredHierarchies.push(hierarchy);
      filteredHierarchyNames.push(hierarchy.name);
    }
  });

  // Include folders with filtered members inside.
  // Folders can include dimensions, measures, segments and hierarchies.
  folders.forEach((folder) => {
    if (filteredFolderNames.includes(folder.name)) {
      return;
    }

    if (
      folder.members.find(
        (memberName) =>
          filteredDimensionNames.includes(memberName) ||
          filteredMeasuresNames.includes(memberName) ||
          filteredSegmentNames.includes(memberName) ||
          filteredHierarchyNames.includes(memberName)
      )
    ) {
      filteredFolders.push(folder);
    }
  });

  return {
    measures: filteredMeasures,
    dimensions: filteredDimensions,
    segments: filteredSegments,
    folders: filteredFolders,
    hierarchies: filteredHierarchies,
    members: [...filteredDimensions, ...filteredMeasures, ...filteredSegments],
  };
}
