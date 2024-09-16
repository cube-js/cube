import { TCubeDimension, TCubeMeasure, TCubeSegment } from '@cubejs-client/core';

import { useEvent } from './event';
import { useRawFilter } from './raw-filter';

export function useFilteredMembers(
  filterString: string,
  members: { dimensions: TCubeDimension[]; measures: TCubeMeasure[]; segments: TCubeSegment[] }
) {
  // Search filters logic
  const rawFilterFn = useRawFilter();

  const { dimensions, measures, segments } = members;

  const filterMemberFn = useEvent(
    <T extends TCubeMeasure | TCubeDimension | TCubeSegment>(items: T[]): T[] => {
      return items.filter((item: T) => {
        return rawFilterFn(item.name.split('.')[1], filterString);
      });
    }
  );

  if (!filterString) {
    return {
      measures: measures.sort((a, b) => a.name.localeCompare(b.name)),
      dimensions: dimensions.sort((a, b) => a.name.localeCompare(b.name)),
      segments: segments.sort((a, b) => a.name.localeCompare(b.name)),
    };
  }

  // Filtered measures
  const shownMeasures = measures.length
    ? filterMemberFn(measures).sort((a, b) => a.name.localeCompare(b.name))
    : [];
  // Filtered dimensions
  const shownDimensions = dimensions.length
    ? filterMemberFn(dimensions).sort((a, b) => a.name.localeCompare(b.name))
    : [];
  const shownSegments = segments.length
    ? filterMemberFn(segments).sort((a, b) => a.name.localeCompare(b.name))
    : [];

  return {
    measures: shownMeasures,
    dimensions: shownDimensions,
    segments: shownSegments,
  };
}
