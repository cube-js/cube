import { Space, TooltipProvider } from '@cube-dev/ui-kit';
import { TCubeSegment } from '@cubejs-client/core';

import { DeleteFilterButton } from './DeleteFilterButton';
import { FilterLabel } from './FilterLabel';

interface MemberFilterProps {
  name: string;
  member: TCubeSegment;
  isCompact?: boolean;
  onRemove: () => void;
}

export function SegmentFilter(props: MemberFilterProps) {
  const { member, name, isCompact, onRemove } = props;

  return (
    <Space gap="1x" placeItems="center">
      <TooltipProvider title="Delete this filter">
        <DeleteFilterButton onPress={onRemove} />
      </TooltipProvider>
      <FilterLabel
        isCompact={isCompact}
        isMissing={!member}
        type="filter"
        member="segment"
        name={name}
      />
    </Space>
  );
}
