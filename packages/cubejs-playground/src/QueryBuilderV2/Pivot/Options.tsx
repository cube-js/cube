import { Checkbox } from '@cube-dev/ui-kit';

import { QueryBuilderContextProps } from '../types';

export function PivotOptions({
  pivotConfig,
  onUpdate,
}: {
  pivotConfig: QueryBuilderContextProps['pivotConfig'];
  onUpdate: QueryBuilderContextProps['updatePivotConfig']['update'];
}) {
  return pivotConfig ? (
    <Checkbox
      isSelected={pivotConfig.fillMissingDates as boolean}
      onChange={() =>
        onUpdate({
          fillMissingDates: !pivotConfig.fillMissingDates,
        })
      }
    >
      Fill Missing Dates
    </Checkbox>
  ) : null;
}
