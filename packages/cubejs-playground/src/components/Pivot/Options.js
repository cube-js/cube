import { Checkbox } from 'antd';

export default function Options({ pivotConfig, onUpdate }) {
  return (
    <Checkbox
      checked={pivotConfig.fillMissingDates}
      onChange={() =>
        onUpdate({
          fillMissingDates: !pivotConfig.fillMissingDates,
        })
      }
    >
      Fill Missing Dates
    </Checkbox>
  );
}
