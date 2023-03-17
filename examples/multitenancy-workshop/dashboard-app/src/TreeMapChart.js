import { ResponsiveTreeMap } from '@nivo/treemap';

export const TreeMapChart = ({ data, name, value }) => {
  const transformedData = {
    id: '',
    children: data.map(row => ({
      id: name(row),
      value: value(row)
    }))
  };

  return (
    <ResponsiveTreeMap
      data={transformedData}
      margin={{ top: 10, right: 10, bottom: 10, left: 10 }}
      leavesOnly={true}
      label={row => `${row.id} — ${row.formattedValue}`}
      isInteractive={false}
    />
  );
}