import { tasty } from '@cube-dev/ui-kit';

const FilteredPartElement = tasty({
  styles: {
    display: 'inline',
    fill: '#purple.25',
  },
});

interface FilteredLabelProps {
  text: string;
  filter: string;
}

export function FilteredLabel({ text, filter }: FilteredLabelProps) {
  const lowerText = text.toLowerCase();

  filter = filter.toLowerCase();

  let index = lowerText.indexOf(filter);

  if (index === -1) {
    filter = filter.replace(/\s/g, '_');

    index = lowerText.indexOf(filter);

    if (index === -1) {
      filter = filter.replace(/_/g, ' ');

      index = lowerText.indexOf(filter);

      if (index === -1) {
        return <>{text}</>;
      }
    }
  }

  const startPart = text.slice(0, index);
  const filterPart = text.slice(index, index + filter.length);
  const endPart = text.slice(index + filter.length);

  return (
    <>
      {startPart}
      <FilteredPartElement>{filterPart}</FilteredPartElement>
      {endPart}
    </>
  );
}
