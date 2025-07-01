import { PrismCode, tasty } from '@cube-dev/ui-kit';

const ContainerElement = tasty({
  styles: {
    display: 'grid',
    placeContent: 'stretch',
    position: 'absolute',
    overflow: 'auto',
    fill: '#light',
    styledScrollbar: true,
    top: 0,
    right: 0,
    left: 0,
    bottom: 0,
  },
});

interface ScrollableCodeContainerProps {
  value: string;
}

export function ScrollableCodeContainer({ value }: ScrollableCodeContainerProps) {
  return (
    <ContainerElement>
      <PrismCode radius={0} padding="1x" code={value} />
    </ContainerElement>
  );
}
