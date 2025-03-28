import { Button, Text, tasty, FolderOpenFilledIcon, FolderFilledIcon } from '@cube-dev/ui-kit';
import { ReactElement } from 'react';

import { FilteredLabel } from './FilteredLabel';

export interface FolderProps {
  name: string;
  isOpen?: boolean;
  onToggle: (isOpen: boolean, name: string) => void;
  filterString?: string;
  children?: ReactElement[];
}

const OpenButton = tasty(Button, {
  size: 'small',
  type: 'neutral',
  styles: {
    gridColumns: 'auto auto 1fr',
    placeContent: 'center start',
    placeItems: 'center start',
    color: '#dark',
  },
});

const FolderElement = tasty({
  styles: {
    display: 'flex',
    flow: 'column',
    gap: '1bw',

    Contents: {
      display: 'flex',
      hide: {
        '': false,
        empty: true,
      },
      position: 'relative',
      margin: '4x left',
      flow: 'column',
      gap: '1bw',
      padding: '0 0 .5x 0',
    },

    FolderLine: {
      position: 'absolute',
      inset: '0 auto .5x (1bw - 2x)',
      fill: '#border-opaque',
      width: '.25x',
      radius: true,
    },

    Extra: {
      display: 'grid',
    },
  },
});

export function Folder(props: FolderProps) {
  const { name, isOpen, onToggle, filterString, children } = props;

  return (
    <FolderElement mods={{ open: isOpen, empty: !children || !children?.length }}>
      <OpenButton
        icon={
          isOpen ? (
            <FolderOpenFilledIcon style={{ color: 'var(--dark-03-color)' }} />
          ) : (
            <FolderFilledIcon style={{ color: 'var(--dark-03-color)' }} />
          )
        }
        onPress={() => onToggle(!isOpen, name)}
      >
        <Text ellipsis color="#dark-02">
          {filterString ? <FilteredLabel text={name} filter={filterString} /> : name}
        </Text>
      </OpenButton>
      <div data-element="Contents">
        {children}
        <div data-element="FolderLine" />
      </div>
    </FolderElement>
  );
}
