import { Button, Menu, MenuTrigger, MoreIcon, tasty } from '@cube-dev/ui-kit';
import { Key } from '@react-types/shared';
import { useMemo } from 'react';

const OptionsButton = tasty(Button, {
  qa: 'FilterOptionsButton',
  'aria-label': 'Filter options',
  size: 'small',
  type: 'secondary',
  icon: <MoreIcon />,
  styles: {
    width: '3x',
  },
});

export type FilterOptionsAction = 'convert' | 'remove' | 'unwrap' | 'wrapWithOr' | 'wrapWithAnd';
export type FilterOptionsType = 'and' | 'or' | 'member' | 'segment' | 'dateRange';

export interface FilterOptionsButtonProps {
  type: FilterOptionsType;
  onAction: (action: FilterOptionsAction) => void;
  disableKeys?: FilterOptionsAction[];
}

export function FilterOptionsButton({ type, disableKeys, onAction }: FilterOptionsButtonProps) {
  const items = useMemo(() => {
    const menuItems: { key: string; label: string; color?: string }[] = [];

    if (type === 'or' || type === 'and') {
      if (type === 'and') {
        menuItems.push({
          key: 'convert',
          label: 'Convert to OR Branch',
        });
      }

      if (type === 'or') {
        menuItems.push({
          key: 'convert',
          label: 'Convert to AND Branch',
        });
      }

      menuItems.push({
        key: 'unwrap',
        label: 'Unwrap Branch',
      });
    }

    if (type === 'member' || type === 'or' || type === 'and') {
      menuItems.push({
        key: 'wrapWithOr',
        label: 'Wrap with OR Branch',
      });

      menuItems.push({
        key: 'wrapWithAnd',
        label: 'Wrap with AND Branch',
      });
    }

    menuItems.push({
      key: 'remove',
      label: 'Remove',
      color: '#danger',
    });

    return menuItems.filter((item) => !disableKeys?.includes(item.key as FilterOptionsAction));
  }, [type, disableKeys]);

  return (
    <MenuTrigger>
      <OptionsButton />
      <Menu onAction={(key: Key) => onAction(key as FilterOptionsAction)}>
        {items.map((item) => (
          <Menu.Item key={item.key} color={item.color}>
            {item.label}
          </Menu.Item>
        ))}
      </Menu>
    </MenuTrigger>
  );
}
