import { Space, tasty, Text, TooltipProvider, ViewIcon, CubeIcon } from '@cube-dev/ui-kit';
import { PlusOutlined, QuestionCircleOutlined } from '@ant-design/icons';

import { CubeStats } from '../types';
import { ChevronIcon } from '../icons/ChevronIcon';
import { NonPublicIcon } from '../icons/NonPublicIcon';

import { ListButton } from './ListButton';
import { MemberBadge } from './Badge';

const CubeListButton = tasty(ListButton, {
  styles: {
    radius: 0,
    color: {
      '': '#dark',
      missing: '#danger-text',
    },
    flow: 'column',
    placeContent: 'space-between',
  },
});

interface CubeListItemProps {
  name: string;
  title?: string;
  description?: string;
  isMissing?: boolean;
  isPrivate?: boolean;
  isDisabled?: boolean;
  isSelected?: boolean;
  rightIcon?: 'arrow' | 'plus' | null;
  type?: 'cube' | 'view';
  stats?: Partial<CubeStats>;
  onItemSelect?: () => void;
}

export function ListCube({
  name,
  title,
  description,
  type = 'cube',
  stats,
  rightIcon = 'arrow',
  isMissing,
  isDisabled,
  isPrivate,
  isSelected,
  onItemSelect,
}: CubeListItemProps) {
  return (
    <TooltipProvider
      title={
        <>
          <b>{title || name}</b>
          {description ? <> – {description}</> : undefined}
        </>
      }
      width="max-content"
      placement="right"
    >
      <CubeListButton
        qa={`Playground-${name}`}
        icon={
          isMissing ? (
            <QuestionCircleOutlined style={{ color: 'var(--danger-text-color)' }} />
          ) : type === 'cube' ? (
            <CubeIcon color="#purple" />
          ) : (
            <ViewIcon color="#purple" />
          )
        }
        type={isSelected ? 'outline' : 'clear'}
        isDisabled={isDisabled}
        mods={{ selected: isSelected, missing: isMissing }}
        rightIcon={
          !isMissing ? (
            rightIcon === 'arrow' ? (
              <ChevronIcon
                direction={!isSelected ? 'right' : 'top'}
                style={{ color: 'var(--purple-color)' }}
              />
            ) : rightIcon === 'plus' ? (
              <PlusOutlined style={{ color: 'var(--purple-color)' }} />
            ) : undefined
          ) : undefined
        }
        onPress={() => !isMissing && onItemSelect?.()}
      >
        <Text ellipsis>{name}</Text>
        {stats && (
          <Space gap=".5x">
            {stats.measures?.length ? (
              <MemberBadge type="measure">{stats.measures.length}</MemberBadge>
            ) : undefined}
            {stats.dimensions?.length ? (
              <MemberBadge type="dimension">{stats.dimensions.length}</MemberBadge>
            ) : undefined}
            {stats?.timeDimensions?.length ? (
              <MemberBadge type="timeDimension">{stats.timeDimensions.length}</MemberBadge>
            ) : undefined}
            {stats.segments?.length ? (
              <MemberBadge type="segment">{stats.segments.length}</MemberBadge>
            ) : undefined}
            {stats.filters?.length ? (
              <MemberBadge type="filter">{stats.filters.length}</MemberBadge>
            ) : undefined}
          </Space>
        )}
        {isPrivate ? <NonPublicIcon type="cube" /> : undefined}
      </CubeListButton>
    </TooltipProvider>
  );
}
