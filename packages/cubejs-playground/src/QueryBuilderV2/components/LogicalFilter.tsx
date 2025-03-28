import { Flex, tasty } from '@cube-dev/ui-kit';
import {
  Filter,
  LogicalAndFilter,
  LogicalOrFilter,
  TCubeDimension,
  TCubeMeasure,
} from '@cubejs-client/core';
import { Key } from '@react-types/shared';

import { useEvent } from '../hooks';
import { useQueryBuilderContext } from '../context';

import { FilterMember } from './FilterMember';
import { AddFilterInput } from './AddFilterInput';
import { FilterOptionsButton } from './FilterOptionsButton';

interface LogicalFilterProps {
  type: 'and' | 'or';
  values: Filter[];
  isCompact?: boolean;
  isAddingCompact?: boolean;
  onRemove: () => void;
  onUnwrap: () => void;
  onChange: (filter: LogicalAndFilter | LogicalOrFilter) => void;
}

const LogicalOperatorContainer = tasty({
  qa: 'LogicalFilter',
  styles: {
    display: 'grid',
    gridColumns: 'min-content min-content 1fr',
    gap: '.5x',
    placeItems: 'center start',
    radius: true,
    fill: {
      '': '#clear',
      ':has(>[data-qa="FilterOptionsButton"][data-is-hovered])': '#light',
    },
    margin: '-.5x',
    padding: '.5x',
    width: 'max-content',
  },
});

const LogicalOperatorButton = tasty({
  styles: {
    display: 'grid',
    gridRows: '1fr auto 1fr',
    placeItems: 'stretch center',
    placeSelf: 'stretch',
    flow: 'column',
    preset: 'c2',
    color: '#dark-03',
    width: '3.5x',

    '&::before': {
      content: '""',
      width: '1ow',
      fill: '#dark.2',
      radius: 'top',
    },

    '&::after': {
      content: '""',
      width: '1ow',
      fill: '#dark.2',
      radius: 'bottom',
    },
  },
});

export function LogicalFilter(props: LogicalFilterProps) {
  const { isCompact, onChange, isAddingCompact, onRemove, onUnwrap, type, values } = props;
  const { members, memberViewType, cubes } = useQueryBuilderContext();

  function getMemberType(member: TCubeMeasure | TCubeDimension) {
    if (!member?.name) {
      return undefined;
    }

    if (members.measures[member.name]) {
      return 'measure';
    }
    if (members.dimensions[member.name]) {
      return 'dimension';
    }

    return undefined;
  }

  const filters = [...values];

  const changeValues = (filters: Filter[]) => {
    onChange({ [type]: filters } as LogicalAndFilter | LogicalOrFilter);
  };

  const removeFilter = (index: number) => {
    filters.splice(index, 1);

    changeValues(filters);
  };

  const updateFilter = (index: number, filter: Filter) => {
    filters[index] = filter;
    changeValues(filters);
  };

  const wrapFilter = useEvent((type: 'and' | 'or') => {
    onChange({ [type]: [{ [type]: values }] } as LogicalAndFilter | LogicalOrFilter);
  });

  const unwrapFilter = (index: number) => {
    const filter = filters[index];
    if ('and' in filter) {
      filters.splice(index, 1, ...filter.and);
    }
    if ('or' in filter) {
      filters.splice(index, 1, ...filter.or);
    }
    changeValues(filters);
  };

  const convert = () => {
    if (type === 'and') {
      onChange({ or: filters });
    } else {
      onChange({ and: filters });
    }
  };

  const onFilterAction = useEvent((key: Key) => {
    switch (key) {
      case 'remove':
        onRemove();
        break;
      case 'unwrap':
        onUnwrap();
        break;
      case 'convert':
        convert();
        break;
      case 'wrapWithAnd':
        wrapFilter('and');
        break;
      case 'wrapWithOr':
        wrapFilter('or');
        break;
    }
  });

  return (
    <LogicalOperatorContainer>
      <FilterOptionsButton
        type={type}
        disableKeys={!values.length ? ['unwrap'] : undefined}
        onAction={onFilterAction}
      />

      <LogicalOperatorButton>{type}</LogicalOperatorButton>

      <Flex flow="column" gap="1x">
        {filters.map((filter, index) => {
          if ('and' in filter) {
            return (
              <LogicalFilter
                key={index}
                type="and"
                values={filter.and}
                isCompact={isCompact}
                isAddingCompact={isAddingCompact}
                onRemove={() => {
                  removeFilter(index);
                }}
                onChange={(filter) => {
                  updateFilter(index, filter);
                }}
                onUnwrap={() => {
                  unwrapFilter(index);
                }}
              />
            );
          }

          if ('or' in filter) {
            return (
              <LogicalFilter
                key={index}
                type="or"
                values={filter.or}
                isCompact={isCompact}
                isAddingCompact={isAddingCompact}
                onRemove={() => {
                  removeFilter(index);
                }}
                onChange={(filter) => {
                  updateFilter(index, filter);
                }}
                onUnwrap={() => {
                  unwrapFilter(index);
                }}
              />
            );
          }

          if (!('member' in filter) || !filter.member) {
            return null;
          }

          const member = members.measures[filter.member] || members.dimensions[filter.member];
          const memberFullName = filter.member;
          const cubeName = memberFullName.split('.')[0];
          const cube = cubes.find((cube) => cube.name === cubeName);
          const memberName = memberFullName.split('.')[1];

          return (
            <FilterMember
              key={index}
              isMissing={!member}
              isCompact={isCompact}
              filter={filter}
              memberName={memberName}
              memberTitle={member?.shortTitle}
              cubeName={cubeName}
              cubeTitle={cube?.title}
              memberViewType={memberViewType}
              memberType={getMemberType(member)}
              type={member?.type}
              onRemove={() => {
                removeFilter(index);
              }}
              onChange={(updatedFilter) => {
                updateFilter(index, updatedFilter);
              }}
            />
          );
        })}

        <AddFilterInput
          isCompact={isAddingCompact}
          onAdd={(filter) => {
            changeValues([...filters, filter]);
          }}
        />
      </Flex>
    </LogicalOperatorContainer>
  );
}
