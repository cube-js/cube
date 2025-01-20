import React, { Fragment, Key, useCallback, useMemo } from 'react';
import {
  Badge,
  Item,
  Select,
  Space,
  Switch,
  Tag,
  tasty,
  Text,
  TooltipProvider,
} from '@cube-dev/ui-kit';
import {
  BinaryFilter,
  BinaryOperator,
  Filter,
  LogicalAndFilter,
  LogicalOrFilter,
  TCubeMemberType,
  UnaryFilter,
  UnaryOperator,
} from '@cubejs-client/core';

import { useDeepMemo, useEvent } from '../hooks';
import { OPERATOR_LABELS, OPERATORS, OPERATORS_BY_TYPE, UNARY_OPERATORS } from '../values';
import { MemberViewType } from '../types';

import { ValuesInput } from './ValuesInput';
import { TimeDateRangeSelector } from './TimeDateRangeSelector';
import { TimeDateSelector } from './TimeDateSelector';
import { FilterLabel } from './FilterLabel';
import { FilterOptionsAction, FilterOptionsButton } from './FilterOptionsButton';

interface OperatorSelectorProps {
  type: TCubeMemberType;
  value?: UnaryOperator | BinaryOperator;
  isDisabled?: boolean;
  onChange: (operator: UnaryOperator | BinaryOperator) => void;
}

const MemberFilterElement = tasty(Space, {
  qa: 'MemberFilter',
  styles: {
    gap: '1x',
    placeItems: 'start',
    radius: true,
    fill: {
      '': '#clear',
      ':has([data-qa="FilterOptionsButton"][data-is-hovered])': '#light',
    },
    margin: '-.5x',
    padding: '.5x',
    width: 'max-content',

    InnerContainer: {
      display: 'flex',
      flow: 'row',
      gap: '1x',
      placeItems: 'start',
    },

    MemberContainer: {
      display: 'flex',
      flow: 'row',
      flexShrink: 0,
      gap: '1x',
      placeItems: 'center start',
    },
  },
});

const ValueTag = tasty(Tag, {
  styles: {
    padding: '.625x .75x',
    preset: 't3',
    fill: '#light',
  },
});

function OperatorSelector(props: OperatorSelectorProps) {
  const { value, isDisabled, type, onChange } = props;

  return (
    <Select
      isDisabled={isDisabled}
      aria-label="Filter operator"
      size="small"
      listBoxStyles={{ height: 'auto' }}
      selectedKey={value}
      onSelectionChange={(operator: Key) => onChange(operator as UnaryOperator | BinaryOperator)}
    >
      {OPERATORS_BY_TYPE[type || 'all']?.map((operator) => {
        return (
          <Item key={operator} textValue={OPERATOR_LABELS[operator]}>
            <Text preset="t3m">{OPERATOR_LABELS[operator]}</Text>
          </Item>
        );
      })}
    </Select>
  );
}

interface FilterMemberProps {
  filter: BinaryFilter | UnaryFilter;
  cubeName?: string;
  cubeTitle?: string;
  memberName?: string;
  memberTitle?: string;
  memberType?: 'dimension' | 'measure';
  memberViewType?: MemberViewType;
  type: TCubeMemberType;
  // Extra compact where all items in the filter are stretched to fit within the container.
  isExtraCompact?: boolean;
  isCompact?: boolean;
  isMissing?: boolean;
  onChange: (filter: Filter) => void;
  onRemove: () => void;
}

export function FilterMember(props: FilterMemberProps) {
  const {
    filter,
    memberType,
    isCompact,
    isExtraCompact = false,
    isMissing,
    type,
    cubeName,
    cubeTitle,
    memberName,
    memberTitle,
    memberViewType,
    onRemove,
    onChange,
  } = props;

  const onOperatorChange = useEvent((operator?: Key) => {
    const updatedFilter = {
      values: [],
      ...filter,
      operator: operator,
    } as BinaryFilter | UnaryFilter;

    if (type === 'time') {
      updatedFilter.values = [];
    }

    if (['set', 'notSet'].includes(operator as string)) {
      delete updatedFilter.values;
    }

    if (['equals', 'notEquals'].includes(operator as string) && type === 'boolean') {
      updatedFilter.values = ['true'];
    }

    onChange(updatedFilter);
  });

  const onValuesChange = useEvent((values?: string[]) => {
    onChange({ ...filter, values: values } as Filter);
  });

  const wrapFilter = useEvent((type: 'and' | 'or') => {
    onChange({ [type]: [filter] } as LogicalAndFilter | LogicalOrFilter);
  });

  const inputs = useDeepMemo(() => {
    const operator = filter.operator;

    if (
      !('member' in filter) ||
      UNARY_OPERATORS.includes(filter.operator) ||
      !OPERATORS.includes(filter.operator)
    ) {
      return null;
    }

    const allowSuggestions =
      type === 'string' && (operator === 'equals' || operator === 'notEquals');

    switch (type) {
      case 'number':
      case 'string':
        return (
          <ValuesInput
            key={operator}
            memberName={'member' in filter ? filter.member : undefined}
            memberType={memberType}
            allowSuggestions={allowSuggestions}
            isCompact={isExtraCompact}
            type={type === 'number' ? 'number' : 'string'}
            values={filter.values || []}
            onChange={onValuesChange}
          />
        );
      case 'boolean':
        return (
          <Switch
            margin=".5x top"
            isSelected={filter.values?.[0] === 'true'}
            onChange={(value) => onValuesChange(value ? ['true'] : ['false'])}
          />
        );
      case 'time':
        if (filter.operator.includes('Range')) {
          return (
            <TimeDateRangeSelector
              key={filter.operator}
              value={(filter.values as [string, string]) || []}
              onChange={onValuesChange}
            />
          );
        } else if (filter.operator.includes('Date')) {
          return (
            <TimeDateSelector
              key={filter.operator}
              value={filter.values?.[0]}
              onChange={(val) => {
                onValuesChange([val]);
              }}
            />
          );
        } else {
          return (
            <ValuesInput
              key={filter.operator}
              memberName={'member' in filter ? filter.member : undefined}
              memberType={memberType}
              allowSuggestions={allowSuggestions}
              placeholder="Date/time in ISO 8601"
              type="string"
              values={filter.values || []}
              onChange={onValuesChange}
            />
          );
        }
      default:
        return filter.values?.map((value: string, i: number) => {
          return <ValueTag key={i}>{value}</ValueTag>;
        });
    }
  }, [filter, type]);

  const ElementWrapper = useMemo(
    () => (isExtraCompact ? Fragment : MemberFilterElement),
    [isExtraCompact]
  );

  const InnerContainer = useCallback(
    ({ children }: React.PropsWithChildren<{}>) => {
      if (isExtraCompact) {
        return <>{children}</>;
      }

      return <div data-element="InnerContainer">{children}</div>;
    },
    [isExtraCompact]
  );

  const MemberContainer = useCallback(
    ({ children }: React.PropsWithChildren<{}>) => {
      if (isExtraCompact) {
        return <>{children}</>;
      }

      return <div data-element="MemberContainer">{children}</div>;
    },
    [isExtraCompact]
  );

  const onAction = useEvent((key: FilterOptionsAction) => {
    switch (key) {
      case 'remove':
        onRemove();
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
    <ElementWrapper>
      <FilterOptionsButton type="member" onAction={onAction} />

      <InnerContainer>
        {'and' in filter || 'or' in filter ? (
          <>
            <TooltipProvider
              activeWrap
              aria-label="UNSUPPORTED OPERATOR"
              title={JSON.stringify(filter)}
            >
              <Badge type="disabled">UNSUPPORTED OPERATOR...</Badge>
            </TooltipProvider>
          </>
        ) : (
          <>
            <MemberContainer>
              {'member' in filter && filter.member ? (
                <FilterLabel
                  isCompact={isCompact}
                  isMissing={isMissing}
                  member={memberType}
                  memberName={memberName}
                  memberTitle={memberTitle}
                  cubeName={cubeName}
                  cubeTitle={cubeTitle}
                  memberViewType={memberViewType}
                  type={type}
                  name={filter.member}
                />
              ) : null}
              {
                <OperatorSelector
                  isDisabled={
                    !type || ('operator' in filter && !OPERATORS.includes(filter.operator))
                  }
                  type={type}
                  value={'operator' in filter ? filter.operator : undefined}
                  onChange={onOperatorChange}
                />
              }
            </MemberContainer>

            {isExtraCompact ? inputs : <div>{inputs}</div>}
          </>
        )}
      </InnerContainer>
    </ElementWrapper>
  );
}
