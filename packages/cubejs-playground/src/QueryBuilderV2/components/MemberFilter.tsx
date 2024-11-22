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
  TCubeMemberType,
  UnaryFilter,
  UnaryOperator,
} from '@cubejs-client/core';

import { useDeepMemo } from '../hooks';
import { OPERATOR_LABELS, OPERATORS, OPERATORS_BY_TYPE, UNARY_OPERATORS } from '../values';

import { ValuesInput } from './ValuesInput';
import { TimeDateRangeSelector } from './TimeDateRangeSelector';
import { TimeDateSelector } from './TimeDateSelector';
import { DeleteFilterButton } from './DeleteFilterButton';
import { FilterLabel } from './FilterLabel';

interface OperatorSelectorProps {
  type: TCubeMemberType;
  value?: UnaryOperator | BinaryOperator;
  isDisabled?: boolean;
  onChange: (operator: UnaryOperator | BinaryOperator) => void;
}

const MemberFilterElement = tasty(Space, {
  styles: {
    gap: '1x',
    placeItems: 'start',

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
  },
});

function OperatorSelector(props: OperatorSelectorProps) {
  const { value, isDisabled, type, onChange } = props;

  return (
    <Select
      isDisabled={isDisabled}
      aria-label="Filter operator"
      size="small"
      width="14x max-content max-content"
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

interface MemberFilterProps {
  member: Filter;
  memberType?: 'dimension' | 'measure';
  type: TCubeMemberType;
  // Extra compact where all items in the filter are streched to fit within the container.
  isExtraCompact?: boolean;
  isCompact?: boolean;
  isMissing?: boolean;
  onChange: (filter: Filter) => void;
  onRemove: () => void;
}

export function MemberFilter(props: MemberFilterProps) {
  const {
    member,
    memberType,
    isCompact,
    isExtraCompact = false,
    isMissing,
    type,
    onRemove,
  } = props;

  const onOperatorChange = useCallback(
    (operator?: Key) => {
      const updatedFilter = {
        ...member,
        operator: operator,
        values: [],
      } as Filter;

      if (['set', 'notSet'].includes(operator as string)) {
        delete (updatedFilter as UnaryFilter | BinaryFilter).values;
      }

      if (['equals', 'notEquals'].includes(operator as string) && type === 'boolean') {
        (updatedFilter as UnaryFilter | BinaryFilter).values = ['true'];
      }

      props.onChange(updatedFilter);
    },
    [props.onChange]
  );

  const onValuesChange = useCallback(
    (values?: string[]) => {
      props.onChange({ ...member, values: values } as Filter);
    },
    [props.onChange]
  );

  const inputs = useDeepMemo(() => {
    if (
      !('member' in member) ||
      UNARY_OPERATORS.includes(member.operator) ||
      !OPERATORS.includes(member.operator)
    ) {
      return null;
    }

    switch (type) {
      case 'number':
      case 'string':
        return (
          <ValuesInput
            isCompact={isExtraCompact}
            type={type === 'number' ? 'number' : 'string'}
            values={member.values || []}
            onChange={onValuesChange}
          />
        );
      case 'boolean':
        return (
          <Switch
            margin=".5x top"
            isSelected={member.values?.[0] === 'true'}
            onChange={(value) => onValuesChange(value ? ['true'] : ['false'])}
          />
        );
      case 'time':
        if (member.operator.includes('Range')) {
          return (
            <TimeDateRangeSelector
              value={(member.values as [string, string]) || []}
              onChange={onValuesChange}
            />
          );
        } else if (member.operator.includes('Date')) {
          return (
            <TimeDateSelector
              value={member.values?.[0]}
              onChange={(val) => {
                onValuesChange([val]);
              }}
            />
          );
        } else {
          return (
            <ValuesInput type="string" values={member.values || []} onChange={onValuesChange} />
          );
        }
      default:
        return member.values?.map((value: string, i: number) => {
          return <ValueTag key={i}>{value}</ValueTag>;
        });
    }
  }, [member, type]);

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

  return (
    <ElementWrapper>
      <TooltipProvider title="Delete this filter">
        <DeleteFilterButton onPress={onRemove} />
      </TooltipProvider>

      <InnerContainer>
        {'and' in member || 'or' in member ? (
          <>
            <TooltipProvider
              activeWrap
              aria-label="UNSUPPORTED OPERATOR"
              title={JSON.stringify(member)}
            >
              <Badge type="disabled">UNSUPPORTED OPERATOR...</Badge>
            </TooltipProvider>
          </>
        ) : (
          <>
            <MemberContainer>
              {'member' in member && member.member ? (
                <FilterLabel
                  isCompact={isCompact}
                  isMissing={isMissing}
                  member={memberType}
                  type={type}
                  name={member.member}
                />
              ) : null}
              {
                <OperatorSelector
                  isDisabled={
                    !type || ('operator' in member && !OPERATORS.includes(member.operator))
                  }
                  type={type}
                  value={'operator' in member ? member.operator : undefined}
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
