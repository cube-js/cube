import { PlusOutlined } from '@ant-design/icons';
import { Fragment } from 'react';

import { SectionRow, Select } from '../components';
import { useDeepMemo } from '../hooks';
import FilterInput from './FilterInput';
import MemberDropdown from './MemberDropdown';
import MissingMemberTooltip from './MissingMemberTooltip';
import RemoveButtonGroup from './RemoveButtonGroup';

type Props = {
  disabled: boolean;
  members: any[];
  availableMembers: any;
  addMemberName: any;
  updateMethods: any;
  missingMembers: any;
};

const FilterGroup = ({
  disabled = false,
  members,
  availableMembers,
  addMemberName,
  updateMethods,
  missingMembers,
}: Props) => {
  const operatorsByMemberName = useDeepMemo(() => {
    return members.reduce(
      (memo, item) => ({
        ...memo,
        [item.member]: [...(memo[item.member] || []), item.operator],
      }),
      {}
    );
  }, [members]);

  return (
    <SectionRow>
      {members.map((m) => {
        const isMissing = missingMembers.includes(m.member);

        const buttonGroup = (
          <RemoveButtonGroup
            disabled={disabled}
            className={disabled ? 'disabled' : null}
            color={isMissing ? 'danger' : 'primary'}
            onRemoveClick={() => updateMethods.remove(m)}
          >
            <MemberDropdown
              disabled={disabled}
              availableCubes={availableMembers}
              style={{
                minWidth: 150,
              }}
              onClick={(updateWith) =>
                updateMethods.update(m, { ...m, dimension: updateWith })
              }
            >
              {m.dimension.title}
            </MemberDropdown>
          </RemoveButtonGroup>
        );

        return (
          <Fragment key={`${m.member}-${m.operator}`}>
            {isMissing ? (
              <MissingMemberTooltip>{buttonGroup}</MissingMemberTooltip>
            ) : (
              buttonGroup
            )}

            <Select
              disabled={disabled}
              value={m.operator}
              style={{ width: 200 }}
              onChange={(operator) =>
                updateMethods.update(m, { ...m, operator })
              }
            >
              {m.operators.map((operator) => {
                const isOperatorDisabled = operatorsByMemberName[
                  m.member
                ]?.includes(operator.name);

                return (
                  <Select.Option
                    key={operator.name}
                    value={operator.name}
                    title={
                      isOperatorDisabled
                        ? `There is already a filter applied with this operator for ${
                            m.dimension?.title || m.name
                          }`
                        : operator.name
                    }
                    disabled={isOperatorDisabled}
                  >
                    {operator.title}
                  </Select.Option>
                );
              })}
            </Select>

            {!['set', 'notSet'].includes(m.operator) ? (
              <FilterInput
                key="filterInput"
                disabled={disabled}
                member={m}
                updateMethods={updateMethods}
              />
            ) : null}
          </Fragment>
        );
      })}

      <MemberDropdown
        availableCubes={availableMembers}
        type="dashed"
        disabled={disabled || members.find((m) => !m.operator)}
        icon={<PlusOutlined />}
        onClick={(m) => updateMethods.add({ member: m })}
      >
        {!members.length ? addMemberName : null}
      </MemberDropdown>
    </SectionRow>
  );
};

export default FilterGroup;
