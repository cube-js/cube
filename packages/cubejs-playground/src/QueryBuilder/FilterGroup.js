import { Fragment } from 'react';
import * as PropTypes from 'prop-types';
import { PlusOutlined } from '@ant-design/icons';

import MemberDropdown from './MemberDropdown';
import RemoveButtonGroup from './RemoveButtonGroup';
import FilterInput from './FilterInput';
import MissingMemberTooltip from './MissingMemberTooltip';
import { SectionRow, Select } from '../components';

const FilterGroup = ({
  disabled = false,
  members,
  availableMembers,
  addMemberName,
  updateMethods,
  missingMembers,
}) => (
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
            availableMembers={availableMembers}
            style={{
              width: 150,
              textOverflow: 'ellipsis',
              overflow: 'hidden',
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
        <Fragment key={m.index}>
          {isMissing ? (
            <MissingMemberTooltip>{buttonGroup}</MissingMemberTooltip>
          ) : (
            buttonGroup
          )}

          <Select
            disabled={disabled}
            value={m.operator}
            style={{ width: 200 }}
            onChange={(operator) => updateMethods.update(m, { ...m, operator })}
          >
            {m.operators.map((operator) => (
              <Select.Option key={operator.name} value={operator.name}>
                {operator.title}
              </Select.Option>
            ))}
          </Select>
          
          <FilterInput
            key="filterInput"
            disabled={disabled}
            member={m}
            updateMethods={updateMethods}
          />
        </Fragment>
      );
    })}
    <MemberDropdown
      availableMembers={availableMembers}
      type="dashed"
      disabled={disabled}
      icon={<PlusOutlined />}
      onClick={(m) => updateMethods.add({ member: m })}
    >
      {!members.length ? addMemberName : null}
    </MemberDropdown>
  </SectionRow>
);

FilterGroup.propTypes = {
  members: PropTypes.array.isRequired,
  availableMembers: PropTypes.array.isRequired,
  addMemberName: PropTypes.string.isRequired,
  updateMethods: PropTypes.object.isRequired,
};

export default FilterGroup;
