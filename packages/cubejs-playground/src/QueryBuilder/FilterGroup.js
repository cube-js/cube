import { Fragment } from 'react';
import * as PropTypes from 'prop-types';
import { PlusOutlined } from '@ant-design/icons';

import MemberDropdown from './MemberDropdown';
import RemoveButtonGroup from './RemoveButtonGroup';
import FilterInput from './FilterInput';
import MissingMemberTooltip from './MissingMemberTooltip';
import { SectionRow, Select } from '../components';

const FilterGroup = ({
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
          color={isMissing ? 'danger' : 'primary'}
          onRemoveClick={() => updateMethods.remove(m)}
        >
          <MemberDropdown
            onClick={(updateWith) =>
              updateMethods.update(m, { ...m, dimension: updateWith })
            }
            availableMembers={availableMembers}
            style={{
              width: 150,
              textOverflow: 'ellipsis',
              overflow: 'hidden',
            }}
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
            value={m.operator}
            onChange={(operator) => updateMethods.update(m, { ...m, operator })}
            style={{ width: 200 }}
          >
            {m.operators.map((operator) => (
              <Select.Option key={operator.name} value={operator.name}>
                {operator.title}
              </Select.Option>
            ))}
          </Select>
          
          <FilterInput
            member={m}
            key="filterInput"
            updateMethods={updateMethods}
          />
        </Fragment>
      );
    })}
    <MemberDropdown
      onClick={(m) => updateMethods.add({ dimension: m })}
      availableMembers={availableMembers}
      type="dashed"
      icon={<PlusOutlined />}
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
