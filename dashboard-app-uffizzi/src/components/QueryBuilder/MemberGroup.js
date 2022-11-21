import React from 'react';
import * as PropTypes from 'prop-types';
import { Icon } from '@ant-design/compatible';
import MemberDropdown from './MemberDropdown';
import RemoveButtonGroup from './RemoveButtonGroup';

const MemberGroup = ({
  members,
  availableMembers,
  addMemberName,
  updateMethods,
}) => (
  <span>
    {members.map((m) => (
      <RemoveButtonGroup
        key={m.index || m.name}
        onRemoveClick={() => updateMethods.remove(m)}
      >
        <MemberDropdown
          availableMembers={availableMembers}
          onClick={(updateWith) => updateMethods.update(m, updateWith)}
        >
          {m.title}
        </MemberDropdown>
      </RemoveButtonGroup>
    ))}
    <MemberDropdown
      onClick={(m) => updateMethods.add(m)}
      availableMembers={availableMembers}
      type="dashed"
      icon={<Icon type="plus" />}
    >
      {addMemberName}
    </MemberDropdown>
  </span>
);

MemberGroup.propTypes = {
  members: PropTypes.array.isRequired,
  availableMembers: PropTypes.array.isRequired,
  addMemberName: PropTypes.string.isRequired,
  updateMethods: PropTypes.object.isRequired,
};
export default MemberGroup;
