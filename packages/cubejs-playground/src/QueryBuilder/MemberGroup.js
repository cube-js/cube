import * as PropTypes from 'prop-types';
import { PlusOutlined } from '@ant-design/icons';

import MemberDropdown from './MemberDropdown';
import RemoveButtonGroup from './RemoveButtonGroup';
import { SectionRow } from '../components';
import MissingMemberTooltip from './MissingMemberTooltip';

const MemberGroup = ({
  members,
  availableMembers,
  missingMembers,
  addMemberName,
  updateMethods,
}) => (
  <SectionRow>
    {members.map((m) => {
      const isMissing = missingMembers.includes(m.title);

      const buttonGroup = (
        <RemoveButtonGroup
          key={m.index || m.name}
          color={isMissing ? 'danger' : 'primary'}
          onRemoveClick={() => updateMethods.remove(m)}
        >
          <MemberDropdown
            availableMembers={availableMembers}
            onClick={(updateWith) => updateMethods.update(m, updateWith)}
          >
            {m.title}
          </MemberDropdown>
        </RemoveButtonGroup>
      );

      return isMissing ? (
        <MissingMemberTooltip>
          {buttonGroup}
        </MissingMemberTooltip>
      ) : (
        buttonGroup
      );
    })}
    <MemberDropdown
      onClick={(m) => updateMethods.add(m)}
      availableMembers={availableMembers}
      type="dashed"
      icon={<PlusOutlined />}
    >
      {!members.length ? addMemberName : null}
    </MemberDropdown>
  </SectionRow>
);

MemberGroup.propTypes = {
  members: PropTypes.array.isRequired,
  missingMembers: PropTypes.array.isRequired,
  availableMembers: PropTypes.array.isRequired,
  addMemberName: PropTypes.string.isRequired,
  updateMethods: PropTypes.object.isRequired,
};

export default MemberGroup;
