import * as PropTypes from 'prop-types';
import { PlusOutlined } from '@ant-design/icons';
import MemberDropdown from './MemberDropdown';
import RemoveButtonGroup from './RemoveButtonGroup';
import { SectionRow } from '../components';

const MemberGroup = ({
  members,
  availableMembers,
  addMemberName,
  updateMethods,
}) => (
  <SectionRow>
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
      icon={<PlusOutlined />}
    >
      {!members.length ? addMemberName : null}
    </MemberDropdown>
  </SectionRow>
);

MemberGroup.propTypes = {
  members: PropTypes.array.isRequired,
  availableMembers: PropTypes.array.isRequired,
  addMemberName: PropTypes.string.isRequired,
  updateMethods: PropTypes.object.isRequired,
};

export default MemberGroup;
