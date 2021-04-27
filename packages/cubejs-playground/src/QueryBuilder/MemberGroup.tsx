import { PlusOutlined } from '@ant-design/icons';

import MemberDropdown from './MemberDropdown';
import RemoveButtonGroup from './RemoveButtonGroup';
import { SectionRow } from '../components';
import MissingMemberTooltip from './MissingMemberTooltip';

const MemberGroup = ({
  disabled = false,
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
          disabled={disabled}
          className={disabled ? 'disabled' : null}
          color={isMissing ? 'danger' : 'primary'}
          onRemoveClick={() => updateMethods.remove(m)}
        >
          <MemberDropdown
            disabled={disabled}
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
      disabled={disabled}
      availableMembers={availableMembers}
      type="dashed"
      data-testid={addMemberName}
      icon={<PlusOutlined />}
      onClick={(m) => updateMethods.add(m)}
    >
      {!members.length ? addMemberName : null}
    </MemberDropdown>
  </SectionRow>
);

export default MemberGroup;
