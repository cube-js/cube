import { PlusOutlined } from '@ant-design/icons';
import { AvailableCube } from '@cubejs-client/react';
import { useCallback } from 'react';

import MemberDropdown from './MemberDropdown';
import RemoveButtonGroup from './RemoveButtonGroup';
import { SectionRow } from '../components';
import MissingMemberTooltip from './MissingMemberTooltip';

type MemberGroupProps = {
  disalbed?: boolean;
  availableMembers: AvailableCube[];
  [key: string]: any;
};

export default function MemberGroup({
  disabled = false,
  members,
  availableMembers,
  missingMembers,
  addMemberName,
  updateMethods,
}: MemberGroupProps) {
  const handleClick = useCallback((m) => updateMethods.add(m), []);

  return (
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
              availableCubes={availableMembers}
              onClick={(updateWith) => updateMethods.update(m, updateWith)}
            >
              {m.title}
            </MemberDropdown>
          </RemoveButtonGroup>
        );

        return isMissing ? (
          <MissingMemberTooltip key={m.index || m.name}>
            {buttonGroup}
          </MissingMemberTooltip>
        ) : (
          buttonGroup
        );
      })}

      <MemberDropdown
        data-testid={addMemberName}
        disabled={disabled}
        availableCubes={availableMembers}
        type="dashed"
        icon={<PlusOutlined />}
        onClick={handleClick}
      >
        {!members.length ? addMemberName : null}
      </MemberDropdown>
    </SectionRow>
  );
}
