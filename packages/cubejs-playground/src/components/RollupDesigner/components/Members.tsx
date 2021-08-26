import { TCubeMember } from '@cubejs-client/core';
import { Space, Typography } from 'antd';

import MemberDropdown from '../../../QueryBuilder/MemberDropdown';
import RemoveButtonGroup from '../../../QueryBuilder/RemoveButtonGroup';

type MembersProps = {
  title: string;
  members: TCubeMember[];
  onRemove: (key: string) => void;
};

export function Members({ title, members, onRemove }: MembersProps) {
  return (
    <>
      <Typography.Paragraph>
        <Typography.Text>{title}</Typography.Text>
      </Typography.Paragraph>

      <Space wrap>
        {members.map((member) => (
          <RemoveButtonGroup
            key={member.name}
            onRemoveClick={() => onRemove(member.name)}
          >
            <MemberDropdown
              showNoMembersPlaceholder={false}
              availableMembers={[]}
              onClick={() => undefined}
            >
              {member.title}
            </MemberDropdown>
          </RemoveButtonGroup>
        ))}
      </Space>
    </>
  );
}
