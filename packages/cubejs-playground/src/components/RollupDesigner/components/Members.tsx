import { TCubeMember } from '@cubejs-client/core';
import { Typography } from 'antd';

import { Flex } from '../../../grid';
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

      <Flex gap={2} wrap>
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
      </Flex>
    </>
  );
}
