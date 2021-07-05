import { TCubeMember } from '@cubejs-client/core';
import { Typography } from 'antd';
import styled from 'styled-components';

import MemberDropdown from '../../../QueryBuilder/MemberDropdown';
import RemoveButtonGroup from '../../../QueryBuilder/RemoveButtonGroup';

const Flex = styled.div`
  display: flex;
  flex-direction: column;
  gap: 16px;
`;

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

      <Flex>
        {members.map((member) => (
          <div key={member.name}>
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
          </div>
        ))}
      </Flex>
    </>
  );
}
