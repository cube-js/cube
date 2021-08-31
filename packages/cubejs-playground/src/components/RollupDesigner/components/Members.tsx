import { TCubeMember } from '@cubejs-client/core';
import { Space, Typography } from 'antd';
import styled from 'styled-components';

import { MemberTag } from './MemberTag';

export const MemberType = styled(Typography.Paragraph)`
  font-size: 10px;
  text-transform: uppercase;
  color: rgba(20, 20, 70, 0.5);
`;

type MembersProps = {
  title: string;
  members: TCubeMember[];
  onRemove: (key: string) => void;
};

export function Members({ title, members, onRemove }: MembersProps) {
  return (
    <>
      <MemberType>{title}</MemberType>

      <Space wrap>
        {members.map((member) => (
          <MemberTag
            name={member.shortTitle}
            cubeName={member.title.replace(member.shortTitle, '')}
            onClose={() => onRemove(member.name)}
          />
        ))}
      </Space>
    </>
  );
}
