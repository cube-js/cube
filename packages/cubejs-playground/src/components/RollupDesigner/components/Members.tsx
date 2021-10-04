import { BaseCubeMember } from '@cubejs-client/core';
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
  members: Array<BaseCubeMember | undefined>;
  onRemove: (key: string) => void;
};

export function Members({ title, members, onRemove }: MembersProps) {
  return (
    <>
      <MemberType>{title}</MemberType>

      <Space wrap>
        {members.map((member) => {
          if (!member) {
            console.warn(
              `Rollup Designer received 'undefined' member as ${title}`
            );
            return null;
          }

          return (
            <MemberTag
              name={member.shortTitle}
              cubeName={member.title.replace(member.shortTitle, '').trim()}
              onClose={() => onRemove(member.name)}
            />
          );
        })}
      </Space>
    </>
  );
}
