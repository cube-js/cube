import { TCubeMember } from '@cubejs-client/core';
import { Button, Typography } from 'antd';
import styled from 'styled-components';

const Box = styled.div`
  /* padding: 20px 24px; */
  /* background: #a5a3a3; */
`;

type MembersProps = {
  title: string;
  members: TCubeMember[];
};

export function Members({ title, members }: MembersProps) {
  return (
    <Box>
      <Typography.Paragraph>
        <Typography.Text>{title}</Typography.Text>
      </Typography.Paragraph>

      <div>
        {members.map((member) => (
          <Button>{member.title}</Button>
        ))}
      </div>
    </Box>
  );
}
