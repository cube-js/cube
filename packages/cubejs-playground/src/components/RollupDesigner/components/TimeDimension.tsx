import { TCubeMember, TimeDimensionGranularity } from '@cubejs-client/core';
import { Typography, Button } from 'antd';
import styled from 'styled-components';

const Flex = styled.div`
  display: flex;
  gap: 16px;
`;

type TimeDimensionProps = {
  member: TCubeMember;
  granularity?: TimeDimensionGranularity;
};

export function TimeDimension({ member, granularity }: TimeDimensionProps) {
  return (
    <>
      <Typography.Paragraph>
        <Typography.Text>Time dimension</Typography.Text>
      </Typography.Paragraph>

      <Flex>
        <Button>{member.title}</Button>
        {granularity ? <Button>{granularity}</Button> : null}
      </Flex>
    </>
  );
}
