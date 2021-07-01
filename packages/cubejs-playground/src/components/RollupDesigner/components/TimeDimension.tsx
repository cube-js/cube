import {
  GRANULARITIES,
  TCubeMember,
  TimeDimensionGranularity,
} from '@cubejs-client/core';
import { Typography, Button, Menu } from 'antd';
import styled from 'styled-components';
import ButtonDropdown from '../../../QueryBuilder/ButtonDropdown';
import MemberDropdown from '../../../QueryBuilder/MemberDropdown';
import RemoveButtonGroup from '../../../QueryBuilder/RemoveButtonGroup';

const Flex = styled.div`
  display: flex;
  gap: 16px;
`;

type TimeDimensionProps = {
  member: TCubeMember;
  granularity?: TimeDimensionGranularity;
  onGranularityChange: (
    granularity: TimeDimensionGranularity | undefined
  ) => void;
  onRemove: (key: string) => void;
};

export function TimeDimension({
  member,
  granularity,
  onGranularityChange,
  onRemove,
}: TimeDimensionProps) {
  return (
    <>
      <Typography.Paragraph>
        <Typography.Text>Time dimension</Typography.Text>
      </Typography.Paragraph>

      <Flex>
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

        {granularity ? (
          <ButtonDropdown
            overlay={
              <Menu>
                {GRANULARITIES.map(({ name, title }) => (
                  <Menu.Item
                    key={title}
                    onClick={() => onGranularityChange(name)}
                  >
                    {title}
                  </Menu.Item>
                ))}
              </Menu>
            }
          >
            {granularity}
          </ButtonDropdown>
        ) : null}
      </Flex>
    </>
  );
}
