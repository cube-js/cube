import {
  GRANULARITIES,
  TCubeMember,
  TimeDimensionGranularity,
} from '@cubejs-client/core';
import { Typography, Menu, Space } from 'antd';
import styled from 'styled-components';
import ButtonDropdown from '../../../QueryBuilder/ButtonDropdown';
import MemberDropdown from '../../../QueryBuilder/MemberDropdown';
import RemoveButtonGroup from '../../../QueryBuilder/RemoveButtonGroup';
import { MemberType } from './Members';
import { MemberTag } from './MemberTag';

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
      <MemberType>Time dimension</MemberType>

      <Space>
        <MemberTag
          name={member.shortTitle}
          cubeName={member.title.replace(member.shortTitle, '')}
          onClose={() => onRemove(member.name)}
        />

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
      </Space>
    </>
  );
}
