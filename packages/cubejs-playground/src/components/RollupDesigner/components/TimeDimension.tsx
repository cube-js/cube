import { TCubeMember, TimeDimensionGranularity } from '@cubejs-client/core';
import { Space } from 'antd';

import { MemberType } from './Members';
import { MemberTag } from './MemberTag';
import { GranularitySelect } from './Settings';

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
  granularity = 'day',
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

        <GranularitySelect value={granularity} onChange={onGranularityChange} />
      </Space>
    </>
  );
}
