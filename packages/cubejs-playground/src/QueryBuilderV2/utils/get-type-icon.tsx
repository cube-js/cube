import { BooleanIcon, FilterIcon, NumberIcon, StringIcon, TimeIcon } from '@cube-dev/ui-kit';
import { TCubeMemberType } from '@cubejs-client/core';
import { QuestionCircleOutlined } from '@ant-design/icons';

const ICON_MAP = {
  number: <NumberIcon />,
  string: <StringIcon />,
  time: <TimeIcon />,
  boolean: <BooleanIcon />,
  filter: <FilterIcon />,
};

export function getTypeIcon(type: TCubeMemberType | 'filter') {
  return ICON_MAP[type] || <QuestionCircleOutlined style={{ fontSize: 'var(--icon-size)' }} />;
}
