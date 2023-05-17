import { Tooltip } from 'antd';

export default function MissingMemberTooltip({ children }) {
  return (
    <Tooltip
      overlayClassName="missing-member-tooltip"
      placement="top"
      title="This member was removed from the data model"
      color="var(--dark-01-color)"
    >
      {children}
    </Tooltip>
  );
}
