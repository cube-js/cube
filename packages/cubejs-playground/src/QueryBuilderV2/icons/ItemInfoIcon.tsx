import { memo } from 'react';
import { InfoCircleOutlined } from '@ant-design/icons';
import { TooltipProvider } from '@cube-dev/ui-kit';

export const ItemInfoIcon = memo(function InfoIcon({
  color = 'dark-02',
  title,
  description,
}: {
  color?: string;
  title?: string;
  description?: string;
}) {
  const icon = <InfoCircleOutlined style={{ color: `var(--${color}-color)` }} />;

  if (!description) {
    return icon;
  }

  return (
    <TooltipProvider
      activeWrap
      title={
        <>
          <b>{title?.trim() ?? ''}</b>
          <div>{description?.trim() ?? ''}</div>
        </>
      }
    >
      {icon}
    </TooltipProvider>
  );
});
