import { memo, ReactNode } from 'react';
import { InfoCircleOutlined } from '@ant-design/icons';
import { Text, TooltipProvider } from '@cube-dev/ui-kit';

interface ItemInfoIconProps {
  color?: string;
  description?: ReactNode;
}

export const ItemInfoIcon = memo(function InfoIcon({
  color = 'dark-02',
  description,
}: ItemInfoIconProps) {
  const icon = <InfoCircleOutlined style={{ color: `var(--${color}-color)` }} />;

  if (typeof description === 'string') {
    description = description?.trim();
  }

  if (!description) {
    return icon;
  }

  return (
    <TooltipProvider activeWrap title={<Text preset="p3">{description}</Text>}>
      {icon}
    </TooltipProvider>
  );
});
