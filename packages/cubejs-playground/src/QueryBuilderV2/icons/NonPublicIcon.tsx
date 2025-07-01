import { TooltipProvider } from '@cube-dev/ui-kit';
import { LockOutlined } from '@ant-design/icons';
import { memo } from 'react';

interface NonPublicIconProps {
  type?: 'cube' | 'view' | 'member';
}

const NonPublicIcon = memo(function NonPublicIcon(props: NonPublicIconProps) {
  return (
    <TooltipProvider
      activeWrap
      title={`This ${
        props.type ?? 'member'
      } is marked as not public and can only be queried in the Playground`}
      delay={1000}
    >
      <LockOutlined style={{ color: 'var(--dark-02-color)' }} />
    </TooltipProvider>
  );
});

export { NonPublicIcon };
