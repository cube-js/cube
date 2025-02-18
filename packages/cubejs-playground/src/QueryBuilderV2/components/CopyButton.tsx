import {
  Button,
  useToastsApi,
  copy,
  tasty,
  CubeButtonProps,
  CopyIcon as CopyIconUIKit,
} from '@cube-dev/ui-kit';
import { useState } from 'react';
import { unstable_batchedUpdates } from 'react-dom';

import { useDebouncedState, useEvent } from '../hooks';

import { CopyIcon } from './CopyIcon';

export type CopyButtonProps = {
  value: string;
  onCopy?: () => void;
  toastMessage?: string;
  dontShowToast?: boolean;
} & Omit<CubeButtonProps, 'onPress'>;

const CopyButtonElement = tasty(Button, {
  label: 'Copy value to clipboard',
  type: 'clear',
  size: 'small',
  icon: <CopyIconUIKit />,
});

export function CopyButton(props: CopyButtonProps) {
  const {
    value,
    onCopy,
    toastMessage = 'Copied to clipboard',
    dontShowToast = false,
    ...buttonProps
  } = props;
  const { toast } = useToastsApi();
  const [coping, setCoping] = useDebouncedState(false, 300);
  const [copied, setCopied] = useState(false);

  const onCopyAnimationEnd = useEvent(() => setCopied(false));
  const icon = props.icon ?? <CopyIcon isCopied={copied} onCopyAnimationEnd={onCopyAnimationEnd} />;

  return (
    <CopyButtonElement
      {...buttonProps}
      isLoading={coping}
      icon={icon}
      onPress={async () => {
        setCoping(true);
        await copy(value);
        unstable_batchedUpdates(() => {
          setCoping(false);
          setCopied(true);
        });

        onCopy?.();

        if (dontShowToast) {
          return;
        }
        toast.success(toastMessage);
      }}
    />
  );
}
