import { ButtonProps } from 'antd';
import { useEffect, useRef } from 'react';
import styled from 'styled-components';

import { Button } from '../atoms';

const TooltipTriggerWrapper = styled.div`
  position: relative;
`;

const TooltipContent = styled.div`
  position: absolute;
  left: 16px;
  top: 40px;
  z-index: 9999;
  left: 0;
  overflow: hidden;
  box-shadow: 0 3px 6px -4px rgba(0, 0, 0, 0.12),
    0 6px 16px 0 rgba(0, 0, 0, 0.08), 0 9px 28px 8px rgba(0, 0, 0, 0.05);
`;

function TooltipTrigger({ children, shown, onClose, ...overlayStyles }) {
  const ref = useRef<any>(null);

  useEffect(() => {
    document.addEventListener('mousedown', (event: any) => {
      if (!ref?.current) {
        return;
      }

      if (!ref.current.contains(event.target)) {
        onClose();
      }
    });
  }, []);

  return (
    <TooltipTriggerWrapper>
      {children[0]}
      {shown ? (
        <TooltipContent
          ref={ref}
          className="ant-dropdown ant-dropdown-placement-bottomLeft "
          style={{ ...overlayStyles }}
        >
          {children[1]}
        </TooltipContent>
      ) : null}
    </TooltipTriggerWrapper>
  );
}

type ButtonDropdownProps = {
  show: boolean;
  overlay: React.ReactNode;
  overlayStyles?: any;
  onOverlayClose: () => void;
  onOverlayOpen: () => void;
  onItemClick?: () => void;
} & ButtonProps;

export function ButtonDropdown({
  show,
  overlay,
  disabled = false,
  overlayStyles,
  onOverlayClose,
  onOverlayOpen,
  onItemClick,
  ...buttonProps
}: ButtonDropdownProps) {
  return (
    <TooltipTrigger
      shown={show}
      {...overlayStyles}
      onClose={() => {
        onOverlayClose();
      }}
    >
      <Button
        {...buttonProps}
        onClick={() => {
          onOverlayOpen();
        }}
        disabled={disabled}
      />
      <div onClick={onItemClick}>{overlay}</div>
    </TooltipTrigger>
  );
}
