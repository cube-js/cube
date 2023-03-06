import { Dropdown, DropDownProps, ButtonProps } from 'antd';

import { Button } from '../atoms';

type ButtonDropdownProps = {
  onOverlayClose?: () => void;
} & DropDownProps & ButtonProps

const ButtonDropdown = ({
  overlay,
  disabled = false,
  onOverlayClose,
  ...buttonProps
}: ButtonDropdownProps) => {
  return (
    <Dropdown
      disabled={disabled}
      overlay={overlay}
      placement="bottomLeft"
      trigger={['click']}
      onVisibleChange={(visible) => {
        if (!visible) {
          onOverlayClose?.();
        }
      }}
    >
      <Button {...buttonProps} disabled={disabled} />
    </Dropdown>
  );
};

export default ButtonDropdown;
