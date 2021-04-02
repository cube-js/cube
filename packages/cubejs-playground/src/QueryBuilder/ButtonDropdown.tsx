import { Dropdown } from 'antd';

import { Button } from '../atoms';

const ButtonDropdown = ({ overlay, disabled = false, ...buttonProps }: any) => {
  return (
    <Dropdown
      disabled={disabled}
      overlay={overlay}
      placement="bottomLeft"
      trigger={['click']}
    >
      <Button {...buttonProps} disabled={disabled} data-iddd={111} />
    </Dropdown>
  );
};

export default ButtonDropdown;
