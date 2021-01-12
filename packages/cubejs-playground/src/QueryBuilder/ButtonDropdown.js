import * as PropTypes from 'prop-types';
import { Dropdown } from 'antd';
import { Button } from '../components';

const ButtonDropdown = ({ overlay, ...buttonProps }) => (
  <Dropdown
    overlay={overlay} placement="bottomLeft" trigger={['click']}>
    <Button {...buttonProps} />
  </Dropdown>
);

ButtonDropdown.propTypes = {
  overlay: PropTypes.object.isRequired,
};

export default ButtonDropdown;
