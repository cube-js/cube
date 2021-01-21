import * as PropTypes from 'prop-types';
import { Menu as AntdMenu } from 'antd';
import ButtonDropdown from './ButtonDropdown';
import styled from 'styled-components';

const Menu = styled(AntdMenu)`
  max-height: 320px;
  overflow: hidden auto;
`

// Can't be a Pure Component due to Dropdown lookups overlay component type to set appropriate styles
const memberMenu = (onClick, availableMembers) => (
  <Menu>
    {availableMembers.length ? (
      availableMembers.map((m) => (
        <Menu.Item key={m.name} onClick={() => onClick(m)}>
          {m.title}
        </Menu.Item>
      ))
    ) : (
      <Menu.Item disabled>No members found</Menu.Item>
    )}
  </Menu>
);

const MemberDropdown = ({ onClick, availableMembers, ...buttonProps }) => (
  <ButtonDropdown
    overlay={memberMenu(onClick, availableMembers)}
    {...buttonProps}
  />
);

MemberDropdown.propTypes = {
  onClick: PropTypes.func.isRequired,
  availableMembers: PropTypes.array.isRequired,
};

export default MemberDropdown;
