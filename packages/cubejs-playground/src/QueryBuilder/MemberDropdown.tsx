import { Menu as AntdMenu } from 'antd';
import styled from 'styled-components';

import ButtonDropdown from './ButtonDropdown';

const Menu = styled(AntdMenu)`
  max-height: 320px;
  overflow: hidden auto;
`;

// Can't be a Pure Component due to Dropdown lookups overlay component type to set appropriate styles
function memberMenu(onClick, availableMembers) {
  const hasMembers = availableMembers.some((cube) => cube.members.length > 0);

  return (
    <Menu>
      {hasMembers ? (
        availableMembers.map((cube) =>
          cube.members.length > 0 ? (
            <Menu.ItemGroup key={cube.cubeName} title={cube.cubeTitle}>
              {cube.members.map((m) => (
                <Menu.Item key={m.name} data-testid={m.name} onClick={() => onClick(m)}>
                  {m.shortTitle}
                </Menu.Item>
              ))}
            </Menu.ItemGroup>
          ) : null
        )
      ) : (
        <Menu.Item disabled>No members found</Menu.Item>
      )}
    </Menu>
  );
}

const MemberDropdown = ({ onClick, availableMembers, ...buttonProps }: any) => (
  <ButtonDropdown
    overlay={memberMenu(onClick, availableMembers)}
    {...buttonProps}
  />
);

export default MemberDropdown;
