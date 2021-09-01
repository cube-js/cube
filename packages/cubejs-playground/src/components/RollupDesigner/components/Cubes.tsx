import { CheckOutlined } from '@ant-design/icons';
import { Menu } from 'antd';
import styled from 'styled-components';

import { MembersByCube } from '../../../shared/helpers';
import { QueryMemberKey } from '../../../types';

const { SubMenu } = Menu;

const StyledMenu = styled(Menu)`
  max-height: 600px;
  overflow-y: scroll;

  .ant-menu-sub.ant-menu-inline {
    background: white;
  }

  & li > div.ant-menu-submenu-title {
    font-weight: 500;
    color: var(--menu-highlight-color);
  }

  & li.ant-menu-item-group {
    & > div {
      text-transform: uppercase;
      font-size: 10px;
    }

    & ul > li {
      padding-left: 12px !important;
    }
  }

  .ant-menu-item-group-list > li {
    margin: 0 !important;

    &::after {
      display: none;
    }
  }

  .ant-menu-item-selected,
  .ant-menu-item-active:not(.ant-menu-item-selected) {
    background: var(--primary-9) !important;
    color: var(--primary-color);
  }

  .ant-menu-item-selected.ant-menu-item-active {
    background: var(--primary-8) !important;
  }

  .ant-menu-submenu-arrow {
    left: 10px;
    right: initial;
  }
`;

type CubesProps = {
  selectedKeys: string[];
  membersByCube: MembersByCube[];
  onSelect: (memberType: QueryMemberKey, key: string) => void;
};

const MEMBER_TYPES = ['measures', 'dimensions', 'segments', 'timeDimensions'];

export function Cubes({ selectedKeys, membersByCube, onSelect }: CubesProps) {
  const defaultOpenKeys = selectedKeys.map((key) => key.split('.')[0]);

  return (
    <StyledMenu
      selectedKeys={selectedKeys}
      defaultOpenKeys={defaultOpenKeys}
      mode="inline"
      onClick={(event) => {
        // @ts-ignore
        const { membertype } = event.domEvent.target.dataset;

        onSelect(membertype as QueryMemberKey, event.key.toString());
      }}
    >
      {membersByCube.map((cube) => {
        return (
          <SubMenu key={cube.cubeName} title={cube.cubeTitle}>
            {MEMBER_TYPES.map((memberType) => {
              return (
                <Menu.ItemGroup
                  key={memberType}
                  title={
                    memberType === 'timeDimensions'
                      ? 'time dimensions'
                      : memberType
                  }
                >
                  {cube[memberType]
                    .filter(
                      (member) =>
                        !(memberType === 'dimensions' && member.type === 'time')
                    )
                    .map((member) => (
                      <Menu.Item key={member.name} data-membertype={memberType}>
                        <CheckOutlined
                          style={{
                            visibility: selectedKeys.includes(member.name)
                              ? 'visible'
                              : 'hidden',
                          }}
                        />

                        {member.shortTitle}
                      </Menu.Item>
                    ))}
                </Menu.ItemGroup>
              );
            })}
          </SubMenu>
        );
      })}
    </StyledMenu>
  );
}
