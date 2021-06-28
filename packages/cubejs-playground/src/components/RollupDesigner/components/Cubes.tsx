import { Menu } from 'antd';

import { MembersByCube, ucfirst } from '../../../shared/helpers';

const { SubMenu } = Menu;

type CubesProps = {
  membersByCube: MembersByCube[];
};

const MEMBER_TYPES = ['measures', 'dimensions', 'timeDimensions'];

export function Cubes({ membersByCube }: CubesProps) {
  return (
    <Menu
      onClick={() => undefined}
      style={{ width: 256 }}
      defaultSelectedKeys={['1']}
      defaultOpenKeys={['sub1']}
      mode="inline"
    >
      {membersByCube.map((cube) => {
        return (
          <SubMenu key={cube.cubeName} title={cube.cubeTitle}>
            {MEMBER_TYPES.map((memberType) => {
              return (
                <Menu.ItemGroup key={memberType} title={ucfirst(memberType)}>
                  {cube[memberType].map((member) => {
                    return (
                      <Menu.Item key={member.name}>{member.title}</Menu.Item>
                    );
                  })}
                </Menu.ItemGroup>
              );
            })}
          </SubMenu>
        );
      })}
    </Menu>
  );
}
