import { Menu } from 'antd';

import { MembersByCube, ucfirst } from '../../../shared/helpers';
import { QueryMemberKey } from '../../../types';

const { SubMenu } = Menu;

type CubesProps = {
  selectedKeys: string[];
  membersByCube: MembersByCube[];
  onSelect: (memberType: QueryMemberKey, key: string) => void;
};

const MEMBER_TYPES = ['measures', 'dimensions', 'timeDimensions'];

export function Cubes({ selectedKeys, membersByCube, onSelect }: CubesProps) {
  const defaultOpenKeys = selectedKeys.map((key) => key.split('.')[0]);

  return (
    <Menu
      style={{
        width: 256,
        marginLeft: -24,
      }}
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
                <Menu.ItemGroup key={memberType} title={ucfirst(memberType)}>
                  {cube[memberType]
                    .filter((member) => {
                      return !(
                        memberType === 'dimensions' && member.type === 'time'
                      );
                    })
                    .map((member) => {
                      return (
                        <Menu.Item
                          key={member.name}
                          data-membertype={memberType}
                        >
                          {member.title}
                        </Menu.Item>
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
