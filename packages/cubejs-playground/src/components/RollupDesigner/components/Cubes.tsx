import { Menu } from 'antd';
import { useMemo } from 'react';
import { useSetter } from '../../../hooks/setter';

import { MembersByCube, ucfirst } from '../../../shared/helpers';
import { QueryMemberKey } from '../../../types';

const { SubMenu } = Menu;

type CubesProps = {
  defaultSelectedKeys: string[];
  membersByCube: MembersByCube[];
  onSelect: (memberType: QueryMemberKey, key: string) => void;
};

const MEMBER_TYPES = ['measures', 'dimensions', 'timeDimensions'];

export function Cubes({
  defaultSelectedKeys,
  membersByCube,
  onSelect,
}: CubesProps) {
  const [selectedKeys, toggleSelectedKey] = useSetter<string[], string>(
    (state, key) => {
      if (!key) {
        return state;
      }

      return state.includes(key)
        ? state.filter((v) => v !== key)
        : [...state, key];
    },
    defaultSelectedKeys
  );

  const defaultOpenKeys = defaultSelectedKeys.map((key) => key.split('.')[0]);

  return (
    <Menu
      style={{ width: 256 }}
      selectedKeys={selectedKeys}
      defaultOpenKeys={defaultOpenKeys}
      mode="inline"
      onClick={(event) => {
        // @ts-ignore
        const { membertype } = event.domEvent.target.dataset;
        toggleSelectedKey(event.key.toString());

        onSelect(membertype as QueryMemberKey, event.key.toString());
      }}
    >
      {membersByCube.map((cube) => {
        return (
          <SubMenu key={cube.cubeName} title={cube.cubeTitle}>
            {MEMBER_TYPES.map((memberType) => {
              return (
                <Menu.ItemGroup key={memberType} title={ucfirst(memberType)}>
                  {cube[memberType].map((member) => {
                    return (
                      <Menu.Item key={member.name} data-memberType={memberType}>
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
