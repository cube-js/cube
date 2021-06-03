import { useEffect, useRef, useState } from 'react';
import { AvailableCube } from '@cubejs-client/react';
import { Input, Menu as AntdMenu } from 'antd';
import styled from 'styled-components';
import FlexSearch from 'flexsearch';

import ButtonDropdown from './ButtonDropdown';

const Menu = styled(AntdMenu)`
  max-height: 320px;
  overflow: hidden auto;
  padding-top: 0;

  li.ant-dropdown-menu-item-active {
    background: #f3f3fb;
  }
`;

const SearchMenuItem = styled(Menu.Item)`
  position: sticky;
  top: 0;
  background: white;
  padding-top: 10px;
  padding-bottom: 0;
  margin-bottom: 16px;

  ::after {
    display: block;
    position: absolute;
    content: '';
    width: 100%;
    left: 0;
    bottom: -20px;
    height: 20px;
    background: linear-gradient(
      to bottom,
      rgba(255, 255, 255, 1),
      rgba(255, 255, 255, 0)
    );
  }
`;

function getKeyTitle(members) {
  const items: [string, string][] = [];

  members.forEach((cube) =>
    cube.members.forEach(({ name, title }) => {
      items.push([name, title]);
    })
  );

  return items;
}

function filterMembersByKeys(members: AvailableCube[], keys: string[]) {
  const cubeNames = keys.map((key) => key.split('.')[0]);

  return members
    .filter(({ cubeName }) => cubeNames.includes(cubeName))
    .map((cube) => {
      return {
        ...cube,
        members: cube.members.filter(({ name }) => keys.includes(name)),
      };
    });
}

// Can't be a Pure Component due to Dropdown lookups overlay component type to set appropriate styles
function memberMenu(onClick, availableMembers: AvailableCube[]) {
  const flexSearch = useRef(FlexSearch.create<string>({ encode: 'advanced' }));
  const [search, setSearch] = useState<string>('');
  const [filteredKeys, setFilteredKeys] = useState<string[]>([]);

  const index = flexSearch.current;
  const hasMembers = availableMembers.some((cube) => cube.members.length > 0);

  useEffect(() => {
    getKeyTitle(availableMembers).forEach(([name, title]) =>
      index.add(name as any, title)
    );
  }, [availableMembers]);

  useEffect(() => {
    let currentSearch = search;

    (async () => {
      const results = await index.search(search);
      if (currentSearch !== search) {
        return;
      }

      setFilteredKeys(results);
    })();

    return () => {
      currentSearch = '';
    };
  }, [index, search]);

  const members = search
    ? filterMembersByKeys(availableMembers, filteredKeys)
    : availableMembers;

  return (
    <Menu>
      {hasMembers ? (
        <>
          <SearchMenuItem disabled>
            <Input
              placeholder="Search"
              autoFocus
              value={search}
              allowClear
              onKeyDown={(event) => {
                if (['ArrowDown', 'ArrowUp'].includes(event.key)) {
                  event.preventDefault();
                }
              }}
              onChange={(event) => {
                setSearch(event.target.value);
              }}
            />
          </SearchMenuItem>

          {members.map((cube) =>
            cube.members.length > 0 ? (
              <Menu.ItemGroup key={cube.cubeName} title={cube.cubeTitle}>
                {cube.members.map((m) => (
                  <Menu.Item
                    key={m.name}
                    data-testid={m.name}
                    onClick={() => {
                      setSearch('');
                      setFilteredKeys([]);
                      onClick(m);
                    }}
                  >
                    {m.shortTitle}
                  </Menu.Item>
                ))}
              </Menu.ItemGroup>
            ) : null
          )}
        </>
      ) : (
        <Menu.Item disabled>No members found</Menu.Item>
      )}
    </Menu>
  );
}

type MemberDropdownProps = {
  availableMembers: AvailableCube[];
  [key: string]: any;
};

const MemberDropdown = ({
  availableMembers,
  onClick,
  ...buttonProps
}: MemberDropdownProps) => (
  <ButtonDropdown
    overlay={memberMenu(onClick, availableMembers)}
    {...buttonProps}
  />
);

export default MemberDropdown;
