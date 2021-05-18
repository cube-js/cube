import { useEffect, useMemo, useState } from 'react';
import { Menu as AntdMenu, Input } from 'antd';
import styled from 'styled-components';
import Fuse from 'fuse.js';

import ButtonDropdown from './ButtonDropdown';

const Menu = styled(AntdMenu)`
  max-height: 320px;
  overflow: hidden auto;

  li.ant-dropdown-menu-item-active {
    background: #f3f3fb;
  }
`;

function flattenMembers(members) {
  const map = new Map();

  members.map((cube) =>
    cube.members.forEach((member) => {
      map.set(`${cube.cubeName}:${member.name}`, {
        ...cube,
        ...member,
      });
    })
  );

  return Array.from(map.values());
}

function flattendMembersByCube(members: any[]) {
  return Object.values(
    members.reduce((memo, member) => {
      const { cubeName, cubeTitle, ...memberProps } = member;

      memo[member.cubeName] = {
        cubeName,
        cubeTitle,
        members: [...(memo[member.cubeName]?.members || []), memberProps],
      };

      return memo;
    }, {})
  );
}

// Can't be a Pure Component due to Dropdown lookups overlay component type to set appropriate styles
function memberMenu(onClick, availableMembers) {
  const [search, setSearch] = useState<string>('');
  const [filteredMembers, setFilteredMembers] = useState<null | any[]>(null);
  const [flattendMembers, setFlattendMembers] = useState<null | any[]>(null);

  const hasMembers = availableMembers.some((cube) => cube.members.length > 0);
  const [cubeName, memberName] = search.split('.');

  const members =
    filteredMembers != null
      ? flattendMembersByCube(filteredMembers)
      : availableMembers;

  useEffect(() => {
    setFlattendMembers(flattenMembers(availableMembers));
  }, [availableMembers]);

  const fuse = useMemo(() => {
    if (flattendMembers) {
      return new Fuse(flattendMembers, {
        keys: ['cubeTitle', 'shortTitle'],
        threshold: 0.5,
      });
    }

    return null;
  }, [flattendMembers]);

  const cubeFuse = useMemo(() => {
    if (flattendMembers != null) {
      return new Fuse(flattendMembers, {
        keys: ['cubeTitle'],
        threshold: 0.5,
      });
    }

    return null;
  }, [flattendMembers, memberName]);

  const memberFuse = useMemo(() => {
    if (filteredMembers != null && memberName) {
      return new Fuse(filteredMembers, {
        keys: ['shortTitle'],
        threshold: 0.3,
      });
    }

    return null;
  }, [filteredMembers, memberName]);

  useEffect(() => {
    let currentFuse: Fuse<any> | null;
    let searchValue = '';

    if (memberName === undefined) {
      currentFuse = fuse;
      searchValue = search;
    } else if (memberName === '') {
      currentFuse = cubeFuse;
      searchValue = cubeName;
    } else {
      currentFuse = memberFuse;
      searchValue = memberName;
    }

    if (currentFuse && searchValue) {
      setFilteredMembers(
        currentFuse
          .search(searchValue)
          .map(({ item }) => item)
          .filter(Boolean)
      );
    } else {
      setFilteredMembers(null);
    }
  }, [search, cubeName, memberName, fuse, cubeFuse]);

  return (
    <Menu>
      {hasMembers ? (
        <>
          <Menu.Item disabled>
            <Input
              placeholder="Search"
              autoFocus
              value={search}
              allowClear
              onChange={(event) => {
                setSearch(event.target.value);
              }}
            />
          </Menu.Item>

          {members.map((cube) =>
            cube.members.length > 0 ? (
              <Menu.ItemGroup key={cube.cubeName} title={cube.cubeTitle}>
                {cube.members.map((m) => (
                  <Menu.Item
                    key={m.name}
                    data-testid={m.name}
                    onClick={() => {
                      setSearch('');
                      setFilteredMembers(null);
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

const MemberDropdown = ({ onClick, availableMembers, ...buttonProps }: any) => (
  <ButtonDropdown
    overlay={memberMenu(onClick, availableMembers)}
    {...buttonProps}
  />
);

export default MemberDropdown;
