import { useEffect, useMemo, useState } from 'react';
import { Input, Menu as AntdMenu } from 'antd';
import styled from 'styled-components';
import Fuse from 'fuse.js';

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
    background: linear-gradient(to bottom, rgba(255, 255, 255, 1), rgba(255, 255, 255, 0));
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

function flattenedMembersByCube(members: any[]) {
  return Object.values(
    members
      .sort((a, b) => (a.shortTitle > b.shortTitle ? 1 : -1))
      .reduce((memo, member) => {
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
  const [cubeMembers, setCubeMembers] = useState<null | any[]>(null);
  const [flattenedMembers, setFlattendMembers] = useState<null | any[]>(null);

  const hasMembers = availableMembers.some((cube) => cube.members.length > 0);
  const [cubeName, memberName] = search.split('.');

  const members =
    filteredMembers != null
      ? flattenedMembersByCube(filteredMembers)
      : availableMembers;

  useEffect(() => {
    setFlattendMembers(flattenMembers(availableMembers));
  }, [availableMembers]);

  const fuse = useMemo(() => {
    if (flattenedMembers) {
      return new Fuse(flattenedMembers, {
        keys: ['cubeTitle', 'shortTitle'],
        threshold: 0.2,
      });
    }

    return null;
  }, [flattenedMembers]);

  const cubeFuse = useMemo(() => {
    if (flattenedMembers != null) {
      return new Fuse(flattenedMembers, {
        keys: ['cubeTitle'],
        threshold: 0.2,
      });
    }

    return null;
  }, [flattenedMembers, memberName]);

  const memberFuse = useMemo(() => {
    if (cubeMembers != null && memberName !== undefined) {
      return new Fuse(cubeMembers, {
        keys: ['shortTitle'],
        threshold: 0.2,
      });
    }

    return null;
  }, [cubeMembers, memberName]);

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
      const members = currentFuse
        .search(searchValue)
        .map(({ item }) => item)
        .filter(Boolean);

      setFilteredMembers(members);

      if (memberName === '') {
        setCubeMembers(members);
      }
    } else {
      setFilteredMembers(null);
    }
  }, [search, cubeName, memberName, fuse, cubeFuse]);

  useEffect(() => {
    document.getElementById('member-dropdown-menu')?.scroll({
      top: 0,
    });
  }, [search]);

  return (
    <Menu id="member-dropdown-menu">
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
