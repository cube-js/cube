import { useEffect, useRef, useState } from 'react';
import { AvailableCube } from '@cubejs-client/react';
import { ButtonProps, Input, Menu as AntdMenu } from 'antd';
import styled from 'styled-components';
import FlexSearch from 'flexsearch';
import { CubeMember, BaseCubeMember } from '@cubejs-client/core';

import ButtonDropdown from './ButtonDropdown';
import useDeepMemo from '../hooks/deep-memo';
import { getNameMemberPairs } from '../shared/helpers';

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

function filterMembersByKeys(
  members: AvailableCube<CubeMember>[],
  keys: string[]
) {
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

type MemberDropdownProps = {
  availableCubes: AvailableCube<CubeMember>[];
  showNoMembersPlaceholder?: boolean;
  onClick: (member: BaseCubeMember) => void;
} & ButtonProps;

export default function MemberMenu({
  availableCubes,
  showNoMembersPlaceholder = true,
  onClick,
  ...buttonProps
}: MemberDropdownProps) {
  const flexSearch = useRef(FlexSearch.create<string>({ encode: 'advanced' }));
  const [search, setSearch] = useState<string>('');
  const [filteredKeys, setFilteredKeys] = useState<string[]>([]);

  const index = flexSearch.current;
  const hasMembers = availableCubes.some(
    (cube) => cube.members.filter(({ isVisible }) => isVisible).length > 0
  );

  const indexedMembers = useDeepMemo(() => {
    getNameMemberPairs(availableCubes).forEach(([name, { title }]) =>
      index.add(name as any, title)
    );

    return Object.fromEntries(getNameMemberPairs(availableCubes));
  }, [availableCubes]);

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
    ? filterMembersByKeys(availableCubes, filteredKeys)
    : availableCubes;

  return (
    <ButtonDropdown
      {...buttonProps}
      overlay={
        <Menu
          onClick={(event) => {
            setSearch('');
            setFilteredKeys([]);
            onClick(indexedMembers[event.key]);
          }}
        >
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

              {members.map((cube) => {
                const members = cube.members.filter(
                  ({ isVisible }) => isVisible
                );

                if (!members.length) {
                  return null;
                }

                return (
                  <Menu.ItemGroup key={cube.cubeName} title={cube.cubeTitle}>
                    {members.map((m) => (
                      <Menu.Item key={m.name} data-testid={m.name}>
                        {m.shortTitle}
                      </Menu.Item>
                    ))}
                  </Menu.ItemGroup>
                );
              })}
            </>
          ) : showNoMembersPlaceholder ? (
            <Menu.Item disabled>No members found</Menu.Item>
          ) : null}
        </Menu>
      }
    />
  );
}
