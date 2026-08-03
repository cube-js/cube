import { useEffect, useRef, useState } from 'react';
import { AvailableCube } from '@cubejs-client/react';
import { ButtonProps, Input, Menu as AntdMenu, Tag } from 'antd';
import styled from 'styled-components';
import FlexSearch from 'flexsearch';
import { LockOutlined } from '@ant-design/icons';
import { CubeMember, BaseCubeMember } from '@cubejs-client/core';

import { ButtonDropdown } from './ButtonDropdown';
import { useDeepMemo } from '../hooks';
import { getNameMemberPairs } from '../shared/members';

const Menu = styled(AntdMenu)`
  max-height: 320px;
  overflow: hidden auto;
  padding-top: 0;

  li.ant-dropdown-menu-item-active {
    background: #f3f3fb;
  }
`;

const SearchMenuItem = styled.li`
  position: sticky !important;
  top: 0  !important;
  // this isn't the best solution ever, but according to the situation other solutions are worse
  // antd uses double class pattern (.disabled.active.active) to override the value of background color. actually the
  // easiest way to override it is to use smtn with higher specificity
  background: white !important;
  padding-top: 8px;
  padding-bottom: 8px;
  margin-bottom: 8px;
  cursor: default;
  z-index: 9;

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
    ) !important;
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
  const searchInputRef = useRef<Input | null>(null);
  const index = useRef(new FlexSearch.Index({ tokenize: 'forward' })).current;
  const [search, setSearch] = useState<string>('');
  const [filteredKeys, setFilteredKeys] = useState<string[]>([]);

  const hasMembers = availableCubes.some((cube) => cube.members.length > 0);

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

      setFilteredKeys(results as string[]);
    })();

    return () => {
      currentSearch = '';
    };
  }, [index, search]);

  const [show, setShow] = useState(false);

  const members = search
    ? filterMembersByKeys(availableCubes, filteredKeys)
    : availableCubes;

  return (
    <ButtonDropdown
      show={show}
      {...buttonProps}
      overlayStyles={{
        minWidth: 280
      }}
      onOverlayClose={() => {
        setShow(false);
        setFilteredKeys([]);
        setSearch('');
        searchInputRef.current?.setValue('');
      }}
      onOverlayOpen={() => setShow(true)}
      onClick={() => {
        // we need to delay focusing since React needs to render <Menu /> first :)
        setTimeout(() => {
          searchInputRef.current?.focus({ preventScroll: true });
        });
      }}
      overlay={
        <div className="test">
          <Menu
            className="ant-dropdown-menu ant-dropdown-menu-root"
            onKeyDown={(e) => {
              if (
                [
                  'ArrowDown',
                  'ArrowUp',
                  'ArrowLeft',
                  'ArrowRight',
                  'Enter',
                  'Escape',
                  'Tab',
                  'CapsLock',
                ].includes(e.key)
              ) {
                return;
              }

              if (document.activeElement === searchInputRef.current?.input)
                return;

              searchInputRef.current?.focus({ preventScroll: true });
            }}
            onClick={(event) => {
              if (['__not-found__', '__search_field__'].includes(event.key)) {
                return;
              }

              setSearch('');
              setFilteredKeys([]);
              onClick(indexedMembers[event.key]);
              searchInputRef.current?.setValue('');
              setShow(false);
            }}
          >
            {hasMembers ? (
              <>
                <SearchMenuItem id="hhhh" className="ant-menu-item ant-menu-item-active ant-menu-item-disabled ant-menu-item-only-child">
                  <Input
                    ref={searchInputRef}
                    placeholder="Search"
                    autoFocus
                    allowClear
                    onKeyDown={(event) => {
                      if (
                        ['ArrowDown', 'ArrowUp', 'Enter'].includes(event.key)
                      ) {
                        event.preventDefault();
                      }

                      if (['ArrowLeft', 'ArrowRight'].includes(event.key)) {
                        event.stopPropagation();
                      }
                    }}
                    onChange={(event) => {
                      setSearch(event.target.value);
                    }}
                  />
                </SearchMenuItem>

                {members.map((cube) => {
                  if (!cube.members.length) {
                    return null;
                  }

                  return (
                    <Menu.ItemGroup
                      key={cube.cubeName}
                      title={
                        cube.type ? (
                          <span>
                            {cube.cubeTitle}{' '}
                            {cube.public === false ? <LockOutlined /> : null}{' '}
                            <Tag
                              color={
                                cube.type === 'view' ? '#D26E0B' : '#7A77FF'
                              }
                              style={{
                                padding: '0 4px',
                                lineHeight: 1.5,
                              }}
                            >
                              {cube.type}
                            </Tag>
                          </span>
                        ) : (
                          cube.cubeTitle
                        )
                      }
                    >
                      {cube.members.map((m) => (
                        <Menu.Item
                          key={m.name}
                          data-testid={m.name}
                          className="ant-dropdown-menu-item"
                        >
                          {m.shortTitle}{' '}
                          {m.isVisible === false ? <LockOutlined /> : null}
                        </Menu.Item>
                      ))}
                    </Menu.ItemGroup>
                  );
                })}
              </>
            ) : showNoMembersPlaceholder ? (
              <Menu.Item key="__not-found__" disabled>
                No members found
              </Menu.Item>
            ) : null}
          </Menu>
        </div>
      }
    />
  );
}
