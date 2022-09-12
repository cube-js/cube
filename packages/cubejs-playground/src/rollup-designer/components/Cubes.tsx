import { CheckOutlined, SearchOutlined } from '@ant-design/icons';
import { AvailableMembers } from '@cubejs-client/react';
import { Input, Menu } from 'antd';
import { useLayoutEffect } from 'react';
import styled from 'styled-components';

import { useDeepMemo } from '../../hooks/deep-memo';
import { getMembersByCube, MembersByCube } from '../../shared/members';
import { QueryMemberKey } from '../../types';
import { useCubeMemberSearch } from './cube-member-search';

const { SubMenu } = Menu;

const StyledMenu = styled(Menu)`
  position: relative;
  max-height: 600px;
  overflow-y: scroll;
  overflow-x: hidden;

  li {
    font-size: var(--font-size-base);
  }

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

const SearchInputWrapper = styled.div`
  position: relative;
  border-right: 1px solid rgba(0, 0, 0, 0.06);
  padding: 16px 12px 10px 12px;

  ::after {
    z-index: 1;
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

const MEMBER_TYPES = ['measures', 'dimensions', 'segments', 'timeDimensions'];

function filterMembersByCube(
  membersByCube: MembersByCube[],
  keys: string[]
): MembersByCube[] {
  return membersByCube
    .map((cube) => {
      const membersByType = MEMBER_TYPES.map((type) => [
        type,
        (cube[type] || []).filter(({ name }) => keys.includes(name)),
      ]);

      if (!membersByType.some(([, members]) => members.length)) {
        return false;
      }

      return {
        ...cube,
        ...Object.fromEntries(membersByType),
      };
    })
    .filter(Boolean);
}

type CubesProps = {
  selectedKeys: string[];
  openKeys: string[];
  memberTypeCubeMap: AvailableMembers;
  firstOpenCubeName: string | null;
  onSelect: (memberType: QueryMemberKey, key: string) => void;
  onOpenKeysChange: (openKeys: string[]) => void;
};

export function Cubes({
  memberTypeCubeMap,
  openKeys,
  selectedKeys,
  firstOpenCubeName,
  onSelect,
  onOpenKeysChange,
}: CubesProps) {
  const allCubeKeys = useDeepMemo(() => {
    return getMembersByCube(memberTypeCubeMap).map(({ cubeName }) => cubeName);
  }, [memberTypeCubeMap]);

  const { keys, search, inputProps } = useCubeMemberSearch(memberTypeCubeMap);

  const membersByCube = search
    ? filterMembersByCube(getMembersByCube(memberTypeCubeMap), keys)
    : getMembersByCube(memberTypeCubeMap);

  useLayoutEffect(() => {
    if (firstOpenCubeName) {
      setTimeout(() => {
        document.querySelector('[data-menu="cubes"]')?.scroll({
          top: (
            document.querySelector(
              `[data-cube="${firstOpenCubeName}"]`
            ) as HTMLElement
          )?.offsetTop,
          behavior: 'smooth',
        });
      }, 100);
    }
  }, [firstOpenCubeName]);

  return (
    <>
      <SearchInputWrapper>
        <Input
          {...inputProps}
          allowClear
          autoFocus
          suffix={search ? null : <SearchOutlined />}
        />
      </SearchInputWrapper>

      <StyledMenu
        data-menu="cubes"
        selectedKeys={selectedKeys}
        openKeys={search ? allCubeKeys : openKeys}
        mode="inline"
        onClick={(event) => {
          const { membertype } = (event.domEvent.currentTarget as HTMLElement).dataset;

          onSelect(
            membertype as QueryMemberKey,
            event.key.toString().replace('td:', '')
          );
        }}
      >
        {membersByCube.map((cube) => {
          return (
            <SubMenu
              key={cube.cubeName}
              data-cube={cube.cubeName}
              title={cube.cubeTitle}
              onTitleClick={({ key }) => {
                if (openKeys.includes(key)) {
                  onOpenKeysChange(openKeys.filter((value) => value !== key));
                } else {
                  onOpenKeysChange([...openKeys, key]);
                }
              }}
            >
              {MEMBER_TYPES.map((memberType) => {
                if (!cube[memberType].length) {
                  return null;
                }

                return (
                  <Menu.ItemGroup
                    key={memberType}
                    title={
                      memberType === 'timeDimensions'
                        ? 'time dimensions'
                        : memberType
                    }
                  >
                    {cube[memberType].map((member) => {
                      const key =
                        memberType === 'timeDimensions'
                          ? `td:${member.name}`
                          : member.name;

                      return (
                        <Menu.Item key={key} data-membertype={memberType}>
                          <CheckOutlined
                            style={{
                              visibility: selectedKeys.includes(key)
                                ? 'visible'
                                : 'hidden',
                            }}
                          />

                          {member.shortTitle}
                        </Menu.Item>
                      );
                    })}
                  </Menu.ItemGroup>
                );
              })}
            </SubMenu>
          );
        })}
      </StyledMenu>
    </>
  );
}
