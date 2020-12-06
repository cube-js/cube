import React from 'react';

import { Col, Menu } from 'antd';
import omit from 'lodash/omit';

import MenuItem from '../templates/MenuItem';
import styles from '../../../static/styles/index.module.scss';
import { useFrameworkOfChoice } from '../../stores/frameworkOfChoice';
import {
  Frontmatter,
  MarkdownNode,
  MobileModes,
  ParsedNodeResults,
  Scopes,
} from '../../types';
import { MenuMode } from 'antd/lib/menu';

const menuOrderCloud = ['Quickstart', 'Configuring Cube Cloud'];

const menuOrder = [
  'Getting Started',
  'Cube.js Introduction',
  'Configuration',
  'Caching',
  'Authentication & Authorization',
  'Cube.js Backend',
  'Data Schema',
  'Cube.js Frontend',
  'Deployment',
  'Cube.js CLI',
  'Examples & Tutorials',
];

const nameRules: Record<string, string> = {
  'Getting Started with Cube.js Schema': 'Introduction',
  'cubejs-backend-server-core': '@cubejs-backend/server-core',
  'cubejs-backend-server': '@cubejs-backend/server',
  'Code Reusability: Export and Import': 'Export and Import',
  'Code Reusability: Extending Cubes': 'Extending Cubes',
  'Code Reusability: Schema Generation': 'Schema Generation',
  'Daily, Weekly, Monthly Active Users': 'Active Users',
};

const getMenuTitle = (title: string) => nameRules[title] || title;

const frontmatterItem = ({ title, menuTitle, permalink }: Frontmatter) => (
  <MenuItem
    to={permalink}
    title={getMenuTitle(menuTitle || title)}
    key={permalink}
  />
);
const nodeParser = ({ frontmatter }: MarkdownNode) =>
  frontmatterItem(frontmatter);

type Props = {
  mode: MenuMode;
  mobileMode?: MobileModes;
  scope?: Scopes;
  selectedKeys?: string[];
  items: ParsedNodeResults;
};

const defaultProps: Props = {
  mode: 'inline',
  mobileMode: MobileModes.CONTENT,
  scope: Scopes.DEFAULT,
  selectedKeys: [],
  items: {},
};

const MainMenu: React.FC<Props> = (props = defaultProps) => {
  const menuProps = omit(props, ['mobileMode', 'scope']);
  const [frameworkOfChoice] = useFrameworkOfChoice();
  const isCloudDocs =
    (props.selectedKeys || []).filter((e) => e.match(/^cloud/)).length > 0;
  const menuOrderResolved = isCloudDocs ? menuOrderCloud : menuOrder;

  return (
    <Col xxl={4} xl={5} lg={7} md={9} xs={props.mobileMode === 'menu' ? 24 : 0}>
      <div className={styles.menuWrapper}>
        <Menu {...menuProps} className={styles.antMenu}>
          <MenuItem to={isCloudDocs ? '/cloud' : '/'} title="Home" />
          {menuOrderResolved.map((item) => {
            const subcategoryData = props.items[item];
            const subCategoryNames = Object.keys(subcategoryData);
            if (
              subCategoryNames.length === 1 &&
              subcategoryData[subCategoryNames[0]].length === 1
            ) {
              return nodeParser(subcategoryData[subCategoryNames[0]][0]);
            }
            return (
              <Menu.SubMenu
                key={item}
                title={getMenuTitle(item)}
                className={styles.antSubMenu}
              >
                {Object.keys(subcategoryData).map((subCategory) => {
                  if (subCategory === 'nocat') {
                    const subItems = subcategoryData[subCategory].filter(
                      (subItem: MarkdownNode) => {
                        return (
                          !subItem.frontmatter.frameworkOfChoice ||
                          subItem.frontmatter.frameworkOfChoice ===
                            frameworkOfChoice
                        );
                      }
                    );

                    return subItems.map(nodeParser);
                  }
                  return (
                    <Menu.ItemGroup key={subCategory} title={subCategory}>
                      {subcategoryData[subCategory].map(nodeParser)}
                    </Menu.ItemGroup>
                  );
                })}
              </Menu.SubMenu>
            );
          })}
        </Menu>
      </div>
    </Col>
  );
};

export default MainMenu;
