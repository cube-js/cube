import React from 'react';

import { Col, Menu } from 'antd';
import cx from 'classnames';
import omit from 'lodash/omit';

import MenuItem from '../templates/MenuItem';
import * as styles from '../../../static/styles/index.module.scss';
import { useFrameworkOfChoice } from '../../stores/frameworkOfChoice';
import {
  Frontmatter,
  MarkdownNode,
  MobileModes,
  ParsedNodeResults,
  Scopes,
} from '../../types';
import { MenuMode } from 'antd/lib/menu';
import { layout } from '../../theme';

const menuOrder = [
  'Cube.js Introduction',
  'Getting Started',
  'Configuration',
  'Data Modeling',
  'Caching',
  'Authentication & Authorization',
  'REST API',
  'GraphQL API',
  'SQL API',
  'Frontend Integrations',
  'Workspace',
  'Deployment',
  'Monitoring',
  'Examples & Tutorials',
  'FAQs',
  'Guides',
  'Reference'
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

  return (
    <Col
      {...layout.leftSidebar.width}
      xs={props.mobileMode === 'menu' ? 24 : 0}
      className={cx(styles.menuWrapperCol)}
    >
      <div className={cx(styles.menuWrapper, styles.menuWrapperHack)}>
        <Menu {...menuProps} className={styles.antMenu}>
          <MenuItem to="/" title="Home" />
          {menuOrder.map((item) => {
            const subcategoryData = props.items[item];
            const subCategoryNames = Object.keys(subcategoryData);

            const filteredSubcategoryData = subCategoryNames.reduce((result, subCategoryName) => {
              const items = subcategoryData[subCategoryName]

              if (items.length > 0) {
                return {
                  ...result,
                  [subCategoryName]: items,
                }
              }
              return result;
            }, {} as typeof subcategoryData);

            if (
              subCategoryNames.length === 1 &&
              filteredSubcategoryData[subCategoryNames[0]].length === 1
            ) {
              return nodeParser(filteredSubcategoryData[subCategoryNames[0]][0]);
            }
            return (
              <Menu.SubMenu
                key={item}
                title={getMenuTitle(item)}
                className={styles.antSubMenu}
              >
                {Object.keys(filteredSubcategoryData).map((subCategory) => {
                  if (subCategory === 'nocat') {
                    const subItems = filteredSubcategoryData[subCategory].filter(
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
                      {filteredSubcategoryData[subCategory].map(nodeParser)}
                    </Menu.ItemGroup>
                  );
                })}
              </Menu.SubMenu>
            );
          })}
          <MenuItem to="https://cube.dev/blog/category/changelog" title="Changelog" />
        </Menu>
      </div>
    </Col>
  );
};

export default MainMenu;
