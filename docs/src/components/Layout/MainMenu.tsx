import React from 'react';
import PropTypes from 'prop-types';

import { Col, Menu } from 'antd';
import omit from 'lodash/omit';

import MenuItem from 'src/components/templates/MenuItem';
import styles from '../../../static/styles/index.module.scss';
import { useFrameworkOfChoice } from '../../stores/frameworkOfChoice';

const { SubMenu } = Menu;

const menuOrderCloud = [
  "Quickstart",
  "Configuring Cube Cloud"
];

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
  'Examples & Tutorials'
];

const nameRules = {
  "Getting Started with Cube.js Schema": "Introduction",
  "cubejs-backend-server-core": "@cubejs-backend/server-core",
  "cubejs-backend-server": "@cubejs-backend/server",
  "Code Reusability: Export and Import": "Export and Import",
  "Code Reusability: Extending Cubes": "Extending Cubes",
  "Code Reusability: Schema Generation": "Schema Generation",
  "Daily, Weekly, Monthly Active Users": "Active Users",
}

const getMenuTitle = title => nameRules[title] || title;

const frontmatterItem = ({ title, menuTitle, permalink }) => <MenuItem to={permalink} title={getMenuTitle(menuTitle || title)} key={permalink} />;
const nodeParser = ({ frontmatter = {} }) => frontmatterItem(frontmatter);

const MainMenu = props => {
  const menuProps = omit(props, ['mobileMode', 'scope']);
  const [frameworkOfChoice] = useFrameworkOfChoice();
  const isCloudDocs = (props.selectedKeys || []).filter(e => e.match(/^cloud/)).length > 0
  const menuOrderResolved = isCloudDocs ? menuOrderCloud : menuOrder;

  return (
    <Col
      xxl={4}
      xl={5}
      lg={7}
      md={9}
      xs={props.mobileMode === 'menu' ? 24 : 0}
    >
      <div className={styles.menuWrapper}>
        <Menu {...menuProps} className={styles.antMenu}>
          <MenuItem to={isCloudDocs ? '/cloud' : '/'} title="Home" />
          {
            menuOrderResolved.map(item => {
              const subCategories = Object.keys(props.items[item]);
              if (subCategories.length === 1 && props.items[item][subCategories[0]].length === 1) {
                return nodeParser(props.items[item][subCategories[0]][0]);
              }
              return (
                <SubMenu key={item} title={getMenuTitle(item)} className={styles.antSubMenu}>
                  {
                    Object.keys(props.items[item]).map(subCategory => {
                      if (subCategory === 'nocat') {
                        const subItems = props.items[item][subCategory]
                          .filter(item => {
                            return !item.frontmatter.frameworkOfChoice || item.frontmatter.frameworkOfChoice === frameworkOfChoice;
                          });

                        return subItems.map(nodeParser);
                      }
                      return (
                        <Menu.ItemGroup key={subCategory} title={subCategory}>
                          { props.items[item][subCategory].map(nodeParser) }
                        </Menu.ItemGroup>
                      );
                    })
                  }
                </SubMenu>
              );
            })
          }
        </Menu>
      </div>
    </Col>
  )
}

MainMenu.propTypes = {
  mobileMode: PropTypes.oneOf(['content', 'menu', 'search']),
  scope: PropTypes.oneOf(['default', 'cubejs']),
  items: PropTypes.object,
}

MainMenu.defaultProps = {
  mobileMode: 'content',
  scope: 'default',
  items: {},
}

export default MainMenu;
