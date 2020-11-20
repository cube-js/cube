import React from 'react';
import PropTypes from 'prop-types';

import { Col, Menu } from 'antd';
import omit from 'lodash/omit';

import MenuItem from 'src/components/templates/MenuItem';
import styles from '../../../static/styles/index.module.scss';
import { useFrameworkOfChoice } from '../../stores/frameworkOfChoice';

const { SubMenu } = Menu;

const menuOrder = [
  'Getting Started',
  'Cube.js Introduction',
  'Configuration',
  'Cube.js CLI',
  'Cube.js Backend',
  'Cube.js Frontend',
  'Data Schema',
  'Deployment',
  'Tutorials',
  'Examples'
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
          <MenuItem to="/" title="Home" />
          {
            menuOrder.map(item => {
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
