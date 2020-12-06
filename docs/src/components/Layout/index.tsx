import React from 'react';
import cx from 'classnames';
import PropTypes from 'prop-types';
import uniq from 'lodash/uniq';
import { Layout, Row, Col } from 'antd';
import { StaticQuery, graphql } from 'gatsby';

import FrameworkOfChoiceStore, {
  useFrameworkOfChoice
} from '../../stores/frameworkOfChoice';
import Search from '../Search';
import Header from '../Header';
import MobileFooter from './MobileFooter';
import MainMenu from './MainMenu';
import ScrollMenu from './ScrollMenu';
import FrameworkSwitcher from '../templates/FrameworkSwitcher';

import styles from '../../../static/styles/index.module.scss';
import '../../../static/styles/prism.scss';
import { renameCategory } from '../../rename-category';

// trim leading and trailing slashes
export const trimSlashes = (str) => str.replace(/^\/|\/$/g, '');

const MOBILE_MODE_SET = ['content', 'menu', 'search'];

const pathname = location => {
  if (process.env.NODE_ENV === 'production') {
    return location.pathname.replace(process.env.PATH_PREFIX, '');
  }

  return location.pathname;
}

const menuSort = (a, b) => {
  const af = a.frontmatter || {};
  const bf = b.frontmatter || {};
  if (af.menuOrder && bf.menuOrder) return af.menuOrder - bf.menuOrder;
  if (af.menuOrder) return 1;
  if (bf.menuOrder) return -1;

  return af.title.localeCompare(bf.title);
};

const blankCategory = ({ frontmatter = {} }) => frontmatter.subCategory || 'nocat';

const fixNodeCategory = node => (
  { ...node, frontmatter: { ...node.frontmatter, category: renameCategory(node.frontmatter.category) } }
);

const parseResults = nodes => {
  const allNodes = nodes.reduce((categories, { node = {} }) => {
    const newNode = fixNodeCategory(node);
    (categories[newNode.frontmatter.category] = categories[newNode.frontmatter.category] || []).push(newNode);
    return categories;
  },
  {}
  );

  Object.keys(allNodes).forEach(node =>
    allNodes[node] = allNodes[node].sort(menuSort).reduce((subCategories, node) => {
      (subCategories[blankCategory(node)] = subCategories[blankCategory(node)] || []).push(node);
      return subCategories;
    }, {})
  );

  return allNodes;
};

const FrameworkChoiceFix = (props) => {
  const framework = props.framework;
  const [frameworkOfChoice, setFrameworkOfChoice] = useFrameworkOfChoice();

  if (framework && framework !== frameworkOfChoice) {
    setFrameworkOfChoice(framework);
  }

  return '';
}

class AppLayout extends React.Component {
  state = {
    sections: [],
    mobileMode: 'content',
    githubUrl: '',
  }

  componentDidMount() {
    // S3 trailing slash
    if (/\/$/.test(this.props.location.pathname)) {
      let path = this.props.location.pathname.replace(/\/$/, '');

      if (this.props.location.hash) {
        path += this.props.location.hash
      }

      this.props.navigate(path);
    }
  }

  changePage = props => {
    this.setState({
      scope: props.scope,
      openSubMenus: [props.category],
      mobileMode: 'content',
      noscrollmenu: props.noscrollmenu
    });
  }

  onOpenChange = keys => {
    this.setState({
      openSubMenus: uniq(keys).filter(k => !!k)
    });
  }

  setMobileMode = mobileMode => {
    if (MOBILE_MODE_SET.includes(mobileMode) && this.state.mobileMode !== mobileMode) {
      this.setState({ mobileMode })
    }
  }

  setScrollSectionsAndGithubUrl = (sections, githubUrl) => {
    this.setState({
      sections,
      githubUrl
    });
  }

  render() {
    const { children, location, data: pageData } = this.props;

    const path = pathname(location);

    const menuProps = {
      mode: "inline",
      selectedKeys: [trimSlashes(path)],
      openKeys: this.state.openSubMenus,
      onOpenChange: this.onOpenChange,
      mobileMode: this.state.mobileMode,
      scope: this.state.scope,
    }
    const childrenWithProps = React.Children.map(children, child =>
      React.cloneElement(child, { ...this.props,
        layout: false,
        changePage: this.changePage,
        setScrollSectionsAndGithubUrl: this.setScrollSectionsAndGithubUrl
      })
    );

    const pageFrameworkOfChoice = pageData && pageData.markdownRemark.frontmatter.frameworkOfChoice;

    return (
      <FrameworkOfChoiceStore>
        <FrameworkChoiceFix framework={pageFrameworkOfChoice}/>
        <StaticQuery
          query={graphql`
            query {
              allMarkdownRemark(
                limit: 1000
              ) {
                edges {
                  node {
                    html
                    fileAbsolutePath
                    frontmatter {
                      permalink
                      title
                      menuTitle
                      scope
                      category
                      menuOrder
                      subCategory
                      frameworkOfChoice
                    }
                  }
                }
              }
            }`}
          render={data => (
            <Row>
              <Header
                className={cx(styles.header, { [styles.fixed]: this.state.mobileMode === 'menu' })}
                mobileSearch={this.state.mobileMode === 'search'}
              >
                <Search
                  mobile={this.state.mobileMode === 'search'}
                  onClose={() => this.setMobileMode('content')}
                  navigate={this.props.navigate}
                />
              </Header>
              <Col
                md={24}
                className={cx(styles.contentColumn, { fixed: this.state.mobileMode === 'menu' })}
              >
                <MainMenu items={parseResults(data.allMarkdownRemark.edges)} {...menuProps} />
                <Col
                  xxl={16}
                  xl={14}
                  lg={17}
                  md={15}
                  xs={this.state.mobileMode === 'content' ? 24 : 0}
                >
                  {
                    pageFrameworkOfChoice && <FrameworkSwitcher value={pageFrameworkOfChoice} />
                  }
                  <Layout.Content className={styles.docContentWrapper} style={{ margin: '0 24px 100px 24px' }}>
                    {
                      childrenWithProps
                    }
                  </Layout.Content>
                </Col>
                {!this.state.noscrollmenu && <ScrollMenu sections={this.state.sections} githubUrl={this.state.githubUrl} />}
              </Col>
              <MobileFooter
                mobileMode={this.state.mobileMode}
                setMobileMode={this.setMobileMode}
              />
            </Row>
          )}
        />
      </FrameworkOfChoiceStore>
    )
  }
}

AppLayout.propTypes = {
  children: PropTypes.object,
  navigate: PropTypes.func.isRequired,
}

export default AppLayout;
