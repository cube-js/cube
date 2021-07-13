import React from 'react';
import cx from 'classnames';
import uniq from 'lodash/uniq';
import { Layout, Row, Col } from 'antd';
import { StaticQuery, graphql } from 'gatsby';

import FrameworkOfChoiceStore, {
  useFrameworkOfChoice,
} from '../../stores/frameworkOfChoice';
import EventBanner from '../EventBanner';
import Search from '../Search';
import Header from '../Header';
import MobileFooter from './MobileFooter';
import MainMenu from './MainMenu';
import ScrollMenu from './ScrollMenu';
import FrameworkSwitcher from '../templates/FrameworkSwitcher';

import * as styles from '../../../static/styles/index.module.scss';
import '../../../static/styles/prism.scss';
import { renameCategory } from '../../rename-category';
import {
  Category,
  MarkdownNode,
  MobileModes,
  Page,
  ParsedNodeResults,
  Scopes,
  SectionWithoutNodes,
  SetScrollSectionsAndGithubUrlFunction,
} from '../../types';
import { RouteComponentProps } from '@reach/router';
import { MenuMode } from 'antd/lib/menu';
import { layout } from '../../theme';

// trim leading and trailing slashes
export const trimSlashes = (str: string) => str.replace(/^\/|\/$/g, '');

const MOBILE_MODE_SET = ['content', 'menu', 'search'];

const pathname = (location: Location) => {
  if (process.env.NODE_ENV === 'production') {
    return location.pathname.replace(process.env.PATH_PREFIX as string, '');
  }

  return location.pathname;
};

const menuSort = (a: MarkdownNode, b: MarkdownNode) => {
  const af = a.frontmatter || {};
  const bf = b.frontmatter || {};
  if (af.menuOrder && bf.menuOrder) return af.menuOrder - bf.menuOrder;
  if (af.menuOrder) return 1;
  if (bf.menuOrder) return -1;

  return af.title.localeCompare(bf.title);
};

const blankCategory = ({ frontmatter }: MarkdownNode) =>
  frontmatter.subCategory || 'nocat';

const fixNodeCategory = (node: MarkdownNode): MarkdownNode => ({
  ...node,
  frontmatter: {
    ...node.frontmatter,
    category: renameCategory(node.frontmatter.category),
  },
});

// @TODO Move this entire thing into a Gatsby plugin
const parseResults = (
  nodes: LayoutQueryResponse['allMarkdownRemark']['edges']
) => {
  let allNodes = nodes.reduce((categories, { node }) => {
    const newNode = fixNodeCategory(node);
    const existingNodes = categories[newNode.frontmatter.category] || [];

    return {
      ...categories,
      [newNode.frontmatter.category]: [...existingNodes, newNode],
    };
  }, {} as Record<string, MarkdownNode[]>);

  Object.keys(allNodes).forEach((categoryName) => {
    // @ts-ignore
    allNodes[categoryName] = allNodes[categoryName]
      .sort(menuSort)
      .reduce((subCategories, node) => {
        const existingNodes = subCategories[blankCategory(node)] || [];

        return {
          ...subCategories,
          [blankCategory(node)]: [...existingNodes, node],
        };
      }, {} as Category);
  });

  return (allNodes as unknown) as ParsedNodeResults;
};

type FrameworkChoiceFixProps = {
  framework: string;
};

const FrameworkChoiceFix: React.FC<FrameworkChoiceFixProps> = (props) => {
  const framework = props.framework;
  const [frameworkOfChoice, setFrameworkOfChoice] = useFrameworkOfChoice();

  if (framework && framework !== frameworkOfChoice) {
    setFrameworkOfChoice(framework);
  }

  return null;
};

type Props = {
  data: PageQueryResponse;
  navigate(url: string): void;
};

type State = {
  githubUrl: string;
  mobileMode: MobileModes;
  noscrollmenu?: boolean;
  openSubMenus?: string[];
  scope?: Scopes;
  sections: SectionWithoutNodes[];
};

class AppLayout extends React.Component<
  Required<RouteComponentProps<Props>>,
  State
> {
  state: State = {
    sections: [],
    mobileMode: MobileModes.CONTENT,
    githubUrl: '',
  };

  componentDidMount() {
    // S3 trailing slash
    if (/\/$/.test(this.props.location.pathname)) {
      let path = this.props.location.pathname.replace(/\/$/, '');

      if (this.props.location.hash) {
        path += this.props.location.hash;
      }

      this.props.navigate(path);
    }
  }

  changePage = (props: Page) => {
    this.setState({
      scope: props.scope,
      openSubMenus: [props.category],
      mobileMode: MobileModes.CONTENT,
      noscrollmenu: props.noscrollmenu,
    });
  };

  onOpenChange = (keys: string[]) => {
    this.setState({
      openSubMenus: uniq(keys).filter((k) => !!k),
    });
  };

  setMobileMode = (mobileMode: MobileModes) => {
    if (
      MOBILE_MODE_SET.includes(mobileMode) &&
      this.state.mobileMode !== mobileMode
    ) {
      this.setState({ mobileMode });
    }
  };

  setScrollSectionsAndGithubUrl: SetScrollSectionsAndGithubUrlFunction = (
    sections,
    githubUrl
  ) => {
    this.setState({
      sections,
      githubUrl,
    });
  };

  render() {
    const { children, location, data: pageData } = this.props;

    const path = pathname(location);

    const menuProps = {
      mode: 'inline' as MenuMode,
      selectedKeys: [trimSlashes(path)],
      openKeys: this.state.openSubMenus,
      onOpenChange: this.onOpenChange,
      mobileMode: this.state.mobileMode,
      scope: this.state.scope,
    };
    const childrenWithProps = React.Children.map(children, (child) =>
      React.cloneElement(child as any, {
        ...this.props,
        layout: false,
        changePage: this.changePage,
        setScrollSectionsAndGithubUrl: this.setScrollSectionsAndGithubUrl,
      })
    );

    const pageFrameworkOfChoice: string =
      pageData && pageData.markdownRemark.frontmatter.frameworkOfChoice!;

    return (
      <FrameworkOfChoiceStore>
        <FrameworkChoiceFix framework={pageFrameworkOfChoice} />
        <StaticQuery
          query={layoutQuery}
          render={(data: LayoutQueryResponse) => (
            <>
            <EventBanner />
            <Row>
              <Header
                className={cx(styles.header, {
                  [styles.fixed]: this.state.mobileMode === MobileModes.MENU,
                })}
                mobileSearch={this.state.mobileMode === MobileModes.SEARCH}
              >
                <Search
                  mobile={this.state.mobileMode === MobileModes.SEARCH}
                  onClose={() => this.setMobileMode(MobileModes.CONTENT)}
                  navigate={this.props.navigate}
                />
              </Header>
              <Col
                md={24}
                className={cx(styles.contentColumn, {
                  fixed: this.state.mobileMode === MobileModes.MENU,
                })}
              >
                <MainMenu
                  items={parseResults(data.allMarkdownRemark.edges)}
                  {...menuProps}
                />
                <Col
                  {...layout.contentArea.width}
                  xs={
                    this.state.mobileMode === 'content'
                      ? { span: 22, offset: 1 }
                      : 0
                  }
                >
                  {pageFrameworkOfChoice && (
                    <FrameworkSwitcher value={pageFrameworkOfChoice} />
                  )}
                  <Layout.Content
                    className={styles.contentWrapper}
                    style={{ margin: '0 24px 100px 24px' }}
                  >
                    {childrenWithProps}
                  </Layout.Content>
                </Col>
                {!this.state.noscrollmenu && (
                  <ScrollMenu
                    sections={this.state.sections}
                    githubUrl={this.state.githubUrl}
                  />
                )}
              </Col>
              <MobileFooter
                mobileMode={this.state.mobileMode}
                setMobileMode={this.setMobileMode}
              />
            </Row>
            </>
          )}
        />
      </FrameworkOfChoiceStore>
    );
  }
}

interface PageQueryResponse {
  markdownRemark: MarkdownNode;
}

interface Edge<T> {
  node: T;
}

interface LayoutQueryResponse {
  allMarkdownRemark: {
    edges: Edge<MarkdownNode>[];
  };
}

const layoutQuery = graphql`
  query LayoutQuery {
    allMarkdownRemark(limit: 1000) {
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
  }
`;

export default AppLayout;
