import React, { Component } from 'react';
import { renderToString } from 'react-dom/server';
import { graphql } from 'gatsby';
import { MDXRenderer } from 'gatsby-plugin-mdx';
import { MDXProvider } from '@mdx-js/react';
import Helmet from 'react-helmet';
import ReactHtmlParser from 'react-html-parser';
import { scroller } from 'react-scroll';
import { Icon } from 'antd';
import cx from 'classnames';
import kebabCase from 'lodash/kebabCase';
import get from 'lodash/get';
import last from 'lodash/last';
import { renameCategory } from '../rename-category';

import 'katex/dist/katex.min.css';
import '../../static/styles/math.scss';

import FeedbackBlock from '../components/FeedbackBlock';
import ScrollLink, {
  SCROLL_OFFSET,
} from '../components/templates/ScrollSpyLink';

import * as styles from '../../static/styles/index.module.scss';
import { Page, Section, SetScrollSectionsAndGithubUrlFunction } from '../types';

// define components to using in MDX
import GitHubCodeBlock from '../components/GitHubCodeBlock';
import CubeQueryResultSet from '../components/CubeQueryResultSet';
import GitHubFolderLink from '../components/GitHubFolderLink';
import {
  DangerBox,
  InfoBox,
  SuccessBox,
  WarningBox,
} from '../components/AlertBox/AlertBox';
import { LoomVideo } from '../components/LoomVideo/LoomVideo';
import { Grid } from '../components/Grid/Grid';
import { GridItem } from '../components/Grid/GridItem';
import ScrollSpyH2 from '../components/Headers/ScrollSpyH2';
import ScrollSpyH3 from '../components/Headers/ScrollSpyH3';
import MyH2 from '../components/Headers/MyH2';
import MyH3 from '../components/Headers/MyH3';
import { ParameterTable } from '../components/ReferenceDocs/ParameterTable';
import { Snippet, SnippetGroup } from '../components/Snippets/SnippetGroup';

const MyH4: React.FC<{ children: string }> = ({ children }) => {
  return (<h4 id={kebabCase(children)} name={kebabCase(children)}>{children}</h4>);
}

const components = {
  DangerBox,
  InfoBox,
  SuccessBox,
  WarningBox,
  LoomVideo,
  Grid,
  GridItem,
  GitHubCodeBlock,
  CubeQueryResultSet,
  GitHubFolderLink,
  ParameterTable,
  SnippetGroup,
  Snippet,
  h2: ScrollSpyH2,
  h3: ScrollSpyH3,
  h4: MyH4,
};

const MDX = (props) => (
  <MDXProvider components={components}>
    <MDXRenderer>{props?.data?.mdx?.body}</MDXRenderer>
  </MDXProvider>
);
const MDXForSideMenu = (props) => (
  <MDXProvider components={{ ...components, h2: MyH2, h3: MyH3 }}>
    <MDXRenderer>{props?.data?.mdx?.body}</MDXRenderer>
  </MDXProvider>
);

const mdContentCallback = () => {
  const accordionTriggers = document.getElementsByClassName(
    'accordion-trigger'
  );

  Array.prototype.forEach.call(accordionTriggers, (item) => {
    item.onclick = function (e: MouseEvent) {
      e.preventDefault();
      const target = document.getElementById(item.id + '-body');
      target?.classList.toggle('active');
    };
  });
};

const repoBaseUrl = 'https://github.com/cube-js/cube.js/blob/master';
const getGithubUrl = (fileAbsolutePath: string) => {
  const arr = fileAbsolutePath.replace(/(.*)cube\.js\/(.*)/g, '$2').split('/');
  return [repoBaseUrl, ...arr.slice(arr.indexOf('cube.js') + 1)].join('/');
};

// @TODO Find a way to move this out of here
declare global {
  interface Window {
    Prism: any;
  }
}

type Props = {
  changePage(page: Page): void;
  setScrollSectionsAndGithubUrl: SetScrollSectionsAndGithubUrlFunction;
  data: any;
  pageContext: any;
};

type State = {
  nodes: any[];
};

class DocTemplate extends Component<Props, State> {
  state = {
    nodes: [],
  };

  componentWillMount() {
    const { mdx = {} } = this.props.data;
    const { frontmatter } = mdx;

    this.props.changePage({
      scope: frontmatter.scope,
      category: renameCategory(frontmatter.category),
      noscrollmenu: false,
    });

    const hackFixFileAbsPath = this.props.pageContext.fileAbsolutePath.replace(
      '/opt/build/repo/',
      ''
    );

    this.createAnchors(
      <MDXForSideMenu {...this.props} />,
      frontmatter.title,
      getGithubUrl(hackFixFileAbsPath)
    );
  }

  componentDidMount() {
    window.Prism && window.Prism.highlightAll();
    // this.setNamesToHeaders();
    this.scrollToHash();
    mdContentCallback();
  }

  componentDidUpdate() {
    this.scrollToHash();
  }

  scrollToHash = () => {
    setTimeout(() => {
      const nodeID = get(this.props, 'location.hash', '').slice(1);
      if (nodeID) {
        scroller.scrollTo(nodeID, { offset: SCROLL_OFFSET });
      }
    }, 100);
  };

  createAnchors = (element: any, title: string, githubUrl: string) => {
    if (!element) {
      this.props.setScrollSectionsAndGithubUrl([], '');
      return;
    }
    const stringElement = renderToString(element);
    // the code below transforms html from markdown to section-based html
    // for normal scrollspy
    const rawNodes = ReactHtmlParser(stringElement);
    const sectionTags: Section[] = [
      {
        id: kebabCase(title),
        type: 'h1',
        className: styles.topSection,
        nodes: [
          React.createElement(
            'h1',
            { key: 'top', className: styles.topHeader },
            title
          ),
        ],
        title,
      },
    ];

    let currentParentID = kebabCase(title);
    let currentID: string;

    rawNodes.forEach((item) => {
      let linkedHTag;

      // This skips over any inline-comments in the Markdown source, such as
      // `<!-- prettier-ignore-start -->`
      if (!item) {
        return;
      }

      if (
        item.type === 'p' &&
        item.props.children.length === 1 &&
        item.props.children[0].type === 'a'
      ) {
        item = (
          <div
            id={`${item.key}:block-link`}
            key={`${item.key}:block-link`}
            className="block-link"
          >
            {item.props.children[0]}
          </div>
        );
      }

      if (item.type === 'table') {
        item = React.createElement('div', {
          id: `${item.key}:wrapper`,
          key: `${item.key}:wrapper`,
          className: 'table-wrapper',
          children: [
            item,
            React.createElement('div', {
              id: `${item.key}:padding`,
              key: `${item.key}:padding`,
            }),
          ],
        });
      }

      if (item.type === 'h2' || item.type === 'h3') {
        let className = '';
        const prevSection = last(sectionTags) as Section;

        const isPreviousSectionClearable =
          (prevSection.type === 'h1' || prevSection.type === 'h2') &&
          ((prevSection.type === 'h1' && prevSection.nodes.length > 2) ||
            prevSection.nodes.length === 1);

        className = cx(className, {
          [styles.postClearSection]: isPreviousSectionClearable,
        });

        let elementId = item.props.children[0];
        let elementTitle = item.props.children[0];
        // Handle code-block H2 headers
        if (Array.isArray(item.props.children) && item.props.children.length === 1 && typeof item.props.children[0] !== 'string') {
          elementId = item.props.children[0].props.children[0];
        }
        // Handle code-block H3 headers with custom ID prefix
        if (Array.isArray(item.props.children) && item.props.children.length > 1) {
          elementId = item.props.children[1].props.children[0];
        }

        currentID = kebabCase(elementId);

        if (item.type === 'h2') {
          prevSection.className = cx(prevSection.className, {
            [styles.lastSection]: true,
            [styles.clearSection]: isPreviousSectionClearable,
          });

          currentParentID = currentID;
        }

        sectionTags.push({
          id:
            currentParentID != currentID
              ? `${currentParentID}-${currentID}`
              : currentID,
          type: item.type,
          nodes: [],
          title: elementTitle,
          className,
        });

        linkedHTag = React.cloneElement(
          item,
          { className: styles.hTag },
          React.createElement(
            ScrollLink,
            { to: currentID },
            React.createElement(Icon, {
              type: 'link',
              className: styles.hTagIcon,
            }),
            elementId
          )
        );
      }

      last(sectionTags)?.nodes?.push(linkedHTag || item);
    });

    const sections = sectionTags.map((item) => ({
      id: item.id,
      title: item.title,
      type: item.type,
    }));

    this.props.setScrollSectionsAndGithubUrl(sections, githubUrl);
  };

  render() {
    const { mdx = {} } = this.props.data;
    const { frontmatter } = mdx;
    const { isDisableFeedbackBlock } = frontmatter;

    return (
      <div>
        <Helmet>
          <title>{`${frontmatter.title} | Cube Docs`}</title>
          <meta name="description" content={`${frontmatter.title} | Documentation for working with Cube, the open-source analytics framework`}></meta>
        </Helmet>
        <div className={styles.docContentWrapper}>
          <div className={cx(styles.docContent, 'docContent')}>
            <h1 id={kebabCase(frontmatter.title)}>{frontmatter.title}</h1>
            <MDX {...this.props} />
            {!isDisableFeedbackBlock && (
              <FeedbackBlock page={frontmatter.permalink} />
            )}
          </div>
        </div>
      </div>
    );
  }
}

export default DocTemplate;

export const pageQuery = graphql`
  query postByPath($path: String!) {
    mdx(frontmatter: { permalink: { eq: $path } }) {
      body
      frontmatter {
        permalink
        title
        menuTitle
        scope
        category
        frameworkOfChoice
        isDisableFeedbackBlock
      }
    }
  }
`;
