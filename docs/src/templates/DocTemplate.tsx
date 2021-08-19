import React, { Component } from 'react';
import { graphql } from 'gatsby';
import { MDXRenderer } from 'gatsby-plugin-mdx';
import { MDXProvider } from '@mdx-js/react'
import Helmet from 'react-helmet';
import { scroller } from 'react-scroll';
import get from 'lodash/get';
import { renameCategory } from '../rename-category';

import "gatsby-remark-mathjax-ssr/mathjax.css";

import * as styles from '../../static/styles/index.module.scss';
import { Page, SetScrollSectionsAndGithubUrlFunction } from '../types';

// define components to using in MDX
import GitHubCodeBlock from "../components/GitHubCodeBlock"
import CubeQueryResultSet from "../components/CubeQueryResultSet"
import GitHubFolderLink from "../components/GitHubFolderLink"

const components = { GitHubCodeBlock, CubeQueryResultSet, GitHubFolderLink }

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

  // setNamesToHeaders() {
  //   // hack to work side navigation
  //   const h1 = document.body.getElementsByTagName('h1');
  //   const h2 = document.body.getElementsByTagName('h2');
  //   const h3 = document.body.getElementsByTagName('h3');
  //   const headers = [h2, h3];

  //   console.log(document.body);

  //   h1?.[0]?.setAttribute('name', 'top');

  //   headers.forEach((tag, index) => {
  //     tag.forEach(header => {
  //       console.log(header, index);
  //       header.setAttribute('name', kebabCase(header.innerHTML));
  //     })
  //   })
  // }

  render() {
    const { mdx = {} } = this.props.data;
    const { frontmatter } = mdx;


    return (
      <div>
        <Helmet title={`${frontmatter.title} | Cube.js Docs`} />
        <div className={styles.docContentWrapper}>
          <div className={styles.docContent}>
            <h1>{frontmatter.title}</h1>
            <MDXProvider components={components}>
              <MDXRenderer>{this.props.data.mdx.body}</MDXRenderer>
            </MDXProvider>
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
      }
    }
  }
`;
