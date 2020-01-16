import React from "react";
import Helmet from "react-helmet";
import { graphql } from "gatsby";
import styled from 'styled-components'
import media from "styled-media-query";
import theme, { sharedStyles } from '../theme';
import Layout from "../layout";
import SEO from "../components/SEO/SEO";
import TableOfContents from "../components/TableOfContents/TableOfContents";
import Header from "../components/Header/Header";
import Footer from "../components/Footer/Footer";
import NextPrev from "../components/NextPrev/NextPrev";
import Social from "../components/Social/Social";

const Markdown = styled.div`
  ${sharedStyles.markdown}
`;

const ContentHeader = styled.h1`
  font-family: ${theme.fontFamily};
  color: ${theme.colors.darkPurple};
  font-size: 32px;
  margin-bottom: 35px;
`;

const Container = styled.div`
  padding: 92px 30px 30px;
  max-width: ${theme.contentPageMaxWidth};
  margin: 0 auto;
  display: flex;
  justify-content: space-between;
`;

const ContentContainer = styled.div`
  min-width: 0;
  max-width: 680px;
  ${media.lessThan("medium")`
    max-width: 400px;
  `}
`

export default class PostTemplate extends React.Component {
  render() {
    const { data, pageContext } = this.props;
    const { slug, nextslug, nexttitle, prevslug, prevtitle, config = {}, tableOfContents } = pageContext;
    const postNode = data.markdownRemark;
    const post = postNode.frontmatter;
    if (!post.id) {
      post.id = slug;
    }
    return (
      <Layout config={config}>
        <Helmet>
          <title>{`${post.title} | ${config.siteTitle}`}</title>
        </Helmet>
        <SEO config={config} postPath={slug} postNode={postNode} postSEO />
        <Header githubUrl={config.githubUrl} />
        <Container>
          <ContentContainer>
            <ContentHeader>{post.title}</ContentHeader>
            <Markdown dangerouslySetInnerHTML={{ __html: postNode.html }} />
            <Social
              path={slug}
              title={post.title}
              siteUrl={config.siteUrl}
              siteTitle={config.siteTitle}
            />
          </ContentContainer>
          <TableOfContents current={slug} data={tableOfContents} githubUrl={config.githubUrl} />
        </Container>
        <NextPrev
          nextSlug={nextslug}
          nextTitle={nexttitle}
          prevSlug={prevslug}
          prevTitle={prevtitle}
        />
        <Footer />
      </Layout>
    );
  }
}

/* eslint no-undef: "off" */
export const pageQuery = graphql`
  query BlogPostBySlug($slug: String!) {
    markdownRemark(fields: { slug: { eq: $slug } }) {
      html
      timeToRead
      excerpt
      frontmatter {
        title
      }
      fields {
        slug
      }
    }
  }
`;
