import React from "react";
import { Link } from "gatsby";
import styled from 'styled-components';
import media from "styled-media-query";
import theme from '../../theme';

const Container = styled.div`
  max-width: 900px;
  margin: 0 auto;
  margin-top: 200px;
  margin-bottom: 140px;
  ${media.lessThan("medium")`
    margin-top: 100px;
  `}
  ${media.lessThan("large")`
    padding-left: ${theme.landingPaddingLessThenLarge};
    padding-right: ${theme.landingPaddingLessThenLarge};
  `}
`

const Title = styled.h1`
  color: ${theme.colors.darkPurple};
  text-align: center;
  margin-bottom: 45px;
`

const LinkContainer = styled.div`
  padding: 35px 0;
  border-bottom: 1px solid rgba(161, 161, 181, 0.3);

   &:nth-child(2) {
    border-top: 1px solid rgba(161, 161, 181, 0.3);
  }

  a {
    color: ${theme.colors.darkPurple};
    font-size: 20px;
    text-decoration: none;

    &:hover {
      text-decoration: underline;
    }
  }
`

class PartsListing extends React.Component {
  getPostList() {
    const postList = [];
    this.props.partsEdges.forEach(postEdge => {
      postList.push({
        path: postEdge.node.fields.slug,
        title: postEdge.node.frontmatter.title,
        order: postEdge.node.frontmatter.order,
        excerpt: postEdge.node.excerpt,
        timeToRead: postEdge.node.timeToRead
      });
    });
    return postList;
  }

  render() {
    const postList = this.getPostList();
    return (
      <Container>
        <Title>Table of Contents</Title>
        {postList.map(post => (
          <LinkContainer key={post.path}>
            <Link to={post.path}>
              {post.title}
            </Link>
          </LinkContainer>
        ))}
      </Container>
    );
  }
}

export default PartsListing;
