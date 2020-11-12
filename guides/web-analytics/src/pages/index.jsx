import React from "react";
import Helmet from "react-helmet";
import { graphql } from "gatsby";
import { Header, Hero, Footer, Feature, Social, SEO, PartsListing, Layout } from "guides-base";
import config from "../../data/SiteConfig";

import featureOneImg from "./feature-1.png";
import featureTwoImg from "./feature-2.png";
import featureThreeImg from "./feature-3.png";

import styled from 'styled-components';
import media from "styled-media-query";

import hero from "./hero.png";

const StyledHeroImage = styled.img`
  ${media.greaterThan("large")`
    margin-top: -50px;
  `}
`;

class Index extends React.Component {
  render() {
    const partsEdges = this.props.data.allMarkdownRemark.edges;
    return (
      <Layout config={config}>
        <SEO config={config} />
        <Helmet title={config.siteTitle} />
        <Header githubUrl={config.githubUrl} />
        <Hero
          title="Building an Open Source Web Analytics Platform"
          subtitle="Learn how to build open source Google Analytics alternative with Cube.js."
          demoUrl="https://web-analytics-demo.cube.dev/"
          startUrl={partsEdges[0].node.fields.slug}
          socialButtons={<Social align="flex-start" siteTitle={config.siteTitle} siteUrl={config.siteUrl} />}
          media={
            <video muted autoPlay playsInline loop preload="auto" poster={hero}>
              <source type="video/mp4" src="videos/web-analytics.mp4" />
            </video>
          }
          withFrame
        />
        <Feature
          imageAlign='left'
          image={featureOneImg}
          metaTitle="Hackable"
          title="Fully Customizable"
          text="Learn how to create your own, completely custom web analytics platform. You will learn how to setup the data collection engine, SQL database, define metrics and build custom  frontend."
        />
        <Feature
          imageAlign='right'
          image={featureTwoImg}
          metaTitle="Embeddable"
          title="Easy to Integrate into Existing App"
          text="Backend components can be easily deployed as microservices into your existing stack. The frontend is a pure React application based on Material UI without any custom styles. You can embed any part of the frontend into your existing application and customize the look and feel to match your styles."
        />
        <Feature
          imageAlign='left'
          image={featureThreeImg}
          metaTitle="Performance first"
          title="Fast and Scalable"
          text="The response time is under 50 ms by using Cube.js pre-aggregations. It scales well for tracking up to several million daily active users. To achieve this performance, Cube.js stores and manages aggregated tables in MySQL with a 5-minute refresh rate."
        />
        <PartsListing partsEdges={partsEdges} />
        <Footer />
      </Layout>
    );
  }
}

export default Index;

/* eslint no-undef: "off" */
export const pageQuery = graphql`
  query IndexQuery {
    allMarkdownRemark(
      limit: 2000
      sort: { fields: [frontmatter___order], order: ASC }
    ) {
      edges {
        node {
          fields {
            slug
          }
          frontmatter {
            title
            order
          }
        }
      }
    }
  }
`;
