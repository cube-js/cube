import React from "react";
import Helmet from "react-helmet";
import { graphql } from "gatsby";
import { Header, Hero, Footer, Feature, Social, SEO, PartsListing, Layout } from "guides-base";
import config from "../../data/SiteConfig";

import featureOneImg from "./feature-1.png";
import featureOneTwo from "./feature-2.png";
import featureOneThree from "./feature-3.png";

import hero from "./hero.png";

class Index extends React.Component {
  render() {
    const partsEdges = this.props.data.allMarkdownRemark.edges;
    return (
      <Layout config={config}>
        <SEO config={config} />
        <Helmet title={config.siteTitle} />
        <Header githubUrl={config.githubUrl} />
        <Hero
          title="D3 Dashboard Tutorial"
          subtitle="Learn how to build a D3 dashboard with example in React, Material UI and Cube.js."
          demoUrl="https://d3-dashboard-demo.cube.dev/"
          startUrl={partsEdges[0].node.fields.slug}
          socialButtons={<Social align="flex-start" siteTitle={config.siteTitle} siteUrl={config.siteUrl} />}
          media={ <img alt="hero" src={hero} /> }
        />
        <Feature
          imageAlign='left'
          image={featureOneImg}
          metaTitle="d3 dashboard example"
          title="Build Dashboard App with D3 and React"
          text="This guide shows how to build a dashboard application with React, D3.js and Material UI. You’ll learn how to set up a database, seed it with data, build an API endpoint on top of it, and then visualize data on the frontend with D3.js."
        />
        <Feature
          imageAlign='right'
          image={featureOneTwo}
          metaTitle="data schema"
          title="Build Analytics API with Cube.js"
          text="You will learn how to model data with Cube.js data schema and build clean API interface to power your analytics dashboard. The guide shows how to create complex metrics and describe relationships in the data."
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
