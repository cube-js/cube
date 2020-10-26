import React from 'react';
import Helmet from 'react-helmet';
import { graphql } from 'gatsby';
import { Header, Hero, Footer, Feature, Social, SEO, PartsListing, Layout } from 'guides-base';
import config from '../../data/SiteConfig';

import featureOneImg from './feature-1.svg';
import featureTwoImg from './feature-2.svg';
import featureThreeImg from './feature-3.svg';

import hero from './hero.png';

class Index extends React.Component {
  render() {
    const partsEdges = this.props.data.allMarkdownRemark.edges;
    return (
      <Layout config={config}>
        <SEO config={config} />
        <Helmet title={config.siteTitle} />
        <Header githubUrl={config.githubUrl} />
        <Hero
          title="Material UI Dashboard with Angular"
          subtitle="Learn how to build a Material UI Dashboard with Angular and Cube.js"
          demoUrl="https://flaky-sheep.gcp-us-central1.cubecloudapp.dev/"
          startUrl={partsEdges[0].node.fields.slug}
          socialButtons={<Social align="flex-start" siteTitle={config.siteTitle} siteUrl={config.siteUrl} />}
          media={
            <video muted autoPlay playsInline loop preload="auto" poster={hero}>
              <source type="video/mp4" src="video/preview.mp4" />
            </video>
          }
          withFrame
        />
        <Feature
          imageAlign="left"
          image={featureOneImg}
          metaTitle="MATERIAL UI DASHBOARD"
          title="Build an Interactive Multi-Page Dashboard with Angular, Material UI, and Cube.js"
          text="Explore how to create your own Angular Material UI dashboard. You will learn step by step how to build a comprehensive dashboard which retrieves and visualizes data from your database without writing SQL code."
        />
        <Feature
          imageAlign="right"
          image={featureTwoImg}
          metaTitle="DATA SCHEMA"
          title="Build an Analytics API with Cube.js"
          text="You will learn how to model data with Cube.js data schema and build a clean API interface to power your analytics dashboard. This guide shows how to create complex metrics and describe relationships in the data."
        />
        <Feature
          imageAlign="left"
          image={featureThreeImg}
          metaTitle="DATA VISUALIZATION TECHNIQUES"
          title="Learn How to Use Various Chart Types to Visualize Data"
          text="Explore how to work with such Material UI components as Bar Chart, Doughnut Chart, and Data Table. Learn the essentials of using these components to build convenient visualizations for business metrics and KPIs."
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
    allMarkdownRemark(limit: 2000, sort: { fields: [frontmatter___order], order: ASC }) {
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
