import React from 'react';
import Helmet from 'react-helmet';
import { graphql } from 'gatsby';
import { Header, Hero, Footer, Feature, Social, SEO, PartsListing, Layout } from 'guides-base';
import config from '../../data/SiteConfig';

import featureOneImg from './pq0xxdnziks2copbfy3r.png';
import featureTwoImg from './lqwki78rj4vnr5kmrb4a.png';
import featureThreeImg from './u7bj4b8jg8r44m6w586w.png';

import hero from './demo.png';

class Index extends React.Component {
  render() {
    const partsEdges = this.props.data.allMarkdownRemark.edges;
    return (
      <Layout config={config}>
        <SEO config={config} />
        <Helmet title={config.siteTitle} />
        <Header githubUrl={config.githubUrl} />
        <Hero
          title="React Pivot Table with AG Grid and Cube.js"
          subtitle="We'll learn how to add a pivot table to a React app using AG Grid, the self-proclaimed best JavaScript grid in the world."
          demoUrl="https://react-pivot-table-demo.cube.dev"
          startUrl={partsEdges[0].node.fields.slug}
          socialButtons={<Social align="flex-start" siteTitle={config.siteTitle} siteUrl={config.siteUrl} />}
          media={
            <img src={hero} />
          }
          withFrame
        />
        <Feature
          imageAlign="left"
          image={featureOneImg}
          metaTitle="BEST REACT GRID IN THE WORLD"
          title="Build a Pivot Table with AG Grid"
          text="We'll build a pivot table data visualization in a React application and explore the features of AG Grid."
        />
        <Feature
          imageAlign="right"
          image={featureTwoImg}
          metaTitle="CUBE.JS ANALYTICAL API"
          title="Create an Analytical API in Minutes"
          text="Learn how to use Cube.js, a powerful open-source analytical API platform, to bootstrap an analytical API and define a data schema for your dataset."
        />
        <Feature
          imageAlign="left"
          image={featureThreeImg}
          metaTitle="UNLIMITED FEATURES"
          title="Extend the Pivot Table"
          text="Take this example and add column sorting, CSV export via a context menu, drag-and-drop in the sidebar, and much more."
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
