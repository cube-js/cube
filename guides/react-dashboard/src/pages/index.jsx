import React from "react";
import Helmet from "react-helmet";
import { graphql } from "gatsby";
import Layout from "../layout";
import PartsListing from "../components/PartsListing/PartsListing";
import SEO from "../components/SEO/SEO";
import Header from "../components/Header/Header";
import Hero from "../components/Hero/Hero";
import Footer from "../components/Footer/Footer";
import Feature from "../components/Feature/Feature";
import config from "../../data/SiteConfig";

import featureOneImg from "./feature-1.png";
import featureOneTwo from "./feature-2.png";
import featureOneThree from "./feature-3.png";

import { page } from 'cubedev-tracking';

class Index extends React.Component {
  componentDidMount() {
    page();
  }

  render() {
    const partsEdges = this.props.data.allMarkdownRemark.edges;
    return (
      <Layout>
        <SEO />
        <Helmet title={config.siteTitle} />
        <Header />
        <Hero startUrl={partsEdges[0].node.fields.slug} />
        <Feature
          imageAlign='left'
          image={featureOneImg}
          metaTitle="Internal tools"
          title="Build Internal Dashboards"
          text="Learn how to build a full-stack React Dashboard from the database to the dynamic charts and drag-n-drop features. You’ll learn how to create an analytics API with Cube.js, how to generate a React dashboard boilerplate, and then customize it however you want."
        />
        <Feature
          imageAlign='right'
          image={featureOneTwo}
          metaTitle="customer-facing apps"
          title="Build Customer-Facing Dashboards"
          text="Learn how to build the React Dashboard directly into your app. You’ll learn the best practices on the backend organization of analytics API and building a customer-facing dynamic React Dashboard."
        />
        <Feature
          imageAlign='left'
          image={featureOneThree}
          metaTitle="analytics platforms"
          title="Build an Analytics Platform"
          text="Do you want to build your own data analytics platform, like Mixpanel or Google Analytics? You can learn how to set up a backend infrastructure, as well as understand the best practices both on the backend and frontend."
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
          excerpt
          timeToRead
          frontmatter {
            title
            order
          }
        }
      }
    }
  }
`;
