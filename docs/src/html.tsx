import React from 'react';
import Head from './Head';
import EventBanner from './components/EventBanner';

export type HTMLProps = {
  css?: any;
  htmlAttributes: any;
  headComponents: any;
  bodyAttributes: any;
  body: any;
  preBodyComponents: any;
  postBodyComponents: any;
};

class HTML extends React.Component<HTMLProps> {
  render() {
    return (
      <html {...this.props.htmlAttributes}>
        <Head headComponents={this.props.headComponents} />
        <body {...this.props.bodyAttributes}>
          <EventBanner />
          {this.props.preBodyComponents}
          <div
            key={`body`}
            id="___gatsby"
            dangerouslySetInnerHTML={{ __html: this.props.body }}
          />
          {this.props.postBodyComponents}
        </body>
      </html>
    );
  }
}

export default HTML;
