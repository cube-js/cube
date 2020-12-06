import React from "react"
import Head from './Head';

class HTML extends React.Component {
  render() {

    return (
      <html {...this.props.htmlAttributes}>
        <Head headComponents={this.props.headComponents} />
        <body {...this.props.bodyAttributes}>
          {this.props.preBodyComponents}
          <div
            key={`body`}
            id="___gatsby"
            dangerouslySetInnerHTML={{ __html: this.props.body }}
          />
          {this.props.postBodyComponents}
        </body>
      </html>
    )
  }
};

export default HTML;
