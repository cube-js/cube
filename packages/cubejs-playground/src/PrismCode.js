import React from 'react';
import Prism from 'prismjs';

class PrismCode extends React.Component {
  componentDidMount() {
    Prism.highlightAll();
  }

  componentDidUpdate() {
    Prism.highlightAll();
  }

  render() {
    return (
      <pre style={this.props.style}>
        <code className="language-javascript">{this.props.code}</code>
      </pre>
    );
  }
}

export default PrismCode;
