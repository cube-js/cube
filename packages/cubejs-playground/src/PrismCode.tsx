import { Component, CSSProperties } from 'react';
import Prism from 'prismjs';

type PrismCodeProps = {
  code: string;
  language?: string;
  style?: CSSProperties;
};

export default class PrismCode extends Component<PrismCodeProps> {
  componentDidMount() {
    Prism.highlightAll();
  }

  componentDidUpdate() {
    Prism.highlightAll();
  }

  render() {
    return (
      <pre style={this.props.style}>
        <code
          data-testid="prism-code"
          className={`language-${this.props.language || 'javascript'}`}
        >
          {this.props.code}
        </code>
      </pre>
    );
  }
}
