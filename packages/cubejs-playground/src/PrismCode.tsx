import { Component, CSSProperties } from 'react';
import Prism from 'prismjs';

type TPrismCodeProps = {
  code: string;
  language?: string;
  style?: CSSProperties;
};

export default class PrismCode extends Component<TPrismCodeProps> {
  componentDidMount() {
    Prism.highlightAll();
  }

  componentDidUpdate() {
    Prism.highlightAll();
  }

  render() {
    return (
      <pre data-testid="prism-code" style={this.props.style}>
        <code className={`language-${this.props.language || 'javascript'}`}>
          {this.props.code}
        </code>
      </pre>
    );
  }
}
