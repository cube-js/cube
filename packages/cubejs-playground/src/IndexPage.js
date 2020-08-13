import React, { Component } from 'react';
import { fetch } from 'whatwg-fetch';
import { Spin } from 'antd';
import { Redirect } from 'react-router-dom';

class IndexPage extends Component {
  constructor(props) {
    super(props);
    this.state = {};
  }

  async componentDidMount() {
    await this.loadFiles();
  }

  async loadFiles() {
    const res = await fetch('/playground/files');
    const result = await res.json();
    this.setState({
      files: result.files,
    });
  }

  render() {
    if (!this.state.files) {
      return (
        <div style={{ textAlign: 'center', padding: 24 }}>
          <Spin />
        </div>
      );
    }
    return (
      <Redirect
        to={
          !this.state.files.length ||
          (this.state.files.length === 1 &&
            this.state.files[0].fileName === 'Orders.js')
            ? '/schema'
            : '/build'
        }
      />
    );
  }
}

export default IndexPage;
