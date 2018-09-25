import React from 'react';
import { func, object } from 'prop-types';
import { equals } from 'ramda';

class QueryRenderer extends React.Component {
  constructor(props) {
    super(props);
    this.state = {};
    if (props.query) {
      this.load(props.query);
    }
  }

  componentDidUpdate(prevProps) {
    let query = this.props.query;
    if (!equals(prevProps.query, query)) {
      this.setState({ isLoading: true });
      this.load(query);
    }
  }

  load(query) {
    this.props.cubejsApi.load(query)
      .then(resultSet => this.setState({ resultSet, error: null, isLoading: false }))
      .catch(error => this.setState({ resultSet: null, error, isLoading: false }));
  }

  render() {
    const loadState = {
      error: this.state.error,
      resultSet: this.state.resultSet,
      loadingState: { isLoading: this.state.isLoading }
    };
    if (this.props.render) {
      return this.props.render(loadState);
    }
    return null;
  }
}

QueryRenderer.propTypes = {
  render: func.required,
  afterRender: func,
  cubejsApi: object.required,
  query: object
};

export { QueryRenderer };
