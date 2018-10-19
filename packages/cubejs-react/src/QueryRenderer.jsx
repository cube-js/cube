import React from 'react';
import * as PropTypes from 'prop-types';
import { equals, map, toPairs, fromPairs } from 'ramda';

export default class QueryRenderer extends React.Component {
  constructor(props) {
    super(props);
    this.state = {};
  }

  componentDidMount() {
    if (this.props.query) {
      this.load(this.props.query);
    }
    if (this.props.queries) {
      this.loadQueries(this.props.queries);
    }
  }

  componentDidUpdate(prevProps) {
    let query = this.props.query;
    if (!equals(prevProps.query, query)) {
      this.load(query);
    }

    let queries = this.props.queries;
    if (!equals(prevProps.queries, queries)) {
      this.loadQueries(queries);
    }
  }

  load(query) {
    this.setState({ isLoading: true, resultSet: null, error: null, sqlQuery: null });
    if (this.props.loadSql === 'only') {
      this.props.cubejsApi.sql(query)
        .then(sqlQuery => this.setState({ sqlQuery, error: null, isLoading: false }))
        .catch(error => this.setState({ resultSet: null, error, isLoading: false }))
    } else if (this.props.loadSql) {
      Promise.all([this.props.cubejsApi.sql(query), this.props.cubejsApi.load(query)])
        .then(([sqlQuery, resultSet]) => this.setState({ sqlQuery, resultSet, error: null, isLoading: false }))
        .catch(error => this.setState({ resultSet: null, error, isLoading: false }))
    } else {
      this.props.cubejsApi.load(query)
        .then(resultSet => this.setState({ resultSet, error: null, isLoading: false }))
        .catch(error => this.setState({ resultSet: null, error, isLoading: false }))
    }
  }

  loadQueries(queries) {
    this.setState({ isLoading: true, resultSet: null, error: null });

    const resultPromises = Promise.all(toPairs(queries).map(
      ([name, query]) => this.props.cubejsApi.load(query).then(r => [name, r])
    ));

    resultPromises
      .then(resultSet => this.setState({
        resultSet: fromPairs(resultSet),
        error: null,
        isLoading: false
      }))
      .catch(error => this.setState({ resultSet: null, error, isLoading: false }))
  }

  render() {
    const loadState = {
      error: this.state.error,
      resultSet: this.props.queries ? (this.state.resultSet || {}) : this.state.resultSet,
      loadingState: { isLoading: this.state.isLoading },
      sqlQuery: this.state.sqlQuery
    };
    if (this.props.render) {
      return this.props.render(loadState);
    }
    return null;
  }
}

QueryRenderer.propTypes = {
  render: PropTypes.func.required,
  afterRender: PropTypes.func,
  cubejsApi: PropTypes.object.required,
  query: PropTypes.object,
  queries: PropTypes.object,
  loadSql: PropTypes.any
};
