import React from 'react';
import { equals, toPairs, fromPairs } from 'ramda';
import { isQueryPresent } from '@cubejs-client/core';

import CubeContext from './CubeContext';

export default class QueryRenderer extends React.Component {
  static contextType = CubeContext;

  static defaultProps = {
    cubeApi: null,
    query: null,
    render: null,
    queries: null,
    loadSql: null,
    updateOnlyOnStateChange: false,
    resetResultSetOnChange: true
  };

  // @deprecated use `isQueryPresent` from `@cubejs-client/core`
  static isQueryPresent(query) {
    return isQueryPresent(query);
  }

  constructor(props) {
    super(props);
    this.state = {};
    this.mutexObj = {};
  }

  componentDidMount() {
    const { query, queries } = this.props;
    if (query) {
      this.load(query);
    }
    if (queries) {
      this.loadQueries(queries);
    }
  }

  shouldComponentUpdate(nextProps, nextState) {
    const {
      query, queries, render, cubeApi, loadSql, updateOnlyOnStateChange
    } = this.props;
    if (!updateOnlyOnStateChange) {
      return true;
    }
    return !equals(nextProps.query, query)
      || !equals(nextProps.queries, queries)
      || ((nextProps.render == null || render == null) && nextProps.render !== render)
      || nextProps.cubeApi !== cubeApi
      || nextProps.loadSql !== loadSql
      || !equals(nextState, this.state)
      || nextProps.updateOnlyOnStateChange !== updateOnlyOnStateChange;
  }

  componentDidUpdate(prevProps) {
    const { query, queries } = this.props;
    if (!equals(prevProps.query, query)) {
      this.load(query);
    }

    if (!equals(prevProps.queries, queries)) {
      this.loadQueries(queries);
    }
  }

  cubeApi() {
    // eslint-disable-next-line react/destructuring-assignment
    return this.props.cubeApi || this.context && this.context.cubeApi;
  }

  load(query) {
    const { resetResultSetOnChange } = this.props;
    this.setState({
      isLoading: true,
      error: null,
      sqlQuery: null,
      ...(resetResultSetOnChange ? { resultSet: null } : {})
    });
    const { loadSql } = this.props;
    const cubeApi = this.cubeApi();

    if (query && isQueryPresent(query)) {
      if (loadSql === 'only') {
        cubeApi.sql(query, { mutexObj: this.mutexObj, mutexKey: 'sql' })
          .then(sqlQuery => this.setState({ sqlQuery, error: null, isLoading: false }))
          .catch(error => this.setState({
            ...(resetResultSetOnChange ? { resultSet: null } : {}),
            error,
            isLoading: false
          }));
      } else if (loadSql) {
        Promise.all([
          cubeApi.sql(query, { mutexObj: this.mutexObj, mutexKey: 'sql' }),
          cubeApi.load(query, { mutexObj: this.mutexObj, mutexKey: 'query' })
        ]).then(([sqlQuery, resultSet]) => this.setState({
          sqlQuery, resultSet, error: null, isLoading: false
        }))
          .catch(error => this.setState({
            ...(resetResultSetOnChange ? { resultSet: null } : {}),
            error,
            isLoading: false
          }));
      } else {
        cubeApi.load(query, { mutexObj: this.mutexObj, mutexKey: 'query' })
          .then(resultSet => this.setState({ resultSet, error: null, isLoading: false }))
          .catch(error => this.setState({
            ...(resetResultSetOnChange ? { resultSet: null } : {}),
            error,
            isLoading: false
          }));
      }
    }
  }

  loadQueries(queries) {
    const cubeApi = this.cubeApi();
    const { resetResultSetOnChange } = this.props;
    this.setState({
      isLoading: true,
      ...(resetResultSetOnChange ? { resultSet: null } : {}),
      error: null
    });

    const resultPromises = Promise.all(toPairs(queries).map(
      ([name, query]) => cubeApi.load(query, { mutexObj: this.mutexObj, mutexKey: name }).then(r => [name, r])
    ));

    resultPromises
      .then(resultSet => this.setState({
        resultSet: fromPairs(resultSet),
        error: null,
        isLoading: false
      }))
      .catch(error => this.setState({
        ...(resetResultSetOnChange ? { resultSet: null } : {}),
        error,
        isLoading: false
      }));
  }

  render() {
    const {
      error, queries, resultSet, isLoading, sqlQuery
    } = this.state;
    const { render } = this.props;

    const loadState = {
      error: error ? new Error(error.response?.plainError || error.message || error.toString()) : null,
      resultSet: queries ? (resultSet || {}) : resultSet,
      loadingState: { isLoading },
      sqlQuery
    };

    if (render) {
      return render(loadState);
    }

    return null;
  }
}
