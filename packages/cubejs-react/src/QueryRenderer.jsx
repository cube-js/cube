import React from 'react';
import * as PropTypes from 'prop-types';
import { equals, toPairs, fromPairs } from 'ramda';
import isQueryPresent from './isQueryPresent';
import CubeContext from "./CubeContext";

export default class QueryRenderer extends React.Component {
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
      query, queries, render, cubejsApi, loadSql, updateOnlyOnStateChange
    } = this.props;
    if (!updateOnlyOnStateChange) {
      return true;
    }
    return !equals(nextProps.query, query)
      || !equals(nextProps.queries, queries)
      || ((nextProps.render == null || render == null) && nextProps.render !== render)
      || nextProps.cubejsApi !== cubejsApi
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

  cubejsApi() {
    // eslint-disable-next-line react/destructuring-assignment
    return this.props.cubejsApi || this.context && this.context.cubejsApi;
  }

  load(query) {
    this.setState({
      isLoading: true, resultSet: null, error: null, sqlQuery: null
    });
    const { loadSql } = this.props;
    const cubejsApi = this.cubejsApi();
    if (query && QueryRenderer.isQueryPresent(query)) {
      if (loadSql === 'only') {
        cubejsApi.sql(query, { mutexObj: this.mutexObj, mutexKey: 'sql' })
          .then(sqlQuery => this.setState({ sqlQuery, error: null, isLoading: false }))
          .catch(error => this.setState({ resultSet: null, error, isLoading: false }));
      } else if (loadSql) {
        Promise.all([
          cubejsApi.sql(query, { mutexObj: this.mutexObj, mutexKey: 'sql' }),
          cubejsApi.load(query, { mutexObj: this.mutexObj, mutexKey: 'query' })
        ]).then(([sqlQuery, resultSet]) => this.setState({
          sqlQuery, resultSet, error: null, isLoading: false
        }))
          .catch(error => this.setState({ resultSet: null, error, isLoading: false }));
      } else {
        cubejsApi.load(query, { mutexObj: this.mutexObj, mutexKey: 'query' })
          .then(resultSet => this.setState({ resultSet, error: null, isLoading: false }))
          .catch(error => this.setState({ resultSet: null, error, isLoading: false }));
      }
    }
  }

  loadQueries(queries) {
    const cubejsApi = this.cubejsApi();
    this.setState({ isLoading: true, resultSet: null, error: null });

    const resultPromises = Promise.all(toPairs(queries).map(
      ([name, query]) => cubejsApi.load(query, { mutexObj: this.mutexObj, mutexKey: name }).then(r => [name, r])
    ));

    resultPromises
      .then(resultSet => this.setState({
        resultSet: fromPairs(resultSet),
        error: null,
        isLoading: false
      }))
      .catch(error => this.setState({ resultSet: null, error, isLoading: false }));
  }

  render() {
    const {
      error, queries, resultSet, isLoading, sqlQuery
    } = this.state;
    const { render } = this.props;
    const loadState = {
      error,
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

QueryRenderer.contextType = CubeContext;

QueryRenderer.propTypes = {
  render: PropTypes.func,
  cubejsApi: PropTypes.object,
  query: PropTypes.object,
  queries: PropTypes.object,
  loadSql: PropTypes.any,
  updateOnlyOnStateChange: PropTypes.bool
};

QueryRenderer.defaultProps = {
  cubejsApi: null,
  query: null,
  render: null,
  queries: null,
  loadSql: null,
  updateOnlyOnStateChange: false
};
