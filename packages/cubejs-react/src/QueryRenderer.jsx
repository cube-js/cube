import React from 'react';
import * as PropTypes from 'prop-types';
import { equals, toPairs, fromPairs } from 'ramda';

export default class QueryRenderer extends React.Component {
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

  componentDidUpdate(prevProps) {
    const { query, queries } = this.props;
    if (!equals(prevProps.query, query)) {
      this.load(query);
    }

    if (!equals(prevProps.queries, queries)) {
      this.loadQueries(queries);
    }
  }

  isQueryPresent(query) {
    return query.measures && query.measures.length
      || query.dimensions && query.dimensions.length
      || query.timeDimensions && query.timeDimensions.length;
  }

  load(query) {
    this.setState({
      isLoading: true, resultSet: null, error: null, sqlQuery: null
    });
    const { loadSql, cubejsApi } = this.props;
    if (query && this.isQueryPresent(query)) {
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
    this.setState({ isLoading: true, resultSet: null, error: null });

    const resultPromises = Promise.all(toPairs(queries).map(
      ([name, query]) =>
        this.props.cubejsApi.load(query, { mutexObj: this.mutexObj, mutexKey: name }).then(r => [name, r])
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
  render: PropTypes.func,
  afterRender: PropTypes.func,
  cubejsApi: PropTypes.object.isRequired,
  query: PropTypes.object,
  queries: PropTypes.object,
  loadSql: PropTypes.any
};

QueryRenderer.defaultProps = {
  query: {}
};
