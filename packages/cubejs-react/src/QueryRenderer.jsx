import React from 'react';
import * as PropTypes from 'prop-types';
import { equals, toPairs, fromPairs } from 'ramda';

export default class QueryRenderer extends React.Component {
  static isQueryPresent(query) {
    return query.measures && query.measures.length
      || query.dimensions && query.dimensions.length
      || query.timeDimensions && query.timeDimensions.length;
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

  componentDidUpdate(prevProps) {
    const { query, queries } = this.props;
    if (!equals(prevProps.query, query)) {
      this.load(query);
    }

    if (!equals(prevProps.queries, queries)) {
      this.loadQueries(queries);
    }
  }

  load(query) {
    this.setState({
      isLoading: true, resultSet: null, error: null, sqlQuery: null
    });
    const { loadSql, cubejsApi } = this.props;
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
    const { cubejsApi } = this.props;
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

QueryRenderer.propTypes = {
  render: PropTypes.func,
  cubejsApi: PropTypes.object.isRequired,
  query: PropTypes.object,
  queries: PropTypes.object,
  loadSql: PropTypes.any
};

QueryRenderer.defaultProps = {
  query: null,
  render: null,
  queries: null,
  loadSql: null
};
