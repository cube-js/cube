import React from 'react';
import { equals, toPairs, fromPairs } from 'ramda';
import { isQueryPresent } from '@cubejs-client/core';
import type { CubeApi, LoadMethodOptions, Query, ResultSet } from '@cubejs-client/core';

import CubeContext from './CubeContext';
import type {
  MutexObj,
  QueryRendererLoadState,
  QueryRendererProps,
  QueryRendererRenderProps,
  QueryRendererState,
} from './types';

/**
 * `<QueryRenderer />` a react component that accepts a query, fetches the given query, and uses the render prop to render the resulting data
 * @stickyTypes QueryRendererProps, QueryRendererRenderProps
 * @noInheritDoc
 */
export default class QueryRenderer extends React.Component<QueryRendererProps, QueryRendererState> {
  static contextType = CubeContext;

  static defaultProps = {
    cubeApi: null,
    query: null,
    render: null,
    queries: null,
    loadSql: null,
    updateOnlyOnStateChange: false,
    resetResultSetOnChange: true,
    cache: null,
  };

  // @deprecated use `isQueryPresent` from `@cubejs-client/core`
  static isQueryPresent(query: Query | Query[]) {
    return isQueryPresent(query);
  }

  constructor(props: QueryRendererProps) {
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

  shouldComponentUpdate(nextProps: QueryRendererProps, nextState: QueryRendererState) {
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

  componentDidUpdate(prevProps: QueryRendererProps) {
    const { query, queries } = this.props;
    if (!equals(prevProps.query, query)) {
      this.load(query);
    }

    if (!equals(prevProps.queries, queries)) {
      this.loadQueries(queries);
    }
  }

  // These sit after the lifecycle methods because `react/sort-comp` sorts
  // TypeScript field declarations into `everything-else`, which the configured
  // order puts after `lifecycle`. Runtime is unaffected: field initializers run
  // right after `super()`, so the constructor's assignments still win.
  //
  // `this.context` is not re-declared: React types it as `any`, and a field
  // declaration would be emitted at runtime and shadow the context React
  // assigns.
  private mutexObj: MutexObj;

  cubeApi(): CubeApi {
    // eslint-disable-next-line react/destructuring-assignment
    return this.props.cubeApi || this.context && this.context.cubeApi;
  }

  load(query: Query | Query[]) {
    const { resetResultSetOnChange, cache } = this.props;
    this.setState({
      isLoading: true,
      error: null,
      sqlQuery: null,
      ...(resetResultSetOnChange ? { resultSet: null } : {})
    });
    const { loadSql } = this.props;
    const cubeApi = this.cubeApi();

    const loadOptions: LoadMethodOptions = {
      mutexObj: this.mutexObj,
      mutexKey: 'query',
      ...(cache ? { cache } : {}),
    };

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
          cubeApi.load(query, loadOptions)
        ]).then(([sqlQuery, resultSet]) => this.setState({
          sqlQuery, resultSet, error: null, isLoading: false
        }))
          .catch(error => this.setState({
            ...(resetResultSetOnChange ? { resultSet: null } : {}),
            error,
            isLoading: false
          }));
      } else {
        cubeApi.load(query, loadOptions)
          .then(resultSet => this.setState({ resultSet, error: null, isLoading: false }))
          .catch(error => this.setState({
            ...(resetResultSetOnChange ? { resultSet: null } : {}),
            error,
            isLoading: false
          }));
      }
    }
  }

  loadQueries(queries?: { [key: string]: Query }) {
    const cubeApi = this.cubeApi();
    const { resetResultSetOnChange, cache } = this.props;
    this.setState({
      isLoading: true,
      ...(resetResultSetOnChange ? { resultSet: null } : {}),
      error: null
    });

    const resultPromises = Promise.all(toPairs(queries as { [key: string]: Query }).map(
      ([name, query]) => cubeApi.load(query, {
        mutexObj: this.mutexObj,
        mutexKey: name,
        ...(cache ? { cache } : {}),
      }).then((r): [string, ResultSet] => [name, r])
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

    const loadState: QueryRendererLoadState = {
      error: error ? new Error(error.response?.plainError || error.message || error.toString()) : null,
      resultSet: queries ? (resultSet || {}) : resultSet,
      loadingState: { isLoading },
      sqlQuery
    };

    if (render) {
      // The prop is declared as returning `void` for backwards compatibility,
      // while what it returns is what gets rendered
      return render(loadState as QueryRendererRenderProps) as React.ReactNode;
    }

    return null;
  }
}
