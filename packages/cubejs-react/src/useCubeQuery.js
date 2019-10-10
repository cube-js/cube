import {
  useContext, useEffect, useState
} from 'react';
import { equals } from 'ramda';
import CubeContext from './CubeContext';
import isQueryPresent from './isQueryPresent';

export default (query, options = {}) => {
  const [mutexObj] = useState({});
  const [currentQuery, setCurrentQuery] = useState(null);
  const [isLoading, setLoading] = useState(false);
  const [resultSet, setResultSet] = useState(null);
  const [error, setError] = useState(null);
  const context = useContext(CubeContext);

  useEffect(() => {
    async function loadQuery() {
      if (query && isQueryPresent(query) && !equals(currentQuery, query)) {
        setCurrentQuery(query);
        setLoading(true);
        try {
          setResultSet(await (options.cubejsApi || context && context.cubejsApi).load(query, {
            mutexObj,
            mutexKey: 'query'
          }));
          setLoading(false);
        } catch (e) {
          setError(e);
          setLoading(false);
        }
      }
    }
    loadQuery();
  }, [query, options.cubejsApi, context]);

  return { isLoading, resultSet, error };
};
