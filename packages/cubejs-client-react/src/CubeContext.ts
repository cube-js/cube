import { createContext } from 'react';

import type { CubeContextProps } from './types';

/**
 * In case when you need direct access to `cubeApi` you can use `CubeContext` anywhere in your app
 *
 * ```js
 * import React from 'react';
 * import { CubeContext } from '@cubejs-client/react';
 *
 * export default function DisplayComponent() {
 *   const { cubeApi } = React.useContext(CubeContext);
 *   const [rawResults, setRawResults] = React.useState([]);
 *   const query = {
 *     ...
 *   };
 *
 *   React.useEffect(() => {
 *     cubeApi.load(query).then((resultSet) => {
 *       setRawResults(resultSet.rawData());
 *     });
 *   }, [query]);
 *
 *   return (
 *     <>
 *       {rawResults.map(row => (
 *         ...
 *       ))}
 *     </>
 *   )
 * }
 * ```
 */
// The context has no default value: `cubeApi` is only available under a
// `CubeProvider`, and consumers guard against a missing context.
export default createContext<CubeContextProps>(null as unknown as CubeContextProps);
