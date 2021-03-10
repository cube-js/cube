import { useState } from 'react';

import PlaygroundWrapper from '../PlaygroundWrapper';
import PlaygroundQueryBuilder from '../../PlaygroundQueryBuilder';

export default function QueryBuilder(props) {
  const [query, setQuery] = useState({});

  return (
    <PlaygroundWrapper {...props}>
      <PlaygroundQueryBuilder
        query={query}
        setQuery={setQuery}
        apiUrl={props.apiUrl}
        cubejsToken={props.token}
      />
    </PlaygroundWrapper>
  );
}
