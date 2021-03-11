import PlaygroundWrapper from '../PlaygroundWrapper';
import PlaygroundQueryBuilder from '../../PlaygroundQueryBuilder';

export default function QueryBuilder(props) {
  return (
    <PlaygroundWrapper {...props}>
      <PlaygroundQueryBuilder
        query={props.query}
        setQuery={props.setQuery}
        apiUrl={props.apiUrl}
        cubejsToken={props.token}
      />
    </PlaygroundWrapper>
  );
}
