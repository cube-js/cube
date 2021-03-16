import PlaygroundWrapper from '../PlaygroundWrapper';
import PlaygroundQueryBuilder from '../../PlaygroundQueryBuilder';

export default function QueryBuilder({ apiUrl, token, ...props }) {
  return (
    <PlaygroundWrapper apiUrl={apiUrl} token={token} getToken={props.getToken}>
      <PlaygroundQueryBuilder
        apiUrl={apiUrl}
        cubejsToken={token}
        initialVizState={{
          query: props.defaultQuery,
          ...props.initialVizState,
        }}
        onVizStateChanged={(vizState) => props.onVizStateChanged?.(vizState)}
      />
    </PlaygroundWrapper>
  );
}
