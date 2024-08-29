import { Alert } from '@cube-dev/ui-kit';

import { useQueryBuilderContext } from './context';

export function QueryBuilderError() {
  const { verificationError, error, query, isVerifying, joinedCubes } = useQueryBuilderContext();

  if (!joinedCubes?.length) {
    return null;
  }

  return (
    <>
      {!isVerifying &&
      verificationError &&
      !verificationError.toString().includes('Values required for filter') ? (
        <Alert theme="note" padding="1x">
          {verificationError.toString().replace('Error: Error: ', '')}
        </Alert>
      ) : null}
      {error ? (
        <Alert theme="danger" padding="1x">
          {error.toString().replace('Error: Error: ', '')}
        </Alert>
      ) : null}
    </>
  );
}
