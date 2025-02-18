import { Alert } from '@cube-dev/ui-kit';

import { useQueryBuilderContext } from './context';

export function QueryBuilderError() {
  const { verificationError, error, isVerifying, usedCubes } = useQueryBuilderContext();

  if (!usedCubes?.length) {
    return null;
  }

  return (
    <>
      {verificationError && !verificationError.toString().includes('Values required for filter') ? (
        <Alert theme="note" padding="1x" opacity={isVerifying ? '.5' : 1}>
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
