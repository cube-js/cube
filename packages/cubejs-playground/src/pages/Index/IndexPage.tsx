import { useEffect, useLayoutEffect, useState } from 'react';
import { useHistory } from 'react-router-dom';

import { useIsMounted, usePlaygroundContext } from '../../hooks';

export function IndexPage() {
  const { push } = useHistory();
  const isMounted = useIsMounted();
  const context = usePlaygroundContext();

  const [files, setFiles] = useState<any[] | null>(null);

  useEffect(() => {
    async function loadFiles() {
      const res = await fetch('playground/files');
      const result = await res.json();

      if (result.error?.includes('Model files not found')) {
        setFiles([]);
      } else if (result.error) {
        throw result.error;
      }

      if (isMounted()) {
        setFiles(result.files);
      }
    }

    loadFiles();
  }, []);

  useLayoutEffect(() => {
    if (context && files != null) {
      if (context.shouldStartConnectionWizardFlow) {
        push('/connection');
      } else if (
        !files.length ||
        (files.length === 1 && files[0].fileName === 'Orders.js')
      ) {
        push('/schema');
      } else {
        push('/build');
      }
    }
  }, [context, files]);

  return null;
}
