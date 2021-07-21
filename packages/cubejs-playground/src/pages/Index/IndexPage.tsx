import { useEffect, useLayoutEffect, useState } from 'react';
import { useHistory } from 'react-router';
import { fetch } from 'whatwg-fetch';

import { useIsMounted, usePlaygroundContext } from '../../hooks';

export function IndexPage() {
  const { push } = useHistory();
  const isMounted = useIsMounted();
  const context = usePlaygroundContext();

  const [files, setFiles] = useState<any[] | null>(null);

  const [debug, setDebug] = useState<any>({});

  useEffect(() => {
    async function loadFiles() {
      setDebug({ loadFiles: true });
      const res = await fetch('/playground/files');
      const result = await res.json();

      setDebug({ loaded: true, isMounted: isMounted() });

      if (isMounted()) {
        setFiles(result.files);
      } else {
        setDebug({ notMounted: true });
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

  return (
    <div style={{ fontSize: 30 }}>
      <div style={{ marginBottom: 100 }}>{JSON.stringify(debug)}</div>

      <div style={{ marginBottom: 100 }}>files: {JSON.stringify(files)}</div>
      <div style={{ marginBottom: 100 }}>
        context: {JSON.stringify(context)}
      </div>
    </div>
  );
}
