import React, { useEffect, useState } from 'react';
import { checkHasData } from '../api';
import ContentView from '../ContentView';
import UploadView from '../UploadView';

function App() {
  const [hasData, setHasData] = useState(undefined);

  useEffect(() => {
    checkHasData().then(setHasData);
  }, []);

  return hasData === undefined
    ? null
    : hasData
      ? <ContentView />
      : <UploadView onUpload={() => setHasData(true)} />;
}

export default App;