import React from 'react';
import { DocSearch } from '@docsearch/react';
import '@docsearch/css';

import * as styles from '../../../static/styles/index.module.scss';
import '../../../static/styles/docsearch-custom.css';

export default function Search() {
  return (
    <div className={styles.searchBoxWrapper}>
      <DocSearch
        appId={process.env.ALGOLIA_APP_ID as string}
        apiKey={process.env.ALGOLIA_API_KEY as string}
        indexName={process.env.ALGOLIA_INDEX_NAME as string}
      />
    </div>
  );
}
