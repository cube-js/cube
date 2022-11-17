import React from 'react';
import * as styles from './styles.module.scss';

export const Snippet = ({ children }) => children;

export const SnippetGroup = ({ children }) => {
  return (
    <div className={styles.snippetGroup}>
      {children}
    </div>
  );
};
