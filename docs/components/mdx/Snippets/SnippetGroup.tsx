import React from "react";
import styles from "./styles.module.css";

export const Snippet = ({ children }) => children;

export const SnippetGroup = ({ children }) => {
  return <div className={styles.snippetGroup}>{children}</div>;
};
