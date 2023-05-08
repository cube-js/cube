import React from "react";
import styles from "./styles.module.css";

export type InlineButtonProps = {
  children: string;
};

export const InlineButton = ({ children }: InlineButtonProps) => {
  return <span className={styles.button}>{children}</span>;
};

export default InlineButton;
