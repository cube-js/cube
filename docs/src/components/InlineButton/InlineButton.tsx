import React from 'react';
import * as styles from './styles.module.scss';

export type InlineButtonProps = {
  children: string;
}

export const InlineButton = ({ children }: InlineButtonProps) => {
  return <span className={styles.button}>{children}</span>;
};

export default InlineButton;