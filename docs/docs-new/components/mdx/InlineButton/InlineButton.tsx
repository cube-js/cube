import React from "react";
import classes from "./InlineButton.module.scss";

export type InlineButtonProps = {
  children: string;
};

export const InlineButton = ({ children }: InlineButtonProps) => {
  return <span className={classes.Button}>{children}</span>;
};

export default InlineButton;
