import React from "react";
import classes from "./InlineButton.module.css";

export type InlineButtonProps = {
  children: string;
};

export const InlineButton = ({ children }: InlineButtonProps) => {
  return <span className={classes.Button}>{children}</span>;
};

export default InlineButton;
