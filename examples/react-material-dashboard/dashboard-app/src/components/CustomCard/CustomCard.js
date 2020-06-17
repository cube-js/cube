import React from 'react';
import { Card } from "@material-ui/core";
import { makeStyles } from "@material-ui/styles";


const CustomCard = props => {
  const { children, height, padding } = props;

  const useStyles = makeStyles(theme => ({
    card: {
      boxShadow: 'none',
      borderRadius: 8,
      height: height || '100%',
      padding: padding || '8px 8px 0 8px'
    },
  }));

  const classes = useStyles();

  return (
    <Card {...props} className={classes.card}>
      {children}
    </Card>
  );
};

export default CustomCard;
