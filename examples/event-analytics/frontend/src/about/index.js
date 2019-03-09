import React from 'react';

import { withStyles } from '@material-ui/core/styles';
import WindowTitle from '../components/WindowTitle';

import Card from '@material-ui/core/Card';
import CardHeader from '@material-ui/core/CardHeader';
import CardMedia from '@material-ui/core/CardMedia';

const styles = theme => ({
  cardContainerStyles: {
    display: "grid",
    gridColumnGap: "24px",
    gridTemplateColumns: "1fr 1fr",
    rowGap: "24px"
  }
});

const AboutPage = ({ classes }) => (
  <>
    <WindowTitle title="About" />
    <div className={classes.cardContainerStyles}>
      <Card>
        <CardHeader title="Architecture" />
        <CardMedia component="img" image="../schema.png" />
      </Card>
    </div>
  </>
);

export default withStyles(styles)(AboutPage);
