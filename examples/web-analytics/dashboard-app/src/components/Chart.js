import React from "react";
import Card from "@material-ui/core/Card";
import CardContent from "@material-ui/core/CardContent";
import Typography from "@material-ui/core/Typography";
import ButtonGroup from "@material-ui/core/ButtonGroup";
import Button from "@material-ui/core/Button";

import ChartRenderer from "./ChartRenderer";

const Chart = ({ title, vizState, granularityControls }) => (
  <Card>
    <CardContent>
      {title && (
        <Typography component="h2" variant="h6" color="primary" gutterBottom>
          {title}
        </Typography>
      )}
      { granularityControls &&
        <ButtonGroup color="primary" aria-label="outlined primary button group">
          <Button>One</Button>
          <Button>Two</Button>
          <Button>Three</Button>
        </ButtonGroup>
      }
      <ChartRenderer vizState={vizState} />
    </CardContent>
  </Card>
);

export default Chart;
