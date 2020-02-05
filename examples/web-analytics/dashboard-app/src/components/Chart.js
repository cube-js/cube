import React from "react";
import Card from "@material-ui/core/Card";
import CardContent from "@material-ui/core/CardContent";
import Typography from "@material-ui/core/Typography";

import ChartRenderer from "./ChartRenderer";

const Chart = ({ title, vizState }) => (
  <Card>
    <CardContent>
      <Typography component="h2" variant="h6" color="primary" gutterBottom>
        {title}
      </Typography>
      <ChartRenderer vizState={vizState} />
    </CardContent>
  </Card>
);

export default Chart;
