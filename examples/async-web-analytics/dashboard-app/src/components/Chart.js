import React from "react";
import Card from "@material-ui/core/Card";
import CardContent from "@material-ui/core/CardContent";
import Typography from "@material-ui/core/Typography";

import ChartRenderer from "./ChartRenderer";

const heights = {
  pie: 250,
  line: 250,
  number: 50
}

const Chart = ({ title, vizState, height }) => (
  <Card>
    <CardContent>
      <Typography component="p" color="primary" gutterBottom>
        {title}
      </Typography>
      <ChartRenderer vizState={vizState} height={height || heights[vizState.chartType]} />
    </CardContent>
  </Card>
);

export default Chart;
