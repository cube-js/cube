import React from "react";
import Card from "@material-ui/core/Card";
import CardContent from "@material-ui/core/CardContent";
import Typography from "@material-ui/core/Typography";

const DashboardItem = ({ children, title }) => (
  <Card>
    <CardContent>
      {title && (
        <Typography component="h2" variant="h6" color="primary" gutterBottom>
          {title}
        </Typography>
      )}
      {children}
    </CardContent>
  </Card>
);

export default DashboardItem;
