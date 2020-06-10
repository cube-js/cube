import React from 'react';
import clsx from 'clsx';
import PropTypes from 'prop-types';
import { makeStyles } from '@material-ui/styles';
import {
  Card,
  CardHeader,
  CardContent,
  CardActions,
  Button,
  Divider,
  List,
  ListItem,
  ListItemText,
  IconButton,
} from "@material-ui/core";
import ArrowRightIcon from '@material-ui/icons/ArrowRight';
import MoreVertIcon from '@material-ui/icons/MoreVert';
import moment from "moment";
import { QueryRenderer } from "@cubejs-client/react";
import { Link } from "react-router-dom";

const useStyles = makeStyles(() => ({
  root: {
    height: '100%'
  },
  content: {
    padding: 0
  },
  image: {
    height: 48,
    width: 48
  },
  actions: {
    justifyContent: 'flex-end'
  }
}));

const query = {
  order: {
    [`Orders.createdAt`]: "desc"
  },
  limit: 6,
  "measures": [
    "Products.count"
  ],
  "timeDimensions": [
    {
      "dimension": "Products.createdAt"
    }
  ],
  "dimensions": [
    "Products.name",
    "Products.description",
    "Products.createdAt"
  ],
  "filters": []
};

const LatestProducts = props => {
  const { className, cubejsApi, ...rest } = props;

  const classes = useStyles();


  return (
    <QueryRenderer
      query={query}
      cubejsApi={cubejsApi}
      render={({ resultSet }) => {
        if (!resultSet) {
          return <div className="loader"/>;
        }
        let products = resultSet.tablePivot();
        return (
          <Card
            {...rest}
            className={clsx(classes.root, className)}
          >
            <CardHeader
              subtitle={`${products.length} in total`}
              title="Latest products"
            />
            <Divider />
            <CardContent className={classes.content}>
              <List>
                {products.map((product, i) => (
                  <ListItem
                    divider={i < products.length - 1}
                    key={product['Products.name'] + Math.random()}
                  >
                    <ListItemText
                      primary={product['Products.name']}
                      secondary={`Updated ${moment(product['Products.createdAt']).format('DD/MM/YYYY')}`}
                    />
                    <IconButton
                      edge="end"
                      size="small"
                    >
                      <MoreVertIcon />
                    </IconButton>
                  </ListItem>
                ))}
              </List>
            </CardContent>
            <Divider />
            <CardActions className={classes.actions}>
              <Button
                color="primary"
                size="small"
                variant="text"
                component={Link}
                to={'/orders'}
              >
                View all <ArrowRightIcon />
              </Button>
            </CardActions>
          </Card>
        );
      }}
    />
  );
};

LatestProducts.propTypes = {
  className: PropTypes.string
};

export default LatestProducts;
