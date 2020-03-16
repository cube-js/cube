import React from "react";
import { makeStyles } from '@material-ui/core/styles';
import Dialog from '@material-ui/core/Dialog';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogContentText from '@material-ui/core/DialogContentText';
import DialogTitle from '@material-ui/core/DialogTitle';

import List from '@material-ui/core/List';
import ListItem from '@material-ui/core/ListItem';
import ListItemText from '@material-ui/core/ListItemText';

const useStyles = makeStyles(theme => ({
  list: {
    width: 300,
    maxWidth: 360
  }
}));


const SegmentsDialog = ({ open, onClose, segments, selectedKey, onSelect }) => {
  const classes = useStyles();

  return (
    <Dialog
      open={open}
      onClose={onClose}
      aria-labelledby="segments-dialog-title"
      aria-describedby="segments-dialog-description"
    >
      <DialogTitle id="segments-dialog-title">
        Select Segment
      </DialogTitle>
      <DialogContent>
        <List className={classes.list}>
          {segments.map(segment => (
            <ListItem
              button
              selected={selectedKey === segment.key}
              key={segment.key}
              onClick={() => onSelect(segment)}
            >
              <ListItemText
                primary={segment.title}
                secondary={segment.description}
              />
            </ListItem>
          ))}
        </List>
      </DialogContent>
    </Dialog>
  )
};

export default SegmentsDialog;
