import React from 'react';

import ToggleButtonGroup from '@material-ui/lab/ToggleButtonGroup';
import ToggleButton from '@material-ui/lab/ToggleButton';
import ShowChartIcon from '@material-ui/icons/ShowChart';
import BarChartIcon from '@material-ui/icons/BarChart';
import PieChartIcon from '@material-ui/icons/PieChart';
import { withStyles } from '@material-ui/core/styles';

const styles = ({
  toggleContainer: {
    height: 56,
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'flex-start',
  },
});

const VisualizationToggle = ({ value, onChange, classes }) => (
   <div className={classes.toggleContainer}>
     <ToggleButtonGroup
        exclusive
        value={value}
        onChange={((e, value) => {
          if (value) {
            onChange({ type: 'CHANGE_VISUALIZATION_TYPE', value: value})
          }
        })}
      >
       <ToggleButton value="line">
         <ShowChartIcon />
       </ToggleButton>
       <ToggleButton value="bar">
         <BarChartIcon />
       </ToggleButton>
       <ToggleButton value="pie">
         <PieChartIcon />
       </ToggleButton>
     </ToggleButtonGroup>
   </div>
)

export default withStyles(styles)(VisualizationToggle);
