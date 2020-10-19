import React from 'react';
import PropTypes from 'prop-types';
import clsx from 'clsx';
import { makeStyles } from '@material-ui/styles';

const useStyles = makeStyles(theme => ({
  root: {
    display: 'inline-block',
    borderRadius: '50%',
    flexGrow: 0,
    flexShrink: 0,
  },
  sm: {
    height: 15,
    width: 15,
  },
  md: {
    height: 15,
    width: 15,
  },
  lg: {
    height: 15,
    width: 15,
  },
  neutral: { backgroundColor: '#fff' },
  primary: { backgroundColor: '#ccc' },
  info: { backgroundColor: '#3cc' },
  warning: { backgroundColor: '#cc3' },
  danger: { backgroundColor: '#c33' },
  success: { backgroundColor: '#3c3' },
}));

const StatusBullet = props => {
  const { className, size, color, ...rest } = props;

  const classes = useStyles();

  return (
    <span
      {...rest}
      className={clsx(
        {
          [classes.root]: true,
          [classes[size]]: size,
          [classes[color]]: color,
        },
        className,
      )}
    />
  );
};

StatusBullet.propTypes = {
  className: PropTypes.string,
  color: PropTypes.oneOf([
    'neutral',
    'primary',
    'info',
    'success',
    'warning',
    'danger',
  ]),
  size: PropTypes.oneOf(['sm', 'md', 'lg']),
};

StatusBullet.defaultProps = {
  size: 'md',
  color: 'default',
};

export default StatusBullet;