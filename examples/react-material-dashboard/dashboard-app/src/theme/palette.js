import { colors } from '@material-ui/core';

const white = '#FFFFFF';
const black = '#000000';

export default {
  black,
  white,
  primary: {
    contrastText: white,
    dark: '#43436B',
    main: '#9592FF',
    normal: '#7A77FF',
    action: '#EEEDFF',
    light: '#F3F3FB',
  },
  secondary: {
    contrastText: white,
    dark: '#F3F3FB',
    main: '#FF6492',
    light: '#FFA2BE',
    lighten: '#FFE0E9',
  },
  success: {
    contrastText: white,
    dark: '#51D084',
    main: colors.green[600],
    light: colors.green[400],
  },
  info: {
    contrastText: white,
    dark: colors.blue[900],
    main: colors.blue[600],
    light: colors.blue[400],
  },
  warning: {
    contrastText: white,
    dark: colors.orange[900],
    main: colors.orange[600],
    light: colors.orange[400],
  },
  error: {
    contrastText: white,
    dark: '#E56860',
    main: colors.red[600],
    light: colors.red[400],
  },
  text: {
    primary: '#43436B',
    secondary: '#A1A1B5',
    link: '#D5D5E2',
  },
  background: {
    default: '#F4F6F8',
    gray: '#F8F8FC',
    paper: white,
  },
  icon: '#A1A1B5',
  divider: colors.grey[200],
  neutral: '#F3F3FB',
};
